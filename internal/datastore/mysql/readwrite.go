package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/go-sql-driver/mysql"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"
	errUnableToWriteConfig         = "unable to write namespace config: %w"
	errUnableToDeleteConfig        = "unable to delete namespace config: %w"
)

var duplicateEntryRegx = regexp.MustCompile(`^Duplicate entry '(.+)' for key 'uq_relation_tuple_living'$`)

type mysqlReadWriteTXN struct {
	*mysqlReader

	ctx      context.Context
	tx       *sql.Tx
	newTxnID uint64
}

// WriteRelationships takes a list of existing relationships that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified namespace.
func (rwt *mysqlReadWriteTXN) WriteRelationships(mutations []*core.RelationTupleUpdate) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	// there are some fundamental changes introduced to prevent a deadlock in MySQL
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "WriteTuples")
	defer span.End()

	bulkWrite := rwt.WriteTupleQuery
	bulkWriteHasValues := false

	selectForUpdateQuery := rwt.QueryTupleIdsQuery

	clauses := sq.Or{}

	// Process the actual updates
	for _, mut := range mutations {
		tpl := mut.Tuple
		// Implementation for TOUCH deviates from PostgreSQL datastore to prevent a deadlock in MySQL
		if mut.Operation == core.RelationTupleUpdate_TOUCH || mut.Operation == core.RelationTupleUpdate_DELETE {
			clauses = append(clauses, exactRelationshipClause(tpl))
		}

		if mut.Operation == core.RelationTupleUpdate_TOUCH || mut.Operation == core.RelationTupleUpdate_CREATE {
			bulkWrite = bulkWrite.Values(
				tpl.ResourceAndRelation.Namespace,
				tpl.ResourceAndRelation.ObjectId,
				tpl.ResourceAndRelation.Relation,
				tpl.Subject.Namespace,
				tpl.Subject.ObjectId,
				tpl.Subject.Relation,
				rwt.newTxnID,
			)
			bulkWriteHasValues = true
		}
	}

	if len(clauses) > 0 {
		query, args, err := selectForUpdateQuery.Where(clauses).ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		rows, err := rwt.tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		defer migrations.LogOnError(ctx, rows.Close)

		tupleIds := make([]int64, 0, len(clauses))
		for rows.Next() {
			var tupleID int64
			if err := rows.Scan(&tupleID); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			tupleIds = append(tupleIds, tupleID)
		}

		if rows.Err() != nil {
			return fmt.Errorf(errUnableToWriteRelationships, rows.Err())
		}

		if len(tupleIds) > 0 {
			query, args, err := rwt.
				DeleteTupleQuery.
				Where(sq.Eq{colID: tupleIds}).
				Set(colDeletedTxn, rwt.newTxnID).
				ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
			if _, err := rwt.tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		}
	}

	if bulkWriteHasValues {
		query, args, err := bulkWrite.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.ExecContext(ctx, query, args...)
		if err != nil {
			if cerr := convertToWriteConstraintError(err); cerr != nil {
				return cerr
			}

			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteRelationships(filter *v1.RelationshipFilter) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "DeleteRelationships")
	defer span.End()

	// Add clauses for the ResourceFilter
	query := rwt.DeleteTupleQuery.Where(sq.Eq{colNamespace: filter.ResourceType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceId))
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalRelation))
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		tracerAttributes = append(tracerAttributes, common.SubNamespaceNameKey.String(subjectFilter.SubjectType))
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
			tracerAttributes = append(tracerAttributes, common.SubObjectIDKey.String(subjectFilter.OptionalSubjectId))
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
			tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(relationFilter.Relation))
		}
	}

	span.SetAttributes(tracerAttributes...)

	query = query.Set(colDeletedTxn, rwt.newTxnID)

	querySQL, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	if _, err := rwt.tx.ExecContext(ctx, querySQL, args...); err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) WriteNamespaces(newNamespaces ...*core.NamespaceDefinition) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx := datastore.SeparateContextWithTracing(rwt.ctx)

	ctx, span := tracer.Start(ctx, "WriteNamespaces")
	defer span.End()

	deletedNamespaceClause := sq.Or{}
	writeQuery := rwt.WriteNamespaceQuery

	writtenNamespaceNames := make([]string, 0, len(newNamespaces))
	for _, newNamespace := range newNamespaces {
		serialized, err := proto.Marshal(newNamespace)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}
		span.AddEvent("Serialized namespace config")

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})
		writeQuery = writeQuery.Values(newNamespace.Name, serialized, rwt.newTxnID)
		writtenNamespaceNames = append(writtenNamespaceNames, newNamespace.Name)
	}

	span.SetAttributes(common.ObjNamespaceNameKey.StringSlice(writtenNamespaceNames))

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.And{sq.Eq{colDeletedTxn: liveDeletedTxnID}, deletedNamespaceClause}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	query, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteNamespace(nsName string) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	ctx, span := tracer.Start(rwt.ctx, "DeleteNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()
	ctx = datastore.SeparateContextWithTracing(ctx)

	baseQuery := rwt.ReadNamespaceQuery.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
	_, createdAt, err := loadNamespace(ctx, nsName, rwt.tx, baseQuery)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return err
	case err == nil:
		break
	default:
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := rwt.DeleteNamespaceTuplesQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Eq{colNamespace: nsName}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func convertToWriteConstraintError(err error) error {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == errMysqlDuplicateEntry {
		found := duplicateEntryRegx.FindStringSubmatch(mysqlErr.Message)
		if found != nil {
			parts := strings.Split(found[1], "-")
			if len(parts) == 7 {
				return common.NewCreateRelationshipExistsError(&core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: parts[0],
						ObjectId:  parts[1],
						Relation:  parts[2],
					},
					Subject: &core.ObjectAndRelation{
						Namespace: parts[3],
						ObjectId:  parts[4],
						Relation:  parts[5],
					},
				})
			}
		}

		return common.NewCreateRelationshipExistsError(nil)
	}
	return nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func exactRelationshipClause(r *core.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        r.ResourceAndRelation.Namespace,
		colObjectID:         r.ResourceAndRelation.ObjectId,
		colRelation:         r.ResourceAndRelation.Relation,
		colUsersetNamespace: r.Subject.Namespace,
		colUsersetObjectID:  r.Subject.ObjectId,
		colUsersetRelation:  r.Subject.Relation,
	}
}

var _ datastore.ReadWriteTransaction = &mysqlReadWriteTXN{}

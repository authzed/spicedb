package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToWriteConfig         = "unable to write namespace config: %w"
	errUnableToDeleteConfig        = "unable to delete namespace config: %w"
	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"
)

var (
	writeNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
		colCreatedTxn,
	)

	deleteNamespace = psql.Update(tableNamespace).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})

	writeTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedTxn,
	)

	deleteTuple = psql.Update(tableTuple).Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
)

type pgReadWriteTXN struct {
	*pgReader
	ctx      context.Context
	tx       pgx.Tx
	newTxnID uint64
}

func (rwt *pgReadWriteTXN) WriteRelationships(mutations []*v1.RelationshipUpdate) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "WriteTuples")
	defer span.End()

	bulkWrite := writeTuple
	bulkWriteHasValues := false

	// Process the actual updates
	for _, mut := range mutations {
		rel := mut.Relationship

		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_DELETE {
			sql, args, err := deleteTuple.Where(exactRelationshipClause(rel)).Set(colDeletedTxn, rwt.newTxnID).ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		}

		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_CREATE {
			bulkWrite = bulkWrite.Values(
				rel.Resource.ObjectType,
				rel.Resource.ObjectId,
				rel.Relation,
				rel.Subject.Object.ObjectType,
				rel.Subject.Object.ObjectId,
				stringz.DefaultEmpty(rel.Subject.OptionalRelation, datastore.Ellipsis),
				rwt.newTxnID,
			)
			bulkWriteHasValues = true
		}
	}

	if bulkWriteHasValues {
		sql, args, err := bulkWrite.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.Exec(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func (rwt *pgReadWriteTXN) DeleteRelationships(filter *v1.RelationshipFilter) error {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(rwt.ctx), "DeleteRelationships")
	defer span.End()

	// Add clauses for the ResourceFilter
	query := deleteTuple.Where(sq.Eq{colNamespace: filter.ResourceType})
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

	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error {
	ctx := datastore.SeparateContextWithTracing(rwt.ctx)

	ctx, span := tracer.Start(ctx, "WriteNamespaces")
	defer span.End()

	// span.SetAttributes(common.ObjNamespaceNameKey.String(newConfig.Name))

	deletedNamespaceClause := sq.Or{}
	writeQuery := writeNamespace

	for _, newNamespace := range newConfigs {
		serialized, err := proto.Marshal(newNamespace)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}
		span.AddEvent("Serialized namespace config")

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})
		writeQuery = writeQuery.Values(newNamespace.Name, serialized, rwt.newTxnID)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.And{sq.Eq{colDeletedTxn: liveDeletedTxnID}, deletedNamespaceClause}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	sql, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}
	span.AddEvent("Namespace config written")

	return nil
}

func (rwt *pgReadWriteTXN) DeleteNamespace(nsName string) error {
	ctx := datastore.SeparateContextWithTracing(rwt.ctx)

	baseQuery := readNamespace.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
	_, createdAt, err := loadNamespace(ctx, nsName, rwt.tx, baseQuery)
	switch {
	case errors.As(err, &datastore.ErrNamespaceNotFound{}):
		return err
	case err == nil:
		break
	default:
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := deleteNamespaceTuples.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Eq{colNamespace: nsName}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func exactRelationshipClause(r *v1.Relationship) sq.Eq {
	return sq.Eq{
		colNamespace:        r.Resource.ObjectType,
		colObjectID:         r.Resource.ObjectId,
		colRelation:         r.Relation,
		colUsersetNamespace: r.Subject.Object.ObjectType,
		colUsersetObjectID:  r.Subject.Object.ObjectId,
		colUsersetRelation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
	}
}

var _ datastore.ReadWriteTransaction = &pgReadWriteTXN{}

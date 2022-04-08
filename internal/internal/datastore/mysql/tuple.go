package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
)

// WriteTuples takes a list of existing tuples that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified
// namespace.
func (mds *mysqlDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	if err := mds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	bulkWrite := mds.WriteTupleQuery
	bulkWriteHasValues := false

	selectForUpdateQuery := mds.QueryTupleIdsQuery

	clauses := sq.Or{}

	// Process the actual updates
	for _, mut := range mutations {
		rel := mut.Relationship

		// Implementation for TOUCH deviates slightly from PostgreSQL datastore to prevent a deadlock in MySQL
		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_DELETE {
			clauses = append(clauses, exactRelationshipClause(rel))
		}

		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_CREATE {
			bulkWrite = bulkWrite.Values(
				rel.Resource.ObjectType,
				rel.Resource.ObjectId,
				rel.Relation,
				rel.Subject.Object.ObjectType,
				rel.Subject.Object.ObjectId,
				stringz.DefaultEmpty(rel.Subject.OptionalRelation, datastore.Ellipsis),
				newTxnID,
			)
			bulkWriteHasValues = true
		}
	}

	if len(clauses) > 0 {
		query, args, err := selectForUpdateQuery.Where(clauses).ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
		}

		rows, err := mds.db.QueryContext(ctx, query, args...)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
		}
		defer migrations.LogOnError(ctx, rows.Close)

		tupleIds := make([]int64, 0, len(clauses))
		for rows.Next() {
			var tupleID int64
			if err := rows.Scan(&tupleID); err != nil {
				return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
			}

			tupleIds = append(tupleIds, tupleID)
		}

		if rows.Err() != nil {
			return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, rows.Err())
		}

		if len(tupleIds) > 0 {
			query, args, err := mds.DeleteTupleQuery.Where(sq.Eq{colID: tupleIds}).Set(colDeletedTxn, newTxnID).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
			}
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
			}
		}
	}

	if bulkWriteHasValues {
		query, args, err := bulkWrite.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	return revisionFromTransaction(newTxnID), nil
}

// NOTE(chriskirkland): ErrNoRows needs to be configured/dependency injected per sql-driver type
func (mds *mysqlDatastore) checkPreconditions(ctx context.Context, tx *sql.Tx, preconditions []*v1.Precondition) error {
	ctx, span := tracer.Start(ctx, "checkPreconditions")
	defer span.End()

	for _, precond := range preconditions {
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			query, args, err := mds.selectQueryForFilter(precond.Filter).Limit(1).ToSql()
			if err != nil {
				return err
			}

			foundID := -1
			if err := tx.QueryRowContext(ctx, query, args...).Scan(&foundID); err != nil {
				switch {
				case errors.Is(err, sql.ErrNoRows) && precond.Operation == v1.Precondition_OPERATION_MUST_MATCH:
					return datastore.NewPreconditionFailedErr(precond)
				case errors.Is(err, sql.ErrNoRows) && precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH:
					continue
				default:
					return err
				}
			}

			if precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH {
				return datastore.NewPreconditionFailedErr(precond)
			}
		default:
			return fmt.Errorf("unspecified precondition operation")
		}
	}

	return nil
}

// NOTE(chriskirkland): this is all generic other than the squirrel templating for `queryTupleExists`
func (mds *mysqlDatastore) selectQueryForFilter(filter *v1.RelationshipFilter) sq.SelectBuilder {
	query := mds.QueryTupleExistsQuery.Where(sq.Eq{colNamespace: filter.ResourceType})

	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	return query
}

func (mds *mysqlDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := mds.db.BeginTx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteTuples, err)
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	if err := mds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	// Add clauses for the ResourceFilter
	query := mds.DeleteTupleQuery.Where(sq.Eq{colNamespace: filter.ResourceType})
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

	newTxnID, err := mds.createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	query = query.Set(colDeletedTxn, newTxnID)

	querySQL, args, err := query.ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToDeleteTuples, err)
	}

	if _, err := tx.ExecContext(ctx, querySQL, args...); err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(ErrUnableToWriteTuples, err)
	}

	return revisionFromTransaction(newTxnID), nil
}

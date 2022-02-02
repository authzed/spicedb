package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jmoiron/sqlx"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

var (
	writeTuple = sb.Insert(common.TableTuple).Columns(
		common.ColNamespace,
		common.ColObjectID,
		common.ColRelation,
		common.ColUsersetNamespace,
		common.ColUsersetObjectID,
		common.ColUsersetRelation,
		common.ColCreatedTxn,
	)

	deleteTuple = sb.Update(common.TableTuple).Where(sq.Eq{common.ColDeletedTxn: liveDeletedTxnID})

	queryTupleExists = sb.Select(common.ColID).From(common.TableTuple)
)

// WriteTuples takes a list of existing tuples that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified
// namespace.
func (mds *mysqlDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := mds.db.BeginTxx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToInstantiate, err)
	}
	defer tx.Rollback()

	if err := mds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	bulkWrite := writeTuple
	bulkWriteHasValues := false

	// Process the actual updates
	for _, mut := range mutations {
		rel := mut.Relationship

		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_DELETE {
			query, args, err := deleteTuple.Where(common.ExactRelationshipClause(rel)).Set(common.ColDeletedTxn, newTxnID).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
			}

			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
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
				newTxnID,
			)
			bulkWriteHasValues = true
		}
	}

	if bulkWriteHasValues {
		query, args, err := bulkWrite.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	return common.RevisionFromTransaction(newTxnID), nil
}

// NOTE(chriskirkland): ErrNoRows needs to be configured/dependency injected per sql-driver type
func (mds *mysqlDatastore) checkPreconditions(ctx context.Context, tx *sqlx.Tx, preconditions []*v1.Precondition) error {
	ctx, span := tracer.Start(ctx, "checkPreconditions")
	defer span.End()

	for _, precond := range preconditions {
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			query, args, err := selectQueryForFilter(precond.Filter).Limit(1).ToSql()
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
func selectQueryForFilter(filter *v1.RelationshipFilter) sq.SelectBuilder {
	query := queryTupleExists.Where(sq.Eq{common.ColNamespace: filter.ResourceType})

	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{common.ColObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{common.ColRelation: filter.OptionalRelation})
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{common.ColUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{common.ColUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{common.ColUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	return query
}

// NOTE(chriskirkland): nothing special in terms of mysql vs psq other than the concrete transaction types
func (mds *mysqlDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := mds.db.BeginTxx(ctx, nil)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteTuples, err)
	}
	defer tx.Rollback()

	if err := mds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	// Add clauses for the ResourceFilter
	query := deleteTuple.Where(sq.Eq{common.ColNamespace: filter.ResourceType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{common.ColObjectID: filter.OptionalResourceId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceId))
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{common.ColRelation: filter.OptionalRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalRelation))
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{common.ColUsersetNamespace: subjectFilter.SubjectType})
		tracerAttributes = append(tracerAttributes, common.SubNamespaceNameKey.String(subjectFilter.SubjectType))
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{common.ColUsersetObjectID: subjectFilter.OptionalSubjectId})
			tracerAttributes = append(tracerAttributes, common.SubObjectIDKey.String(subjectFilter.OptionalSubjectId))
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{common.ColUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
			tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(relationFilter.Relation))
		}
	}

	span.SetAttributes(tracerAttributes...)

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	query = query.Set(common.ColDeletedTxn, newTxnID)

	sql, args, err := query.ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToDeleteTuples, err)
	}

	if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	err = tx.Commit()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(common.ErrUnableToWriteTuples, err)
	}

	return common.RevisionFromTransaction(newTxnID), nil
}

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

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	errUnableToWriteTuples  = "unable to write tuples: %w"
	errUnableToDeleteTuples = "unable to delete tuples: %w"
)

var (
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

	queryTupleExists = psql.Select(colID).From(tableTuple)
)

func selectQueryForFilter(filter *v1.RelationshipFilter) sq.SelectBuilder {
	query := queryTupleExists.Where(sq.Eq{colNamespace: filter.ResourceType})

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

func (pgd *pgDatastore) checkPreconditions(ctx context.Context, tx pgx.Tx, preconditions []*v1.Precondition) error {
	ctx, span := tracer.Start(ctx, "checkPreconditions")
	defer span.End()

	for _, precond := range preconditions {
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			sql, args, err := selectQueryForFilter(precond.Filter).Limit(1).ToSql()
			if err != nil {
				return err
			}

			foundID := -1
			if err := tx.QueryRow(ctx, sql, args...).Scan(&foundID); err != nil {
				switch {
				case errors.Is(err, pgx.ErrNoRows) && precond.Operation == v1.Precondition_OPERATION_MUST_MATCH:
					return datastore.NewPreconditionFailedErr(precond)
				case errors.Is(err, pgx.ErrNoRows) && precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH:
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

func (pgd *pgDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback(ctx)

	if err := pgd.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	bulkWrite := writeTuple
	bulkWriteHasValues := false

	// Process the actual updates
	for _, mut := range mutations {
		rel := mut.Relationship

		if mut.Operation == v1.RelationshipUpdate_OPERATION_TOUCH || mut.Operation == v1.RelationshipUpdate_OPERATION_DELETE {
			sql, args, err := deleteTuple.Where(exactRelationshipClause(rel)).Set(colDeletedTxn, newTxnID).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}

			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
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
		sql, args, err := bulkWrite.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		_, err = tx.Exec(ctx, sql, args...)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return revisionFromTransaction(newTxnID), nil
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

func (pgd *pgDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}
	defer tx.Rollback(ctx)

	if err := pgd.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

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

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	query = query.Set(colDeletedTxn, newTxnID)

	sql, args, err := query.ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return revisionFromTransaction(newTxnID), nil
}

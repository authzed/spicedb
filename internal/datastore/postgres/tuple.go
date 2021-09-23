package postgres

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	errUnableToWriteTuples     = "unable to write tuples: %w"
	errUnableToDeleteTuples    = "unable to delete tuples: %w"
	errUnableToVerifyNamespace = "unable to verify namespace: %w"
	errUnableToVerifyRelation  = "unable to verify relation: %w"
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

func (pgd *pgDatastore) WriteTuples(ctx context.Context, preconditions []*v0.RelationTuple, mutations []*v0.RelationTupleUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback(ctx)

	span.AddEvent("Checking Preconditions")
	for _, tpl := range preconditions {
		sql, args, err := queryTupleExists.Where(exactTupleClause(tpl)).Limit(1).ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		foundID := -1
		if err := tx.QueryRow(ctx, sql, args...).Scan(&foundID); err != nil {
			if err == pgx.ErrNoRows {
				return datastore.NoRevision, datastore.NewPreconditionFailedErr(tpl)
			}
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	newTxnID, err := createNewTransaction(ctx, tx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	span.AddEvent("Writing Tuples")
	bulkWrite := writeTuple
	bulkWriteHasValues := false

	// Process the actual updates
	for _, mutation := range mutations {
		tpl := mutation.Tuple

		if mutation.Operation == v0.RelationTupleUpdate_TOUCH || mutation.Operation == v0.RelationTupleUpdate_DELETE {
			sql, args, err := deleteTuple.Where(exactTupleClause(tpl)).Set(colDeletedTxn, newTxnID).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}

			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}
		}

		if mutation.Operation == v0.RelationTupleUpdate_TOUCH || mutation.Operation == v0.RelationTupleUpdate_CREATE {
			bulkWrite = bulkWrite.Values(
				tpl.ObjectAndRelation.Namespace,
				tpl.ObjectAndRelation.ObjectId,
				tpl.ObjectAndRelation.Relation,
				tpl.User.GetUserset().Namespace,
				tpl.User.GetUserset().ObjectId,
				tpl.User.GetUserset().Relation,
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

func exactTupleClause(tpl *v0.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        tpl.ObjectAndRelation.Namespace,
		colObjectID:         tpl.ObjectAndRelation.ObjectId,
		colRelation:         tpl.ObjectAndRelation.Relation,
		colUsersetNamespace: tpl.User.GetUserset().Namespace,
		colUsersetObjectID:  tpl.User.GetUserset().ObjectId,
		colUsersetRelation:  tpl.User.GetUserset().Relation,
	}
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

func (pgd *pgDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Relationship, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := pgd.dbpool.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}
	defer tx.Rollback(ctx)

	span.AddEvent("Checking Preconditions")
	for _, relationship := range preconditions {
		sql, args, err := queryTupleExists.Where(exactRelationshipClause(relationship)).Limit(1).ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
		}

		foundID := -1
		if err := tx.QueryRow(ctx, sql, args...).Scan(&foundID); err != nil {
			if err == pgx.ErrNoRows {
				return datastore.NoRevision, datastore.NewPreconditionFailedErrFromRel(relationship)
			}
			return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
		}
	}

	span.AddEvent("Deleting Relationships")
	query := deleteTuple

	// Add clauses for the ResourceFilter
	resourceFilter := filter.ResourceFilter
	query = query.Where(sq.Eq{colNamespace: resourceFilter.ObjectType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(resourceFilter.ObjectType)}
	if filter.ResourceFilter.OptionalObjectId != "" {
		query = query.Where(sq.Eq{colObjectID: resourceFilter.OptionalObjectId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(resourceFilter.OptionalObjectId))
	}
	if filter.ResourceFilter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: resourceFilter.OptionalRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(resourceFilter.OptionalRelation))
	}

	// Add clauses for the SubjectFilter
	subjectFilter := filter.OptionalSubjectFilter
	if subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.ObjectType})
		tracerAttributes = append(tracerAttributes, common.SubNamespaceNameKey.String(subjectFilter.ObjectType))
		if subjectFilter.OptionalObjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalObjectId})
			tracerAttributes = append(tracerAttributes, common.SubObjectIDKey.String(subjectFilter.OptionalObjectId))
		}
		if subjectFilter.OptionalRelation != "" {
			query = query.Where(sq.Eq{colUsersetRelation: subjectFilter.OptionalRelation})
			tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(subjectFilter.OptionalRelation))
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

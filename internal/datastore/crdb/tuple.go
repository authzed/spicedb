package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
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
	upsertTupleSuffix = fmt.Sprintf(
		"ON CONFLICT (%s,%s,%s,%s,%s,%s) DO UPDATE SET %s = now() %s",
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colTimestamp,
		queryReturningTimestamp,
	)
	queryWriteTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).Suffix(upsertTupleSuffix)

	queryDeleteTuples = psql.Delete(tableTuple)

	queryTupleExists = psql.Select(colObjectID).From(tableTuple)
)

func (cds *crdbDatastore) WriteTuples(ctx context.Context, preconditions []*v0.RelationTuple, mutations []*v0.RelationTupleUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback(ctx)

	// Check the preconditions
	span.AddEvent("Checking Preconditions")
	for _, tpl := range preconditions {
		sql, args, err := queryTupleExists.Where(exactTupleClause(tpl)).Limit(1).ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		var foundObjectID string
		if err := tx.QueryRow(ctx, sql, args...).Scan(&foundObjectID); err != nil {
			if err == pgx.ErrNoRows {
				return datastore.NoRevision, datastore.NewPreconditionFailedErr(tpl)
			}
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	span.AddEvent("Writing Tuples")
	bulkWrite := queryWriteTuple
	var bulkWriteCount int64

	// Process the actual updates
	for _, mutation := range mutations {
		tpl := mutation.Tuple

		switch mutation.Operation {
		case v0.RelationTupleUpdate_TOUCH, v0.RelationTupleUpdate_CREATE:
			bulkWrite = bulkWrite.Values(
				tpl.ObjectAndRelation.Namespace,
				tpl.ObjectAndRelation.ObjectId,
				tpl.ObjectAndRelation.Relation,
				tpl.User.GetUserset().Namespace,
				tpl.User.GetUserset().ObjectId,
				tpl.User.GetUserset().Relation,
			)
			bulkWriteCount++
		case v0.RelationTupleUpdate_DELETE:
			sql, args, err := queryDeleteTuples.Where(exactTupleClause(tpl)).ToSql()
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}

			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
			}
		default:
			log.Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
			return datastore.NoRevision, fmt.Errorf(
				errUnableToWriteTuples,
				fmt.Errorf("unknown mutation operation: %s", mutation.Operation))
		}
	}

	var nowRevision decimal.Decimal
	if bulkWriteCount > 0 {
		sql, args, err := bulkWrite.ToSql()
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}

		if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	} else {
		nowRevision, err = readCRDBNow(ctx, tx)
		if err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return nowRevision, nil
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

func (cds *crdbDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Relationship, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := cds.conn.Begin(ctx)
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

		var foundObjectID string
		if err := tx.QueryRow(ctx, sql, args...).Scan(&foundObjectID); err != nil {
			if err == pgx.ErrNoRows {
				return datastore.NoRevision, datastore.NewPreconditionFailedErrFromRel(relationship)
			}
			return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
		}
	}

	span.AddEvent("Deleting Relationships")
	query := queryDeleteTuples.Suffix(queryReturningTimestamp)

	// Add clauses for the ResourceFilter
	resourceFilter := filter.ResourceFilter
	query = query.Where(sq.Eq{colNamespace: resourceFilter.ObjectType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(resourceFilter.ObjectType)}
	if resourceFilter.OptionalObjectId != "" {
		query = query.Where(sq.Eq{colObjectID: resourceFilter.OptionalObjectId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(resourceFilter.OptionalObjectId))
	}
	if resourceFilter.OptionalRelation != "" {
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

	sql, args, err := query.ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	var nowRevision decimal.Decimal
	if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return nowRevision, nil
}

package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
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

func selectQueryForFilter(filter *v1.RelationshipFilter) sq.SelectBuilder {
	query := queryTuples.Where(sq.Eq{colNamespace: filter.ResourceType})

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
		if subjectFilter.OptionalRelation != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis)})
		}
	}

	return query
}

func (cds *crdbDatastore) checkPreconditions(ctx context.Context, tx pgx.Tx, preconditions []*v1.Precondition) error {
	ctx, span := tracer.Start(ctx, "checkPreconditions")
	defer span.End()

	for _, precond := range preconditions {
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			sql, args, err := selectQueryForFilter(precond.Filter).Limit(1).ToSql()
			if err != nil {
				return err
			}

			rows, err := tx.Query(ctx, sql, args...)
			if err != nil {
				return err
			}

			exists := rows.Next()
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}

			if (precond.Operation == v1.Precondition_OPERATION_MUST_MATCH && !exists) ||
				(precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH && exists) {
				rows.Close()
				return datastore.NewPreconditionFailedErr(precond)
			}
			rows.Close()
		default:
			return fmt.Errorf("unspecified precondition operation")
		}
	}

	return nil
}

func (cds *crdbDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()

	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}
	defer tx.Rollback(ctx)

	if err := cds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	bulkWrite := queryWriteTuple
	var bulkWriteCount int64

	// Process the actual updates
	for _, mutation := range mutations {
		rel := mutation.Relationship

		switch mutation.Operation {
		case v1.RelationshipUpdate_OPERATION_TOUCH, v1.RelationshipUpdate_OPERATION_CREATE:
			bulkWrite = bulkWrite.Values(
				rel.Resource.ObjectType,
				rel.Resource.ObjectId,
				rel.Relation,
				rel.Subject.Object.ObjectType,
				rel.Subject.Object.ObjectId,
				stringz.DefaultEmpty(rel.Subject.OptionalRelation, datastore.Ellipsis),
			)
			bulkWriteCount++
		case v1.RelationshipUpdate_OPERATION_DELETE:
			sql, args, err := queryDeleteTuples.Where(exactRelationshipClause(rel)).ToSql()
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

func (cds *crdbDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()

	tx, err := cds.conn.Begin(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}
	defer tx.Rollback(ctx)

	if err := cds.checkPreconditions(ctx, tx, preconditions); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	query := queryDeleteTuples.Suffix(queryReturningTimestamp)

	// Add clauses for the ResourceFilter
	query = query.Where(sq.Eq{colNamespace: filter.ResourceType})
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
		if relation := subjectFilter.OptionalRelation; relation != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relation.Relation, datastore.Ellipsis)})
			tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(relation.Relation))
		}
	}
	span.SetAttributes(tracerAttributes...)

	sql, args, err := query.ToSql()
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	var nowRevision decimal.Decimal
	if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
		if err == pgx.ErrNoRows {
			// CRDB doesn't return the cluster_logical_timestamp if no rows were deleted
			// so we have to read it manually in the same transaction.
			nowRevision, err = readCRDBNow(ctx, tx)
			if err != nil {
				return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
			}
		} else {
			return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return nowRevision, nil
}

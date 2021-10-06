package crdb

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	errUnableToWriteTuples  = "unable to write tuples: %w"
	errUnableToDeleteTuples = "unable to delete tuples: %w"
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

	queryTouchTransaction = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ($1::text) ON CONFLICT (%s) DO UPDATE SET %s = now()",
		tableTransactions,
		colTransactionKey,
		colTransactionKey,
		colTimestamp,
	)

	queryTupleExists = psql.Select(colObjectID).From(tableTuple)
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

func (cds *crdbDatastore) checkPreconditions(ctx context.Context, tx pgx.Tx, keySet keySet, preconditions []*v1.Precondition) error {
	ctx, span := tracer.Start(ctx, "checkPreconditions")
	defer span.End()

	for _, precond := range preconditions {
		cds.AddOverlapKey(keySet, precond.Filter.ResourceType)
		if subjectFilter := precond.Filter.OptionalSubjectFilter; subjectFilter != nil {
			cds.AddOverlapKey(keySet, subjectFilter.SubjectType)
		}
		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH, v1.Precondition_OPERATION_MUST_MATCH:
			sql, args, err := selectQueryForFilter(precond.Filter).Limit(1).ToSql()
			if err != nil {
				return err
			}

			var foundObjectID string
			if err := tx.QueryRow(ctx, sql, args...).Scan(&foundObjectID); err != nil {
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

func (cds *crdbDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "WriteTuples")
	defer span.End()
	var nowRevision datastore.Revision

	if err := cds.execute(ctx, cds.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		keySet := newKeySet()
		if err := cds.checkPreconditions(ctx, tx, keySet, preconditions); err != nil {
			return err
		}
		bulkWrite := queryWriteTuple
		var bulkWriteCount int64

		// Process the actual updates
		for _, mutation := range mutations {
			rel := mutation.Relationship
			cds.AddOverlapKey(keySet, rel.Resource.ObjectType)
			cds.AddOverlapKey(keySet, rel.Subject.Object.ObjectType)

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
					return err
				}

				if err := overlap(ctx, tx, keySet); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, sql, args...); err != nil {
					return err
				}
			default:
				log.Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
				return fmt.Errorf("unknown mutation operation: %s", mutation.Operation)
			}
		}

		if bulkWriteCount > 0 {
			sql, args, err := bulkWrite.ToSql()
			if err != nil {
				return err
			}

			if err := overlap(ctx, tx, keySet); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
				return err
			}
		} else {
			var err error
			nowRevision, err = readCRDBNow(ctx, tx)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return nowRevision, nil
}

func overlap(ctx context.Context, tx pgx.Tx, keys keySet) error {
	for k := range keys {
		if err := tx.QueryRow(ctx, "SELECT key FROM transactions WHERE key = $1 FOR UPDATE", k).Scan(nil); err == pgx.ErrNoRows {
			if _, err := tx.Exec(ctx, "INSERT into transactions(key) values ($1) on conflict (key) do nothing", k); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
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

func (cds *crdbDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ctx, span := tracer.Start(datastore.SeparateContextWithTracing(ctx), "DeleteRelationships")
	defer span.End()
	var nowRevision datastore.Revision

	if err := cds.execute(ctx, cds.conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		keySet := newKeySet()
		if err := cds.checkPreconditions(ctx, tx, keySet, preconditions); err != nil {
			return err
		}

		// Add clauses for the ResourceFilter
		query := queryDeleteTuples.Suffix(queryReturningTimestamp).Where(sq.Eq{colNamespace: filter.ResourceType})
		tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
		if filter.OptionalResourceId != "" {
			query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
			tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceId))
		}
		if filter.OptionalRelation != "" {
			query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
			tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalRelation))
		}
		cds.AddOverlapKey(keySet, filter.ResourceType)

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
			cds.AddOverlapKey(keySet, subjectFilter.SubjectType)
		}
		span.SetAttributes(tracerAttributes...)
		sql, args, err := query.ToSql()
		if err != nil {
			return err
		}

		if err := overlap(ctx, tx, keySet); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, sql, args...).Scan(&nowRevision); err != nil {
			if err == pgx.ErrNoRows {
				// CRDB doesn't return the cluster_logical_timestamp if no rows were deleted
				// so we have to read it manually in the same transaction.
				nowRevision, err = readCRDBNow(ctx, tx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}

		return nil
	}); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	return nowRevision, nil
}

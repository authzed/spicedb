package postgres

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

var queryTuples = psql.Select(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
).From(tableTuple)

var schema = common.SchemaInformation{
	ColNamespace:        colNamespace,
	ColObjectID:         colObjectID,
	ColRelation:         colRelation,
	ColUsersetNamespace: colUsersetNamespace,
	ColUsersetObjectID:  colUsersetObjectID,
	ColUsersetRelation:  colUsersetRelation,
}

func (pgd *pgDatastore) QueryTuples(ctx context.Context, filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	initialQuery := queryTuples.
		Where(sq.Eq{colNamespace: filter.ResourceType}).
		Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{colDeletedTxn: liveDeletedTxnID},
			sq.Gt{colDeletedTxn: revision},
		})

	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}

	if filter.OptionalResourceID != "" {
		initialQuery = initialQuery.Where(sq.Eq{colObjectID: filter.OptionalResourceID})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceID))
	}

	if filter.OptionalResourceRelation != "" {
		initialQuery = initialQuery.Where(sq.Eq{colRelation: filter.OptionalResourceRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalResourceRelation))
	}

	baseSize := len(filter.ResourceType) + len(filter.OptionalResourceID) + len(filter.OptionalResourceRelation)

	return common.TupleQuery{
		Conn:                      pgd.dbpool,
		Schema:                    schema,
		PrepareTransaction:        nil,
		InitialQuery:              initialQuery,
		InitialQuerySizeEstimate:  baseSize,
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          tracerAttributes,
		DebugName:                 "QueryTuples",
		SplitAtEstimatedQuerySize: pgd.splitAtEstimatedQuerySize,
	}
}

func (pgd *pgDatastore) reverseQueryBase(revision datastore.Revision) common.TupleQuery {
	return common.TupleQuery{
		Conn:               pgd.dbpool,
		Schema:             schema,
		PrepareTransaction: nil,
		InitialQuery: queryTuples.
			Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revision},
			}),
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          []attribute.KeyValue{},
		DebugName:                 "ReverseQueryTuples",
		SplitAtEstimatedQuerySize: pgd.splitAtEstimatedQuerySize,
	}
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubject(ctx context.Context, subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return pgd.reverseQueryBase(revision).ReverseQueryTuplesFromSubject(subject)
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectRelation(ctx context.Context, subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return pgd.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation)
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectNamespace(ctx context.Context, subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return pgd.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectNamespace(subjectNamespace)
}

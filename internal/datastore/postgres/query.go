package postgres

import (
	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

var relationNameKey = attribute.Key("authzed.com/spicedb/relationName")

const errUnableToQueryTuples = "unable to query tuples: %w"

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

func (pgd *pgDatastore) QueryTuples(namespace string, revision datastore.Revision) datastore.TupleQuery {
	return common.TupleQuery{
		Conn:               pgd.dbpool,
		Schema:             schema,
		PrepareTransaction: nil,
		InitialQuery: queryTuples.
			Where(sq.Eq{colNamespace: namespace}).
			Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revision},
			}),
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          []attribute.KeyValue{common.NamespaceNameKey.String(namespace)},
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

func (pgd *pgDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return pgd.reverseQueryBase(revision).ReverseQueryTuplesFromSubject(subject)
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return pgd.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation)
}

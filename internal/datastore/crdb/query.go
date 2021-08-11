package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

const (
	querySetTransactionTime = "SET TRANSACTION AS OF SYSTEM TIME %s"
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

func (cds *crdbDatastore) QueryTuples(namespace string, revision datastore.Revision) datastore.TupleQuery {
	return common.TupleQuery{
		Conn:                      cds.conn,
		Schema:                    schema,
		PrepareTransaction:        cds.prepareTransaction,
		InitialQuery:              queryTuples.Where(sq.Eq{colNamespace: namespace}),
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          []attribute.KeyValue{common.NamespaceNameKey.String(namespace)},
		SplitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,
		DebugName:                 "QueryTuples",
	}
}

func (cds *crdbDatastore) prepareTransaction(ctx context.Context, tx pgx.Tx, revision datastore.Revision) error {
	setTxTime := fmt.Sprintf(querySetTransactionTime, revision)
	_, err := tx.Exec(ctx, setTxTime)
	return err
}

func (cds *crdbDatastore) reverseQueryBase(revision datastore.Revision) common.TupleQuery {
	return common.TupleQuery{
		Conn:                      cds.conn,
		Schema:                    schema,
		PrepareTransaction:        cds.prepareTransaction,
		InitialQuery:              queryTuples,
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          []attribute.KeyValue{},
		SplitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,
		DebugName:                 "ReverseQueryTuples",
	}
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubject(subject)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation)
}

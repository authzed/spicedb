package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
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

func (cds *crdbDatastore) QueryTuples(ctx context.Context, filter datastore.TupleQueryResourceFilter, revision datastore.Revision) datastore.TupleQuery {
	initialQuery := queryTuples.Where(sq.Eq{colNamespace: filter.ResourceType})
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
		Conn:                      cds.conn,
		Schema:                    schema,
		PrepareTransaction:        cds.prepareTransaction,
		InitialQuery:              initialQuery,
		InitialQuerySizeEstimate:  baseSize,
		Revision:                  revision,
		Tracer:                    tracer,
		TracerAttributes:          tracerAttributes,
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

func (cds *crdbDatastore) ReverseQueryTuplesFromSubject(ctx context.Context, subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubject(subject)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectRelation(ctx context.Context, subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectNamespace(ctx context.Context, subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectNamespace(subjectNamespace)
}

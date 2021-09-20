package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
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

func (cds *crdbDatastore) QueryTuples(resourceFilter *v1.ObjectFilter, revision datastore.Revision) datastore.TupleQuery {
	if resourceFilter == nil {
		panic("cannot call QueryTuples with a nil filter")
	}

	initialQuery := queryTuples.Where(sq.Eq{colNamespace: resourceFilter.ObjectType})
	tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(resourceFilter.ObjectType)}

	if resourceFilter.OptionalObjectId != "" {
		initialQuery = initialQuery.Where(sq.Eq{colObjectID: resourceFilter.OptionalObjectId})
		tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(resourceFilter.OptionalObjectId))
	}

	if resourceFilter.OptionalRelation != "" {
		initialQuery = initialQuery.Where(sq.Eq{colRelation: resourceFilter.OptionalRelation})
		tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(resourceFilter.OptionalRelation))
	}

	baseSize := len(resourceFilter.ObjectType) + len(resourceFilter.OptionalObjectId) + len(resourceFilter.OptionalRelation)

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

func (cds *crdbDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubject(subject)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return cds.reverseQueryBase(revision).ReverseQueryTuplesFromSubjectNamespace(subjectNamespace)
}

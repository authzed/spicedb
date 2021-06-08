package crdb

import (
	"context"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToQueryTuples = "unable to query tuples: %w"

	querySetTransactionTime = "SET TRANSACTION AS OF SYSTEM TIME %s"
)

var (
	queryTuples = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).From(tableTuple)
)

func (cds *crdbDatastore) QueryTuples(namespace string, revision datastore.Revision) datastore.TupleQuery {
	return crdbTupleQuery{
		commonTupleQuery: commonTupleQuery{
			conn:     cds.conn,
			query:    queryTuples.Where(sq.Eq{colNamespace: namespace}),
			revision: revision,
		},
		namespace: namespace,
	}
}

type commonTupleQuery struct {
	conn     *pgxpool.Pool
	query    sq.SelectBuilder
	revision datastore.Revision
}

type crdbTupleQuery struct {
	commonTupleQuery

	namespace string
}

func (ctq commonTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ctq.query = ctq.query.Limit(limit)
	return ctq
}

func (ctq crdbTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	ctq.query = ctq.query.Where(sq.Eq{colObjectID: objectID})
	return ctq
}

func (ctq crdbTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	ctq.query = ctq.query.Where(sq.Eq{colRelation: relation})
	return ctq
}

func (ctq crdbTupleQuery) WithUserset(userset *pb.ObjectAndRelation) datastore.TupleQuery {
	ctq.query = ctq.query.Where(sq.Eq{
		colUsersetNamespace: userset.Namespace,
		colUsersetObjectID:  userset.ObjectId,
		colUsersetRelation:  userset.Relation,
	})
	return ctq
}

func (ctq commonTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	ctx, span := tracer.Start(ctx, "ExecuteTupleQuery")
	defer span.End()

	sql, args, err := ctq.query.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	span.AddEvent("Query converted to SQL")

	tx, err := ctq.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer tx.Rollback(ctx)

	span.AddEvent("DB transaction established")

	setTxTime := fmt.Sprintf(querySetTransactionTime, ctq.revision)
	if _, err := tx.Exec(ctx, setTxTime); err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer rows.Close()

	span.AddEvent("Query issued to CRDB")

	var tuples []*pb.RelationTuple
	for rows.Next() {
		nextTuple := &pb.RelationTuple{
			ObjectAndRelation: &pb.ObjectAndRelation{},
			User: &pb.User{
				UserOneof: &pb.User_Userset{
					Userset: &pb.ObjectAndRelation{},
				},
			},
		}
		userset := nextTuple.User.GetUserset()
		err := rows.Scan(
			&nextTuple.ObjectAndRelation.Namespace,
			&nextTuple.ObjectAndRelation.ObjectId,
			&nextTuple.ObjectAndRelation.Relation,
			&userset.Namespace,
			&userset.ObjectId,
			&userset.Relation,
		)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}

		tuples = append(tuples, nextTuple)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	span.AddEvent("Tuples loaded", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))

	iter := datastore.NewSliceTupleIterator(tuples)

	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction(sql, args))

	return iter, nil
}

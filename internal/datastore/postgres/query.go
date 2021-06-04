package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	relationNameKey = attribute.Key("authzed.com/spicedb/relationName")
)

const errUnableToQueryTuples = "unable to query tuples: %w"

var (
	errClosedIterator = errors.New("unable to iterate: iterator closed")

	queryTuples = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	).From(tableTuple)
)

func (pgd *pgDatastore) QueryTuples(namespace string, revision uint64) datastore.TupleQuery {
	return pgTupleQuery{
		db: pgd.db,
		query: queryTuples.
			Where(sq.Eq{colNamespace: namespace}).
			Where(sq.LtOrEq{colCreatedTxn: revision}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revision},
			}),
		namespace: namespace,
	}
}

type pgTupleQuery struct {
	db        *sqlx.DB
	query     sq.SelectBuilder
	namespace string
	relation  string
}

func (ptq pgTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ptq.query = ptq.query.Limit(limit)
	return ptq
}

func (ptq pgTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	ptq.query = ptq.query.Where(sq.Eq{colObjectID: objectID})
	return ptq
}

func (ptq pgTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	ptq.query = ptq.query.Where(sq.Eq{colRelation: relation})
	ptq.relation = relation
	return ptq
}

func (ptq pgTupleQuery) WithUserset(userset *pb.ObjectAndRelation) datastore.TupleQuery {
	ptq.query = ptq.query.Where(sq.Eq{
		colUsersetNamespace: userset.Namespace,
		colUsersetObjectID:  userset.ObjectId,
		colUsersetRelation:  userset.Relation,
	})
	return ptq
}

func (ptq pgTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ExecuteTupleQuery")
	defer span.End()

	span.SetAttributes(namespaceNameKey.String(ptq.namespace))
	span.SetAttributes(relationNameKey.String(ptq.relation))

	span.AddEvent("DB transaction established")

	sql, args, err := ptq.query.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	span.AddEvent("Query converted to SQL")

	rows, err := ptq.db.QueryxContext(separateContextWithTracing(ctx), sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer rows.Close()

	span.AddEvent("Query issued to SQL")

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

	iter := &pgTupleIterator{
		tuples: tuples,
	}

	runtime.SetFinalizer(iter, func(iter *pgTupleIterator) {
		if !iter.closed {
			panic(fmt.Sprintf(
				"Tuple iterator garbage collected before Close() was called\n sql: %s\n args: %#v\n",
				sql,
				args,
			))
		}
	})

	return iter, nil
}

type pgTupleIterator struct {
	tuples []*pb.RelationTuple
	closed bool
	err    error
}

func (pti *pgTupleIterator) Next() *pb.RelationTuple {
	if pti.closed {
		pti.err = errClosedIterator
		return nil
	}

	if len(pti.tuples) > 0 {
		first := pti.tuples[0]
		pti.tuples = pti.tuples[1:]
		return first
	}

	return nil
}

func (pti *pgTupleIterator) Err() error {
	return pti.err
}

func (pti *pgTupleIterator) Close() {
	if pti.closed {
		panic("postgres iterator double closed")
	}

	pti.tuples = nil
	pti.closed = true
}

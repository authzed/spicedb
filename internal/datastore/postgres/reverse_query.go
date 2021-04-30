package postgres

import (
	"context"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (pgd *pgDatastore) ReverseQueryTuples(targetNamespaceName string, targetRelationName string, userset *pb.ObjectAndRelation, revision uint64) datastore.ReverseTupleQuery {
	return pgReverseTupleQuery{
		db:                  pgd.db,
		targetNamespaceName: targetNamespaceName,
		targetRelationName:  targetRelationName,
		userset:             userset,
		query: queryTuples.
			Where(sq.LtOrEq{colCreatedTxn: revision}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: revision},
			}).
			Where(sq.Eq{
				colNamespace: targetNamespaceName,
				colRelation:  targetRelationName,
			}).
			Where(sq.Eq{
				colUsersetNamespace: userset.Namespace,
				colUsersetObjectID:  userset.ObjectId,
				colUsersetRelation:  userset.Relation,
			}),
	}
}

type pgReverseTupleQuery struct {
	db    *sqlx.DB
	query sq.SelectBuilder

	targetNamespaceName string
	targetRelationName  string
	userset             *pb.ObjectAndRelation
}

var (
	targetNamespaceNameKey = attribute.Key("authzed.com/spicedb/targetNamespaceName")
	targetRelationNameKey  = attribute.Key("authzed.com/spicedb/targetRelationName")

	usersetNamespaceKey = attribute.Key("authzed.com/spicedb/usersetNamespaceName")
	usersetRelationKey  = attribute.Key("authzed.com/spicedb/usersetRelationName")
	usersetObjectIDKey  = attribute.Key("authzed.com/spicedb/usersetObjectId")
)

func (ptq pgReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ExecuteReverseTupleQuery")
	defer span.End()

	span.SetAttributes(targetNamespaceNameKey.String(ptq.targetNamespaceName))
	span.SetAttributes(targetRelationNameKey.String(ptq.targetRelationName))

	span.SetAttributes(usersetNamespaceKey.String(ptq.userset.Namespace))
	span.SetAttributes(usersetObjectIDKey.String(ptq.userset.ObjectId))
	span.SetAttributes(usersetRelationKey.String(ptq.userset.Relation))

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

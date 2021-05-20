package postgres

import (
	"context"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (pgd *pgDatastore) ReverseQueryTuples(revision decimal.Decimal) datastore.ReverseTupleQuery {
	return pgReverseTupleQuery{
		db: pgd.db,
		query: queryTuples.
			Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
			Where(sq.Or{
				sq.Eq{colDeletedTxn: liveDeletedTxnID},
				sq.Gt{colDeletedTxn: transactionFromRevision(revision)},
			}),
	}
}

type pgReverseTupleQuery struct {
	db    *sqlx.DB
	query sq.SelectBuilder

	objNamespaceName string
	objRelationName  string

	subNamespaceName string
	subRelationName  string
	subObjectId      string
}

var (
	objNamespaceNameKey = attribute.Key("authzed.com/spicedb/objNamespaceName")
	objRelationNameKey  = attribute.Key("authzed.com/spicedb/objRelationName")

	subNamespaceKey = attribute.Key("authzed.com/spicedb/subNamespaceName")
	subRelationKey  = attribute.Key("authzed.com/spicedb/subRelationName")
	subObjectIDKey  = attribute.Key("authzed.com/spicedb/subObjectId")
)

func (ptq pgReverseTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ptq.query = ptq.query.Limit(limit)
	return ptq
}

func (ptq pgReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	ptq.objNamespaceName = namespaceName
	ptq.objRelationName = relationName
	ptq.query = ptq.query.
		Where(sq.Eq{
			colNamespace: namespaceName,
			colRelation:  relationName,
		})
	return ptq
}

func (ptq pgReverseTupleQuery) WithSubjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	if ptq.subNamespaceName != "" {
		panic("WithSubject or WithSubjectRelation already called")
	}

	ptq.subNamespaceName = namespaceName
	ptq.subRelationName = relationName
	ptq.query = ptq.query.Where(sq.Eq{
		colUsersetNamespace: namespaceName,
		colUsersetRelation:  relationName,
	})
	return ptq
}

func (ptq pgReverseTupleQuery) WithSubject(onr *pb.ObjectAndRelation) datastore.ReverseTupleQuery {
	if ptq.subNamespaceName != "" {
		panic("WithSubject or WithSubjectRelation already called")
	}

	ptq.subNamespaceName = onr.Namespace
	ptq.subRelationName = onr.Relation
	ptq.subObjectId = onr.ObjectId
	ptq.query = ptq.query.Where(sq.Eq{
		colUsersetNamespace: onr.Namespace,
		colUsersetRelation:  onr.Relation,
		colUsersetObjectID:  onr.ObjectId,
	})
	return ptq
}

func (ptq pgReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "ExecuteReverseTupleQuery")
	defer span.End()

	span.SetAttributes(objNamespaceNameKey.String(ptq.objNamespaceName))
	span.SetAttributes(objRelationNameKey.String(ptq.objRelationName))

	span.SetAttributes(subNamespaceKey.String(ptq.subNamespaceName))
	span.SetAttributes(subObjectIDKey.String(ptq.subObjectId))
	span.SetAttributes(subRelationKey.String(ptq.subRelationName))

	if ptq.subNamespaceName == "" || ptq.subRelationName == "" {
		return nil, fmt.Errorf("missing subject namespace or relation")
	}

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

	iter := datastore.NewSliceTupleIterator(tuples)

	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction(sql, args))

	return iter, nil
}

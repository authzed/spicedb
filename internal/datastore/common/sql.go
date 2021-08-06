package common

import (
	"context"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

const (
	errUnableToQueryTuples = "unable to query tuples: %w"
)

var (
	NamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/namespaceName")

	objNamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/objNamespaceName")
	objRelationNameKey  = attribute.Key("authzed.com/spicedb/sql/objRelationName")
	objIDKey            = attribute.Key("authzed.com/spicedb/sql/objId")

	subNamespaceKey = attribute.Key("authzed.com/spicedb/sql/subNamespaceName")
	subRelationKey  = attribute.Key("authzed.com/spicedb/sql/subRelationName")
	subObjectIDKey  = attribute.Key("authzed.com/spicedb/sql/subObjectId")

	limitKey = attribute.Key("authzed.com/spicedb/sql/limit")
)

// SchemaInformation holds the schema information from the SQL datastore implementation.
type SchemaInformation struct {
	TableTuple          string
	ColNamespace        string
	ColObjectID         string
	ColRelation         string
	ColUsersetNamespace string
	ColUsersetObjectID  string
	ColUsersetRelation  string
}

// TransactionPreparer is a function provided by the datastore to prepare the transaction before
// the tuple query is run.
type TransactionPreparer func(ctx context.Context, tx pgx.Tx, revision datastore.Revision) error

// TupleQuery is a tuple query builder and runner shared by SQL implementations of the
// datastore.
type TupleQuery struct {
	Conn               *pgxpool.Pool
	Schema             SchemaInformation
	PrepareTransaction TransactionPreparer

	Query    sq.SelectBuilder
	Revision datastore.Revision

	Tracer           trace.Tracer
	TracerAttributes []attribute.KeyValue

	DebugName string
}

func (ctq TupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, limitKey.Int64(int64(limit)))
	ctq.Query = ctq.Query.Limit(limit)
	return ctq
}

func (ctq TupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, objIDKey.String(objectID))
	ctq.Query = ctq.Query.Where(sq.Eq{ctq.Schema.ColObjectID: objectID})
	return ctq
}

func (ctq TupleQuery) WithRelation(relation string) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, objRelationNameKey.String(relation))
	ctq.Query = ctq.Query.Where(sq.Eq{ctq.Schema.ColRelation: relation})
	return ctq
}

func (ctq TupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if usersets == nil || len(usersets) == 0 {
		panic("Cannot send empty usersets into query")
	}

	orClause := sq.Or{sq.Eq{
		ctq.Schema.ColUsersetNamespace: usersets[0].Namespace,
		ctq.Schema.ColUsersetObjectID:  usersets[0].ObjectId,
		ctq.Schema.ColUsersetRelation:  usersets[0].Relation,
	}}

	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(usersets[0].Namespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subObjectIDKey.String(usersets[0].ObjectId))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(usersets[0].Relation))

	for _, userset := range usersets[1:] {
		ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(userset.Namespace))
		ctq.TracerAttributes = append(ctq.TracerAttributes, subObjectIDKey.String(userset.ObjectId))
		ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(userset.Relation))

		orClause = append(orClause, sq.Eq{
			ctq.Schema.ColUsersetNamespace: userset.Namespace,
			ctq.Schema.ColUsersetObjectID:  userset.ObjectId,
			ctq.Schema.ColUsersetRelation:  userset.Relation,
		})
	}

	ctq.Query = ctq.Query.Where(orClause)
	return ctq
}

func (ctq TupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	return ctq.WithUsersets([]*v0.ObjectAndRelation{userset})
}

func (ctq TupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	name := fmt.Sprintf("Execute%s", ctq.DebugName)
	ctx, span := ctq.Tracer.Start(ctx, name)
	defer span.End()

	span.SetAttributes(ctq.TracerAttributes...)

	sql, args, err := ctq.Query.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	span.AddEvent("Query converted to SQL")

	tx, err := ctq.Conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer tx.Rollback(ctx)

	span.AddEvent("DB transaction established")

	if ctq.PrepareTransaction != nil {
		err = ctq.PrepareTransaction(ctx, tx, ctq.Revision)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}

		span.AddEvent("Transaction prepared")
	}

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer rows.Close()

	span.AddEvent("Query issued to database")

	var tuples []*v0.RelationTuple
	for rows.Next() {
		nextTuple := &v0.RelationTuple{
			ObjectAndRelation: &v0.ObjectAndRelation{},
			User: &v0.User{
				UserOneof: &v0.User_Userset{
					Userset: &v0.ObjectAndRelation{},
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

// ReverseQueryTuplesFromSubjectRelation constructs a ReverseTupleQuery from this tuple query.
func (ctq TupleQuery) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string) datastore.ReverseTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(subjectNamespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(subjectRelation))

	ctq.Query = ctq.Query.Where(sq.Eq{
		ctq.Schema.ColUsersetNamespace: subjectNamespace,
		ctq.Schema.ColUsersetRelation:  subjectRelation,
	})
	return ReverseTupleQuery{ctq}
}

// ReverseQueryTuplesFromSubject constructs a ReverseTupleQuery from this tuple query.
func (ctq TupleQuery) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation) datastore.ReverseTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(subject.Namespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subObjectIDKey.String(subject.ObjectId))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(subject.Relation))

	ctq.Query = ctq.Query.Where(sq.Eq{
		ctq.Schema.ColUsersetNamespace: subject.Namespace,
		ctq.Schema.ColUsersetObjectID:  subject.ObjectId,
		ctq.Schema.ColUsersetRelation:  subject.Relation,
	})
	return ReverseTupleQuery{ctq}
}

// ReverseTupleQuery is a common reverse tuple query implementation for SQL datastore implementations.
type ReverseTupleQuery struct {
	TupleQuery
}

func (ctq ReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, objNamespaceNameKey.String(namespaceName))
	ctq.TracerAttributes = append(ctq.TracerAttributes, objRelationNameKey.String(relationName))

	ctq.Query = ctq.Query.
		Where(sq.Eq{
			ctq.Schema.ColNamespace: namespaceName,
			ctq.Schema.ColRelation:  relationName,
		})
	return ctq
}

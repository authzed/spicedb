package common

import (
	"context"
	"fmt"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
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
	// NamespaceNameKey is the attribute name for namespaces in tracers.
	NamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/namespaceName")

	objNamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/objNamespaceName")
	objRelationNameKey  = attribute.Key("authzed.com/spicedb/sql/objRelationName")
	objIDKey            = attribute.Key("authzed.com/spicedb/sql/objId")

	subNamespaceKey = attribute.Key("authzed.com/spicedb/sql/subNamespaceName")
	subRelationKey  = attribute.Key("authzed.com/spicedb/sql/subRelationName")
	subObjectIDKey  = attribute.Key("authzed.com/spicedb/sql/subObjectId")

	limitKey = attribute.Key("authzed.com/spicedb/sql/limit")
)

// DefaultSplitAtEstimatedQuerySize is the default allowed estimated query size before the
// TupleQuery will split the query into multiple calls.
//
// In Postgres, it appears to be 1GB: https://dba.stackexchange.com/questions/131399/is-there-a-maximum-length-constraint-for-a-postgres-query
// In CockroachDB, the maximum is 16MiB: https://www.cockroachlabs.com/docs/stable/known-limitations.html#size-limits-on-statement-input-from-sql-clients
// As a result, we go with half of that to be on the safe side, since the estimate doesn't include
// the field names or operators.
const DefaultSplitAtEstimatedQuerySize = 8 * units.MiB

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

	InitialQuery sq.SelectBuilder
	Revision     datastore.Revision

	Tracer           trace.Tracer
	TracerAttributes []attribute.KeyValue

	DebugName                 string
	SplitAtEstimatedQuerySize units.Base2Bytes

	limit    *uint64
	objectID *string
	relation *string
	usersets *[]*v0.ObjectAndRelation
}

func (ctq TupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	if ctq.limit != nil {
		panic("Called Limit twice")
	}

	ctq.TracerAttributes = append(ctq.TracerAttributes, limitKey.Int64(int64(limit)))
	ctq.limit = &limit
	return ctq
}

func (ctq TupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	if ctq.objectID != nil {
		panic("Called WithObjectID twice")
	}

	ctq.TracerAttributes = append(ctq.TracerAttributes, objIDKey.String(objectID))
	ctq.objectID = &objectID
	return ctq
}

func (ctq TupleQuery) WithRelation(relation string) datastore.TupleQuery {
	if ctq.relation != nil {
		panic("Called WithRelation twice")
	}

	ctq.TracerAttributes = append(ctq.TracerAttributes, objRelationNameKey.String(relation))
	ctq.relation = &relation
	return ctq
}

func (ctq TupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if usersets == nil || len(usersets) == 0 {
		panic("Cannot send nil or empty usersets into query")
	}

	if ctq.usersets != nil {
		panic("Called WithUsersets twice")
	}

	ctq.usersets = &usersets
	return ctq
}

func (ctq TupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(userset.Namespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subObjectIDKey.String(userset.ObjectId))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(userset.Relation))
	return ctq.WithUsersets([]*v0.ObjectAndRelation{userset})
}

func (ctq TupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	// Build the query/queries to execute.
	query := ctq.InitialQuery
	baseEstimatedDataSize := 0

	// Add the base query filters.
	if ctq.objectID != nil {
		query = query.Where(sq.Eq{ctq.Schema.ColObjectID: *ctq.objectID})
		baseEstimatedDataSize += len(*ctq.objectID)
	}

	if ctq.relation != nil {
		query = query.Where(sq.Eq{ctq.Schema.ColRelation: *ctq.relation})
		baseEstimatedDataSize += len(*ctq.relation)
	}

	// Determine split points for the query based on the usersets, if any.
	queries := []sq.SelectBuilder{}
	if ctq.usersets != nil {
		splitIndexes := []int{}
		usersets := *ctq.usersets

		currentEstimatedDataSize := baseEstimatedDataSize
		currentUsersetCount := 0

		for index, userset := range usersets {
			estimatedUsersetSize := len(userset.Namespace) + len(userset.ObjectId) + len(userset.Relation)
			if currentUsersetCount > 0 && estimatedUsersetSize+currentEstimatedDataSize >= int(ctq.SplitAtEstimatedQuerySize) {
				currentEstimatedDataSize = baseEstimatedDataSize
				splitIndexes = append(splitIndexes, index)
			}

			currentUsersetCount++
			currentEstimatedDataSize += estimatedUsersetSize
		}

		addQueryWithUsersets := func(usersets []*v0.ObjectAndRelation) {
			orClause := sq.Or{}
			if len(usersets) == 0 {
				panic("Got empty sub usersets")
			}

			for _, userset := range usersets {
				orClause = append(orClause, sq.Eq{
					ctq.Schema.ColUsersetNamespace: userset.Namespace,
					ctq.Schema.ColUsersetObjectID:  userset.ObjectId,
					ctq.Schema.ColUsersetRelation:  userset.Relation,
				})
			}

			queries = append(queries, query.Where(orClause))
		}

		startIndex := 0
		for _, splitIndex := range splitIndexes {
			addQueryWithUsersets(usersets[startIndex:splitIndex])
			startIndex = splitIndex
		}

		addQueryWithUsersets(usersets[startIndex:])
	} else {
		queries = append(queries, query)
	}

	// Execute each query.
	// TODO: make parallel.
	name := fmt.Sprintf("Execute%s", ctq.DebugName)
	ctx, span := ctq.Tracer.Start(ctx, name)
	defer span.End()

	var tuples []*v0.RelationTuple
	for index, query := range queries {
		var newLimit uint64
		if ctq.limit != nil {
			newLimit = *ctq.limit - uint64(len(tuples))
			if newLimit <= 0 {
				break
			}

			query = query.Limit(newLimit)
		}

		foundTuples, err := ctq.executeQuery(ctx, query, index, newLimit)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, foundTuples...)
	}

	iter := datastore.NewSliceTupleIterator(tuples)
	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction())
	return iter, nil
}

func (ctq TupleQuery) executeQuery(ctx context.Context, query sq.SelectBuilder, index int, limit uint64) ([]*v0.RelationTuple, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	name := fmt.Sprintf("Query-%d", index)
	ctx, span := ctq.Tracer.Start(ctx, name)
	defer span.End()

	span.SetAttributes(ctq.TracerAttributes...)

	sql, args, err := query.ToSql()
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
		if limit > 0 && len(tuples) >= int(limit) {
			return tuples, nil
		}

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
	return tuples, nil
}

// ReverseQueryTuplesFromSubjectRelation constructs a ReverseTupleQuery from this tuple query.
func (ctq TupleQuery) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string) datastore.ReverseTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(subjectNamespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(subjectRelation))

	ctq.InitialQuery = ctq.InitialQuery.Where(sq.Eq{
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

	ctq.InitialQuery = ctq.InitialQuery.Where(sq.Eq{
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

	ctq.InitialQuery = ctq.InitialQuery.
		Where(sq.Eq{
			ctq.Schema.ColNamespace: namespaceName,
			ctq.Schema.ColRelation:  relationName,
		})

	return ctq
}

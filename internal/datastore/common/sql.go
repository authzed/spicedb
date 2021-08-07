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

	Query    sq.SelectBuilder
	Revision datastore.Revision

	Tracer           trace.Tracer
	TracerAttributes []attribute.KeyValue

	DebugName                 string
	SplitAtEstimatedQuerySize units.Base2Bytes

	estimatedDataSize int
}

func (ctq TupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, limitKey.Int64(int64(limit)))
	ctq.Query = ctq.Query.Limit(limit)
	return ctq
}

func (ctq TupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, objIDKey.String(objectID))
	ctq.Query = ctq.Query.Where(sq.Eq{ctq.Schema.ColObjectID: objectID})
	ctq.estimatedDataSize += len(objectID)
	return ctq
}

func (ctq TupleQuery) WithRelation(relation string) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, objRelationNameKey.String(relation))
	ctq.Query = ctq.Query.Where(sq.Eq{ctq.Schema.ColRelation: relation})
	ctq.estimatedDataSize += len(relation)
	return ctq
}

func (ctq TupleQuery) clone() TupleQuery {
	return TupleQuery{
		Conn:               ctq.Conn,
		Schema:             ctq.Schema,
		PrepareTransaction: ctq.PrepareTransaction,

		Query:    ctq.Query,
		Revision: ctq.Revision,

		Tracer:           ctq.Tracer,
		TracerAttributes: ctq.TracerAttributes,

		DebugName: ctq.DebugName,

		estimatedDataSize: ctq.estimatedDataSize,
	}
}

func (ctq TupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if usersets == nil || len(usersets) == 0 {
		panic("Cannot send nil or empty usersets into query")
	}

	// Determine where the query should be split based on the estimated size.
	splitIndexes := []int{}

	currentEstimatedDataSize := ctq.estimatedDataSize
	currentUsersetCount := 0

	for index, userset := range usersets {
		estimatedUsersetSize := len(userset.Namespace) + len(userset.ObjectId) + len(userset.Relation)
		if currentUsersetCount > 0 && estimatedUsersetSize+currentEstimatedDataSize >= int(ctq.SplitAtEstimatedQuerySize) {
			currentEstimatedDataSize = ctq.estimatedDataSize
			splitIndexes = append(splitIndexes, index)
		}

		currentUsersetCount++
		currentEstimatedDataSize += estimatedUsersetSize
	}

	if len(splitIndexes) == 0 {
		// Normal single query.
		orClause := sq.Or{}
		for _, userset := range usersets {
			orClause = append(orClause, sq.Eq{
				ctq.Schema.ColUsersetNamespace: userset.Namespace,
				ctq.Schema.ColUsersetObjectID:  userset.ObjectId,
				ctq.Schema.ColUsersetRelation:  userset.Relation,
			})
		}

		ctq.Query = ctq.Query.Where(orClause)
		return ctq
	}

	// Otherwise, branch from the current query and apply a subset of the usersets to each.
	subQueries := []datastore.TupleQuery{}
	startIndex := 0
	for _, splitIndex := range splitIndexes {
		subQueries = append(subQueries, ctq.buildSubQuery(usersets[startIndex:splitIndex]))
		startIndex = splitIndex
	}

	subQueries = append(subQueries, ctq.buildSubQuery(usersets[startIndex:]))

	return newCombinedQuery(subQueries)
}

func (ctq TupleQuery) buildSubQuery(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	subQuery := ctq.clone()

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

	subQuery.Query = subQuery.Query.Where(orClause)
	return subQuery
}

func (ctq TupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	ctq.TracerAttributes = append(ctq.TracerAttributes, subNamespaceKey.String(userset.Namespace))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subObjectIDKey.String(userset.ObjectId))
	ctq.TracerAttributes = append(ctq.TracerAttributes, subRelationKey.String(userset.Relation))

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

	ctq.estimatedDataSize += len(subject.Namespace) + len(subject.ObjectId) + len(subject.Relation)
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

	ctq.estimatedDataSize += len(namespaceName) + len(relationName)
	return ctq
}

type combinedTupleQuery struct {
	subQueries []datastore.TupleQuery
	limit      uint64
}

func newCombinedQuery(subQueries []datastore.TupleQuery) datastore.TupleQuery {
	return combinedTupleQuery{subQueries, 0}
}

func (ctq combinedTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	for index, subQuery := range ctq.subQueries {
		ctq.subQueries[index] = subQuery.WithObjectID(objectID)
	}
	return ctq
}

func (ctq combinedTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	for index, subQuery := range ctq.subQueries {
		ctq.subQueries[index] = subQuery.WithRelation(relation)
	}
	return ctq
}

func (ctq combinedTupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	for index, subQuery := range ctq.subQueries {
		ctq.subQueries[index] = subQuery.WithUserset(userset)
	}
	return ctq
}

func (ctq combinedTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	for index, subQuery := range ctq.subQueries {
		ctq.subQueries[index] = subQuery.WithUsersets(usersets)
	}
	return ctq
}

func (ctq combinedTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	ctq.limit = limit
	for _, subQuery := range ctq.subQueries {
		subQuery.Limit(limit)
	}
	return ctq
}

func (ctq combinedTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	// TODO(jschorr): make this a parallel execute
	return &combinedTupleIterator{
		ctx:                  ctx,
		ctq:                  ctq,
		currentIterator:      nil,
		currentIteratorIndex: 0,
		returnedResultCount:  0,
		closed:               false,
		err:                  nil,
	}, nil
}

type combinedTupleIterator struct {
	ctx                  context.Context
	ctq                  combinedTupleQuery
	currentIterator      datastore.TupleIterator
	currentIteratorIndex int
	returnedResultCount  uint64
	closed               bool
	err                  error
}

func (cti *combinedTupleIterator) Next() *v0.RelationTuple {
	if cti.closed {
		return nil
	}

	if cti.ctq.limit > 0 && cti.returnedResultCount >= cti.ctq.limit {
		cti.Close()
		return nil
	}

	for {
		it := cti.currentIterator
		if it == nil {
			if !cti.advanceIterator() {
				return nil
			}
			continue
		}

		result := it.Next()
		if result == nil {
			it.Close()
			cti.currentIteratorIndex++
			cti.currentIterator = nil
			continue
		}
		cti.returnedResultCount++
		return result
	}
}

func (cti *combinedTupleIterator) Err() error {
	if cti.err != nil {
		return cti.err
	}

	if cti.closed {
		return nil
	}

	it := cti.currentIterator
	if it == nil {
		return nil
	}

	return it.Err()
}

func (cti *combinedTupleIterator) Close() {
	if cti.closed {
		return
	}

	cti.closed = true
	it := cti.currentIterator
	if it != nil {
		cti.currentIterator = nil
		it.Close()
	}
}

func (cti *combinedTupleIterator) advanceIterator() bool {
	if cti.currentIteratorIndex >= len(cti.ctq.subQueries) {
		return false
	}

	it, err := cti.ctq.subQueries[cti.currentIteratorIndex].Execute(cti.ctx)
	if err != nil {
		cti.err = err
		return false
	}

	cti.currentIterator = it
	return true
}

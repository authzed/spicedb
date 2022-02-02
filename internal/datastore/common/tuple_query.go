package common

import (
	"context"
	"fmt"
	"runtime"

	"github.com/alecthomas/units"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/spicedb/internal/datastore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// GeneralTransactionPreparer is a function provided by the datastore to prepare the transaction before
// the tuple query is run.
type GeneralTransactionPreparer func(ctx context.Context, tx TransactionWrapper, revision datastore.Revision) error

// GeneralTupleQuerySplitter is a tuple query runner shared by SQL implementations of the datastore.
type GeneralTupleQuerySplitter struct {
	DbConn                    DbConnection
	PrepareTransaction        GeneralTransactionPreparer
	SplitAtEstimatedQuerySize units.Base2Bytes

	FilteredQueryBuilder SchemaQueryFilterer
	Revision             datastore.Revision
	Limit                *uint64
	Usersets             []*v0.ObjectAndRelation

	DebugName string
	Tracer    trace.Tracer
}

// SplitAndExecute executes one or more SQL queries based on the data bound to the
// TupleQuerySplitter instance.
func (ctq GeneralTupleQuerySplitter) SplitAndExecute(ctx context.Context) (datastore.TupleIterator, error) {
	// Determine split points for the query based on the usersets, if any.
	queries := []SchemaQueryFilterer{}
	if len(ctq.Usersets) > 0 {
		splitIndexes := []int{}

		currentEstimatedDataSize := ctq.FilteredQueryBuilder.currentEstimatedSize
		currentUsersetCount := 0

		for index, userset := range ctq.Usersets {
			estimatedUsersetSize := len(userset.Namespace) + len(userset.ObjectId) + len(userset.Relation)
			if currentUsersetCount > 0 && estimatedUsersetSize+currentEstimatedDataSize >= int(ctq.SplitAtEstimatedQuerySize) {
				currentEstimatedDataSize = ctq.FilteredQueryBuilder.currentEstimatedSize
				splitIndexes = append(splitIndexes, index)
			}

			currentUsersetCount++
			currentEstimatedDataSize += estimatedUsersetSize
		}

		startIndex := 0
		for _, splitIndex := range splitIndexes {
			queries = append(queries, ctq.FilteredQueryBuilder.FilterToUsersets(ctq.Usersets[startIndex:splitIndex]))
			startIndex = splitIndex
		}

		queries = append(queries, ctq.FilteredQueryBuilder.FilterToUsersets(ctq.Usersets[startIndex:]))
	} else {
		queries = append(queries, ctq.FilteredQueryBuilder)
	}

	// Execute each query.
	// TODO: make parallel.
	name := fmt.Sprintf("Execute%s", ctq.DebugName)
	ctx, span := ctq.Tracer.Start(ctx, name)
	defer span.End()

	var tuples []*v0.RelationTuple
	for index, query := range queries {
		var newLimit uint64
		if ctq.Limit != nil {
			newLimit = *ctq.Limit - uint64(len(tuples))
			if newLimit <= 0 {
				break
			}

			query = query.Limit(newLimit)
		}

		foundTuples, err := ctq.executeSingleQuery(ctx, query, index, newLimit)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, foundTuples...)
	}

	iter := datastore.NewSliceTupleIterator(tuples)
	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction())
	return iter, nil
}

func (ctq GeneralTupleQuerySplitter) executeSingleQuery(ctx context.Context, query SchemaQueryFilterer, index int, limit uint64) ([]*v0.RelationTuple, error) {
	ctx = datastore.SeparateContextWithTracing(ctx)

	name := fmt.Sprintf("Query-%d", index)
	ctx, span := ctq.Tracer.Start(ctx, name)
	defer span.End()

	span.SetAttributes(query.tracerAttributes...)

	sql, args, err := query.queryBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
	}

	span.AddEvent("Query converted to SQL")

	tx, err := ctq.DbConn.BeginTxx(ctx, true)
	if err != nil {
		return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
	}
	defer tx.Rollback(ctx)

	span.AddEvent("DB transaction established")

	if ctq.PrepareTransaction != nil {
		err = ctq.PrepareTransaction(ctx, tx, ctq.Revision)
		if err != nil {
			return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
		}

		span.AddEvent("Transaction prepared")
	}

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
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
			return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
		}

		tuples = append(tuples, nextTuple)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
	}

	span.AddEvent("Tuples loaded", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))
	return tuples, nil
}

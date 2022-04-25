package common

import (
	"context"
	"fmt"
	"math"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

const (
	errUnableToQueryTuples = "unable to query tuples: %w"
)

var (
	// ObjNamespaceNameKey is a tracing attribute representing the resource
	// object type.
	ObjNamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/objNamespaceName")

	// ObjRelationNameKey is a tracing attribute representing the resource
	// relation.
	ObjRelationNameKey = attribute.Key("authzed.com/spicedb/sql/objRelationName")

	// ObjIDKey is a tracing attribute representing the resource object ID.
	ObjIDKey = attribute.Key("authzed.com/spicedb/sql/objId")

	// SubNamespaceNameKey is a tracing attribute representing the subject object
	// type.
	SubNamespaceNameKey = attribute.Key("authzed.com/spicedb/sql/subNamespaceName")

	// SubRelationNameKey is a tracing attribute representing the subject
	// relation.
	SubRelationNameKey = attribute.Key("authzed.com/spicedb/sql/subRelationName")

	// SubObjectIDKey is a tracing attribute representing the the subject object
	// ID.
	SubObjectIDKey = attribute.Key("authzed.com/spicedb/sql/subObjectId")

	limitKey = attribute.Key("authzed.com/spicedb/sql/limit")

	tracer = otel.Tracer("spicedb/internal/datastore/common")
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

// SchemaQueryFilterer wraps a SchemaInformation and SelectBuilder to give an opinionated
// way to build query objects.
type SchemaQueryFilterer struct {
	schema           SchemaInformation
	queryBuilder     sq.SelectBuilder
	tracerAttributes []attribute.KeyValue
}

// NewSchemaQueryFilterer creates a new SchemaQueryFilterer object.
func NewSchemaQueryFilterer(schema SchemaInformation, initialQuery sq.SelectBuilder) SchemaQueryFilterer {
	return SchemaQueryFilterer{
		schema:       schema,
		queryBuilder: initialQuery,
	}
}

// FilterToResourceType returns a new SchemaQueryFilterer that is limited to resources of the
// specified type.
func (sqf SchemaQueryFilterer) FilterToResourceType(resourceType string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColNamespace: resourceType})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjNamespaceNameKey.String(resourceType))
	return sqf
}

// FilterToResourceID returns a new SchemaQueryFilterer that is limited to resources with the
// specified ID.
func (sqf SchemaQueryFilterer) FilterToResourceID(objectID string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColObjectID: objectID})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjIDKey.String(objectID))
	return sqf
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColRelation: relation})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjRelationNameKey.String(relation))
	return sqf
}

// FilterToSubjectFilter returns a new SchemaQueryFilterer that is limited to resources with
// subjects that match the specified filter.
func (sqf SchemaQueryFilterer) FilterToSubjectFilter(filter *v1.SubjectFilter) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetNamespace: filter.SubjectType})
	sqf.tracerAttributes = append(sqf.tracerAttributes, SubNamespaceNameKey.String(filter.SubjectType))

	if filter.OptionalSubjectId != "" {
		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetObjectID: filter.OptionalSubjectId})
		sqf.tracerAttributes = append(sqf.tracerAttributes, SubObjectIDKey.String(filter.OptionalSubjectId))
	}

	if filter.OptionalRelation != nil {
		dsRelationName := stringz.DefaultEmpty(filter.OptionalRelation.Relation, datastore.Ellipsis)

		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
		sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
	}

	return sqf
}

// FilterToUsersets returns a new SchemaQueryFilterer that is limited to resources with subjects
// in the specified list of usersets. Nil or empty usersets parameter does not affect the underlying
// query.
func (sqf SchemaQueryFilterer) filterToUsersets(usersets []*core.ObjectAndRelation) SchemaQueryFilterer {
	if len(usersets) == 0 {
		return sqf
	}

	orClause := sq.Or{}
	for _, userset := range usersets {
		orClause = append(orClause, sq.Eq{
			sqf.schema.ColUsersetNamespace: userset.Namespace,
			sqf.schema.ColUsersetObjectID:  userset.ObjectId,
			sqf.schema.ColUsersetRelation:  userset.Relation,
		})
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(orClause)

	return sqf
}

// Limit returns a new SchemaQueryFilterer which is limited to the specified number of results.
func (sqf SchemaQueryFilterer) limit(limit uint64) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Limit(limit)
	sqf.tracerAttributes = append(sqf.tracerAttributes, limitKey.Int64(int64(limit)))
	return sqf
}

// TupleQuerySplitter is a tuple query runner shared by SQL implementations of the datastore.
type TupleQuerySplitter struct {
	Executor         ExecuteQueryFunc
	UsersetBatchSize int
}

// SplitAndExecuteQuery is used to split up the usersets in a very large query and execute
// them as separate queries.
func (tqs TupleQuerySplitter) SplitAndExecuteQuery(
	ctx context.Context,
	query SchemaQueryFilterer,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	ctx, span := tracer.Start(ctx, "SplitAndExecuteQuery")
	defer span.End()
	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	var tuples []*core.RelationTuple
	remainingLimit := math.MaxInt
	if queryOpts.Limit != nil {
		remainingLimit = int(*queryOpts.Limit)
	}

	remainingUsersets := queryOpts.Usersets
	for remaining := 1; remaining > 0; remaining = len(remainingUsersets) {
		upperBound := len(remainingUsersets)
		if upperBound > tqs.UsersetBatchSize {
			upperBound = tqs.UsersetBatchSize
		}

		batch := remainingUsersets[:upperBound]
		toExecute := query.limit(uint64(remainingLimit)).filterToUsersets(batch)

		sql, args, err := toExecute.queryBuilder.ToSql()
		if err != nil {
			return nil, err
		}

		queryTuples, err := tqs.Executor(ctx, revision, sql, args)
		if err != nil {
			return nil, err
		}

		if len(queryTuples) > remainingLimit {
			queryTuples = queryTuples[:remainingLimit]
		}

		tuples = append(tuples, queryTuples...)
		remainingUsersets = remainingUsersets[upperBound:]
	}

	iter := datastore.NewSliceTupleIterator(tuples)
	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction())
	return iter, nil
}

// ExecuteQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteQueryFunc func(ctx context.Context, revision datastore.Revision, sql string, args []interface{}) ([]*core.RelationTuple, error)

// TransactionPreparer is a function provided by the datastore to prepare the transaction before
// the tuple query is run.
type TransactionPreparer func(ctx context.Context, tx pgx.Tx, revision datastore.Revision) error

func NewPGXExecutor(conn *pgxpool.Pool, prepTxn TransactionPreparer) ExecuteQueryFunc {
	return func(ctx context.Context, revision datastore.Revision, sql string, args []interface{}) ([]*core.RelationTuple, error) {
		ctx = datastore.SeparateContextWithTracing(ctx)

		span := trace.SpanFromContext(ctx)

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}
		defer tx.Rollback(ctx)

		span.AddEvent("DB transaction established")

		if prepTxn != nil {
			err = prepTxn(ctx, tx, revision)
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

		var tuples []*core.RelationTuple
		for rows.Next() {
			nextTuple := &core.RelationTuple{
				ObjectAndRelation: &core.ObjectAndRelation{},
				User: &core.User{
					UserOneof: &core.User_Userset{
						Userset: &core.ObjectAndRelation{},
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
}

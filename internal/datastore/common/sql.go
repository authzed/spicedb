package common

import (
	"context"
	"fmt"
	"os"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	"github.com/alecthomas/units"
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	TableNamespaceDefault   = "namespace_config"
	TableTransactionDefault = "relation_tuple_transaction"
	TableTupleDefault       = "relation_tuple"

	ColID               = "id"
	ColTimestamp        = "timestamp"
	ColNamespace        = "namespace"
	ColConfig           = "serialized_config"
	ColCreatedTxn       = "created_transaction"
	ColDeletedTxn       = "deleted_transaction"
	ColObjectID         = "object_id"
	ColRelation         = "relation"
	ColUsersetNamespace = "userset_namespace"
	ColUsersetObjectID  = "userset_object_id"
	ColUsersetRelation  = "userset_relation"
)

var (
	TableNamespace   = tableNameWithPrefix(TableNamespaceDefault)
	TableTransaction = tableNameWithPrefix(TableTransactionDefault)
	TableTuple       = tableNameWithPrefix(TableTupleDefault)

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
)

// DefaultSplitAtEstimatedQuerySize is the default allowed estimated query size before the
// TupleQuerySplitter will split the query into multiple calls.
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

// SchemaQueryFilterer wraps a SchemaInformation and SelectBuilder to give an opinionated
// way to build query objects.
type SchemaQueryFilterer struct {
	schema               SchemaInformation
	queryBuilder         sq.SelectBuilder
	currentEstimatedSize int
	tracerAttributes     []attribute.KeyValue
}

// Check if there's an enironment variable set with a prefix for db table names. If there is,
// prepend it to the given tableName, otherwise just return the tableName.
func tableNameWithPrefix(tableName string) string {
	tablePrefix, ok := os.LookupEnv("table_prefix")
	// os.LookupEnv will return an error if the environment variable isn't set
	if !ok {
		return tableName
	}

	return fmt.Sprintf("%s%s", tablePrefix, tableName)
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
	sqf.currentEstimatedSize += len(resourceType)
	return sqf
}

// FilterToResourceID returns a new SchemaQueryFilterer that is limited to resources with the
// specified ID.
func (sqf SchemaQueryFilterer) FilterToResourceID(objectID string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColObjectID: objectID})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjIDKey.String(objectID))
	sqf.currentEstimatedSize += len(objectID)
	return sqf
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColRelation: relation})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjRelationNameKey.String(relation))
	sqf.currentEstimatedSize += len(relation)
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

	sqf.currentEstimatedSize += len(filter.SubjectType) + len(filter.OptionalSubjectId)

	if filter.OptionalRelation != nil {
		dsRelationName := stringz.DefaultEmpty(filter.OptionalRelation.Relation, datastore.Ellipsis)

		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
		sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
		sqf.currentEstimatedSize += len(dsRelationName)
	}

	return sqf
}

// FilterToUsersets returns a new SchemaQueryFilterer that is limited to resources with subjects
// in the specified list of usersets.
func (sqf SchemaQueryFilterer) FilterToUsersets(usersets []*v0.ObjectAndRelation) SchemaQueryFilterer {
	if len(usersets) == 0 {
		panic("Got empty usersets filter")
	}

	orClause := sq.Or{}
	for _, userset := range usersets {
		orClause = append(orClause, sq.Eq{
			sqf.schema.ColUsersetNamespace: userset.Namespace,
			sqf.schema.ColUsersetObjectID:  userset.ObjectId,
			sqf.schema.ColUsersetRelation:  userset.Relation,
		})
		sqf.currentEstimatedSize += len(userset.Namespace) + len(userset.ObjectId) + len(userset.Relation)
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(orClause)

	return sqf
}

// Limit returns a new SchemaQueryFilterer which is limited to the specified number of results.
func (sqf SchemaQueryFilterer) Limit(limit uint64) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Limit(limit)
	sqf.tracerAttributes = append(sqf.tracerAttributes, limitKey.Int64(int64(limit)))
	return sqf
}

// TransactionPreparer is a function provided by the datastore to prepare the transaction before
// the tuple query is run.
type TransactionPreparer func(ctx context.Context, tx Transaction, revision datastore.Revision) error

// TupleQuerySplitter is a tuple query runner shared by SQL implementations of the datastore.
type TupleQuerySplitter struct {
	TransactionBeginner
	PrepareTransaction        TransactionPreparer
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
func (ctq TupleQuerySplitter) SplitAndExecute(ctx context.Context) (datastore.TupleIterator, error) {
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

func (ctq TupleQuerySplitter) executeSingleQuery(ctx context.Context, query SchemaQueryFilterer, index int, limit uint64) ([]*v0.RelationTuple, error) {
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

	tx, err := ctq.BeginTransaction(ctx, true)
	if err != nil {
		return nil, fmt.Errorf(ErrUnableToQueryTuples, err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("error rolling back transaction")
		}
	}()

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

func RevisionFromTransaction(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}

func TransactionFromRevision(revision datastore.Revision) uint64 {
	return uint64(revision.IntPart())
}

func ExactRelationshipClause(r *v1.Relationship) sq.Eq {
	return sq.Eq{
		ColNamespace:        r.Resource.ObjectType,
		ColObjectID:         r.Resource.ObjectId,
		ColRelation:         r.Relation,
		ColUsersetNamespace: r.Subject.Object.ObjectType,
		ColUsersetObjectID:  r.Subject.Object.ObjectId,
		ColUsersetRelation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
	}
}

func FilterToLivingObjects(original sq.SelectBuilder, revision datastore.Revision, maxTransactionID uint64) sq.SelectBuilder {
	return original.Where(sq.LtOrEq{ColCreatedTxn: TransactionFromRevision(revision)}).
		Where(sq.Or{
			sq.Eq{ColDeletedTxn: maxTransactionID},
			sq.Gt{ColDeletedTxn: revision},
		})
}

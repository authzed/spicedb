package common

import (
	"context"
	"fmt"
	"math"
	"runtime"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

// FilterToResourceIDs returns a new SchemaQueryFilterer that is limited to resources with any of the
// specified IDs.
func (sqf SchemaQueryFilterer) FilterToResourceIDs(resourceIds []string) SchemaQueryFilterer {
	// TODO(jschorr): Change this panic into an automatic query split, if we find it necessary.
	if len(resourceIds) > datastore.FilterMaximumIDCount {
		panic(fmt.Sprintf("Cannot have more than %d resources IDs in a single filter", datastore.FilterMaximumIDCount))
	}

	inClause := fmt.Sprintf("%s IN (", sqf.schema.ColObjectID)
	args := make([]any, 0, len(resourceIds))

	for index, resourceID := range resourceIds {
		if len(resourceID) == 0 {
			panic("got empty resource id")
		}

		if index > 0 {
			inClause += ", "
		}

		inClause += "?"

		args = append(args, resourceID)
		sqf.tracerAttributes = append(sqf.tracerAttributes, ObjIDKey.String(resourceID))
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(inClause+")", args...)
	return sqf
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColRelation: relation})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjRelationNameKey.String(relation))
	return sqf
}

// FilterWithRelationshipsFilter returns a new SchemaQueryFilterer that is limited to resources with
// resources that match the specified filter.
func (sqf SchemaQueryFilterer) FilterWithRelationshipsFilter(filter datastore.RelationshipsFilter) SchemaQueryFilterer {
	sqf = sqf.FilterToResourceType(filter.ResourceType)

	if filter.OptionalResourceRelation != "" {
		sqf = sqf.FilterToRelation(filter.OptionalResourceRelation)
	}

	if len(filter.OptionalResourceIds) > 0 {
		sqf = sqf.FilterToResourceIDs(filter.OptionalResourceIds)
	}

	if filter.OptionalSubjectsFilter != nil {
		sqf = sqf.FilterWithSubjectsFilter(*filter.OptionalSubjectsFilter)
	}

	return sqf
}

// FilterWithSubjectsFilter returns a new SchemaQueryFilterer that is limited to resources with
// subjects that match the specified filter.
func (sqf SchemaQueryFilterer) FilterWithSubjectsFilter(filter datastore.SubjectsFilter) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetNamespace: filter.SubjectType})
	sqf.tracerAttributes = append(sqf.tracerAttributes, SubNamespaceNameKey.String(filter.SubjectType))

	if len(filter.OptionalSubjectIds) > 0 {
		// TODO(jschorr): Change this panic into an automatic query split, if we find it necessary.
		if len(filter.OptionalSubjectIds) > datastore.FilterMaximumIDCount {
			panic(fmt.Sprintf("Cannot have more than %d subject IDs in a single filter", datastore.FilterMaximumIDCount))
		}

		inClause := fmt.Sprintf("%s IN (", sqf.schema.ColUsersetObjectID)
		args := make([]any, 0, len(filter.OptionalSubjectIds))

		for index, subjectID := range filter.OptionalSubjectIds {
			if len(subjectID) == 0 {
				panic("got empty subject id")
			}

			if index > 0 {
				inClause += ", "
			}

			inClause += "?"

			args = append(args, subjectID)
			sqf.tracerAttributes = append(sqf.tracerAttributes, SubObjectIDKey.String(subjectID))
		}

		sqf.queryBuilder = sqf.queryBuilder.Where(inClause+")", args...)
	}

	if !filter.RelationFilter.IsEmpty() {
		relations := make([]string, 0, 2)
		if filter.RelationFilter.IncludeEllipsisRelation {
			relations = append(relations, datastore.Ellipsis)
		}

		if filter.RelationFilter.NonEllipsisRelation != "" {
			relations = append(relations, filter.RelationFilter.NonEllipsisRelation)
		}

		if len(relations) == 1 {
			relName := relations[0]
			sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(relName))
			sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetRelation: relName})
		} else {
			orClause := sq.Or{}
			for _, relationName := range relations {
				dsRelationName := stringz.DefaultEmpty(relationName, datastore.Ellipsis)
				orClause = append(orClause, sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
				sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
			}

			sqf.queryBuilder = sqf.queryBuilder.Where(orClause)
		}
	}

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
	UsersetBatchSize uint16
}

// SplitAndExecuteQuery is used to split up the usersets in a very large query and execute
// them as separate queries.
func (tqs TupleQuerySplitter) SplitAndExecuteQuery(
	ctx context.Context,
	query SchemaQueryFilterer,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
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
		upperBound := uint16(len(remainingUsersets))
		if upperBound > tqs.UsersetBatchSize {
			upperBound = tqs.UsersetBatchSize
		}

		batch := remainingUsersets[:upperBound]
		toExecute := query.limit(uint64(remainingLimit)).filterToUsersets(batch)

		sql, args, err := toExecute.queryBuilder.ToSql()
		if err != nil {
			return nil, err
		}

		queryTuples, err := tqs.Executor(ctx, sql, args)
		if err != nil {
			return nil, err
		}

		if len(queryTuples) > remainingLimit {
			queryTuples = queryTuples[:remainingLimit]
		}

		tuples = append(tuples, queryTuples...)
		remainingUsersets = remainingUsersets[upperBound:]
	}

	iter := datastore.NewSliceRelationshipIterator(tuples)
	runtime.SetFinalizer(iter, datastore.BuildFinalizerFunction())
	return iter, nil
}

// ExecuteQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteQueryFunc func(ctx context.Context, sql string, args []any) ([]*core.RelationTuple, error)

// TxCleanupFunc is a function that should be executed when the caller of
// TransactionFactory is done with the transaction.
type TxCleanupFunc func(context.Context)

package common

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var (
	// CaveatNameKey is a tracing attribute representing a caveat name
	CaveatNameKey = attribute.Key("authzed.com/spicedb/sql/caveatName")

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
	ColCaveatName       string
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

func (sqf SchemaQueryFilterer) MustFilterToResourceIDs(resourceIds []string) SchemaQueryFilterer {
	updated, err := sqf.FilterToResourceIDs(resourceIds)
	if err != nil {
		panic(err)
	}
	return updated
}

// FilterToResourceIDs returns a new SchemaQueryFilterer that is limited to resources with any of the
// specified IDs.
func (sqf SchemaQueryFilterer) FilterToResourceIDs(resourceIds []string) (SchemaQueryFilterer, error) {
	// TODO(jschorr): Change this panic into an automatic query split, if we find it necessary.
	if len(resourceIds) > int(datastore.FilterMaximumIDCount) {
		return sqf, spiceerrors.MustBugf("cannot have more than %d resources IDs in a single filter", datastore.FilterMaximumIDCount)
	}

	inClause := sqf.schema.ColObjectID + " IN ("
	args := make([]any, 0, len(resourceIds))

	for index, resourceID := range resourceIds {
		if len(resourceID) == 0 {
			return sqf, spiceerrors.MustBugf("got empty resource ID")
		}

		if index > 0 {
			inClause += ", "
		}

		inClause += "?"

		args = append(args, resourceID)
		sqf.tracerAttributes = append(sqf.tracerAttributes, ObjIDKey.String(resourceID))
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(inClause+")", args...)
	return sqf, nil
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColRelation: relation})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjRelationNameKey.String(relation))
	return sqf
}

// MustFilterWithRelationshipsFilter returns a new SchemaQueryFilterer that is limited to resources with
// resources that match the specified filter.
func (sqf SchemaQueryFilterer) MustFilterWithRelationshipsFilter(filter datastore.RelationshipsFilter) SchemaQueryFilterer {
	updated, err := sqf.FilterWithRelationshipsFilter(filter)
	if err != nil {
		panic(err)
	}
	return updated
}

func (sqf SchemaQueryFilterer) FilterWithRelationshipsFilter(filter datastore.RelationshipsFilter) (SchemaQueryFilterer, error) {
	sqf = sqf.FilterToResourceType(filter.ResourceType)

	if filter.OptionalResourceRelation != "" {
		sqf = sqf.FilterToRelation(filter.OptionalResourceRelation)
	}

	if len(filter.OptionalResourceIds) > 0 {
		usqf, err := sqf.FilterToResourceIDs(filter.OptionalResourceIds)
		if err != nil {
			return sqf, err
		}
		sqf = usqf
	}

	if len(filter.OptionalSubjectsSelectors) > 0 {
		usqf, err := sqf.FilterWithSubjectsSelectors(filter.OptionalSubjectsSelectors...)
		if err != nil {
			return sqf, err
		}
		sqf = usqf
	}

	if filter.OptionalCaveatName != "" {
		sqf = sqf.FilterWithCaveatName(filter.OptionalCaveatName)
	}

	return sqf, nil
}

// MustFilterWithSubjectsSelectors returns a new SchemaQueryFilterer that is limited to resources with
// subjects that match the specified selector(s).
func (sqf SchemaQueryFilterer) MustFilterWithSubjectsSelectors(selectors ...datastore.SubjectsSelector) SchemaQueryFilterer {
	usqf, err := sqf.FilterWithSubjectsSelectors(selectors...)
	if err != nil {
		panic(err)
	}
	return usqf
}

// FilterWithSubjectsSelectors returns a new SchemaQueryFilterer that is limited to resources with
// subjects that match the specified selector(s).
func (sqf SchemaQueryFilterer) FilterWithSubjectsSelectors(selectors ...datastore.SubjectsSelector) (SchemaQueryFilterer, error) {
	selectorsOrClause := sq.Or{}

	for _, selector := range selectors {
		selectorClause := sq.And{}

		if len(selector.OptionalSubjectType) > 0 {
			selectorClause = append(selectorClause, sq.Eq{sqf.schema.ColUsersetNamespace: selector.OptionalSubjectType})
			sqf.tracerAttributes = append(sqf.tracerAttributes, SubNamespaceNameKey.String(selector.OptionalSubjectType))
		}

		if len(selector.OptionalSubjectIds) > 0 {
			// TODO(jschorr): Change this panic into an automatic query split, if we find it necessary.
			if len(selector.OptionalSubjectIds) > int(datastore.FilterMaximumIDCount) {
				return sqf, spiceerrors.MustBugf("cannot have more than %d subject IDs in a single filter", datastore.FilterMaximumIDCount)
			}

			inClause := sqf.schema.ColUsersetObjectID + " IN ("
			args := make([]any, 0, len(selector.OptionalSubjectIds))

			for index, subjectID := range selector.OptionalSubjectIds {
				if len(subjectID) == 0 {
					return sqf, spiceerrors.MustBugf("got empty subject ID")
				}

				if index > 0 {
					inClause += ", "
				}

				inClause += "?"

				args = append(args, subjectID)
				sqf.tracerAttributes = append(sqf.tracerAttributes, SubObjectIDKey.String(subjectID))
			}

			selectorClause = append(selectorClause, sq.Expr(inClause+")", args...))
		}

		if !selector.RelationFilter.IsEmpty() {
			if selector.RelationFilter.OnlyNonEllipsisRelations {
				selectorClause = append(selectorClause, sq.NotEq{sqf.schema.ColUsersetRelation: datastore.Ellipsis})
			} else {
				relations := make([]string, 0, 2)
				if selector.RelationFilter.IncludeEllipsisRelation {
					relations = append(relations, datastore.Ellipsis)
				}

				if selector.RelationFilter.NonEllipsisRelation != "" {
					relations = append(relations, selector.RelationFilter.NonEllipsisRelation)
				}

				if len(relations) == 1 {
					relName := relations[0]
					sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(relName))
					selectorClause = append(selectorClause, sq.Eq{sqf.schema.ColUsersetRelation: relName})
				} else {
					orClause := sq.Or{}
					for _, relationName := range relations {
						dsRelationName := stringz.DefaultEmpty(relationName, datastore.Ellipsis)
						orClause = append(orClause, sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
						sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
					}

					selectorClause = append(selectorClause, orClause)
				}
			}
		}

		selectorsOrClause = append(selectorsOrClause, selectorClause)
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(selectorsOrClause)
	return sqf, nil
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

func (sqf SchemaQueryFilterer) FilterWithCaveatName(caveatName string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColCaveatName: caveatName})
	sqf.tracerAttributes = append(sqf.tracerAttributes, CaveatNameKey.String(caveatName))
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
	runtime.SetFinalizer(iter, datastore.MustIteratorBeClosed)
	return iter, nil
}

func InlineSqlArgs(sqlQuery string, args []interface{}) string {
	for _, arg := range args {
		var formattedArg string
		switch arg.(type) {
		case string:
			formattedArg = fmt.Sprintf("'%v'", arg)
		default:
			formattedArg = fmt.Sprint(arg)
		}
		sqlQuery = strings.Replace(sqlQuery, "?", formattedArg, 1)
	}
	return sqlQuery
}

// ExecuteQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteQueryFunc func(ctx context.Context, sql string, args []any) ([]*core.RelationTuple, error)

// TxCleanupFunc is a function that should be executed when the caller of
// TransactionFactory is done with the transaction.
type TxCleanupFunc func(context.Context)

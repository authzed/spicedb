package common

import (
	"context"
	"math"
	"strings"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
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

// PaginationFilterType is an enumerator
type PaginationFilterType uint8

const (
	// TupleComparison uses a comparison with a compound key,
	// e.g. (namespace, object_id, relation) > ('ns', '123', 'viewer')
	// which is not compatible with all datastores.
	TupleComparison PaginationFilterType = iota

	// ExpandedLogicComparison comparison uses a nested tree of ANDs and ORs to properly
	// filter out already received relationships. Useful for databases that do not support
	// tuple comparison, or do not execute it efficiently
	ExpandedLogicComparison
)

// SchemaInformation holds the schema information from the SQL datastore implementation.
type SchemaInformation struct {
	colNamespace         string
	colObjectID          string
	colRelation          string
	colUsersetNamespace  string
	colUsersetObjectID   string
	colUsersetRelation   string
	colCaveatName        string
	paginationFilterType PaginationFilterType
}

func NewSchemaInformation(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatName string,
	paginationFilterType PaginationFilterType,
) SchemaInformation {
	return SchemaInformation{
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatName,
		paginationFilterType,
	}
}

// SchemaQueryFilterer wraps a SchemaInformation and SelectBuilder to give an opinionated
// way to build query objects.
type SchemaQueryFilterer struct {
	schema                SchemaInformation
	queryBuilder          sq.SelectBuilder
	filteringColumnCounts map[string]int
	tracerAttributes      []attribute.KeyValue
}

// NewSchemaQueryFilterer creates a new SchemaQueryFilterer object.
func NewSchemaQueryFilterer(schema SchemaInformation, initialQuery sq.SelectBuilder) SchemaQueryFilterer {
	return SchemaQueryFilterer{
		schema:                schema,
		queryBuilder:          initialQuery,
		filteringColumnCounts: map[string]int{},
	}
}

func (sqf SchemaQueryFilterer) TupleOrder(order options.SortOrder) SchemaQueryFilterer {
	switch order {
	case options.ByResource:
		sqf.queryBuilder = sqf.queryBuilder.OrderBy(
			sqf.schema.colNamespace,
			sqf.schema.colObjectID,
			sqf.schema.colRelation,
			sqf.schema.colUsersetNamespace,
			sqf.schema.colUsersetObjectID,
			sqf.schema.colUsersetRelation,
		)

	case options.BySubject:
		sqf.queryBuilder = sqf.queryBuilder.OrderBy(
			sqf.schema.colUsersetNamespace,
			sqf.schema.colUsersetObjectID,
			sqf.schema.colUsersetRelation,
			sqf.schema.colNamespace,
			sqf.schema.colObjectID,
			sqf.schema.colRelation,
		)
	}

	return sqf
}

type nameAndValue struct {
	name  string
	value string
}

func (sqf SchemaQueryFilterer) After(cursor *core.RelationTuple, order options.SortOrder) SchemaQueryFilterer {
	columnsAndValues := map[options.SortOrder][]nameAndValue{
		options.ByResource: {
			{
				sqf.schema.colNamespace, cursor.ResourceAndRelation.Namespace,
			},
			{
				sqf.schema.colObjectID, cursor.ResourceAndRelation.ObjectId,
			},
			{
				sqf.schema.colRelation, cursor.ResourceAndRelation.Relation,
			},
			{
				sqf.schema.colUsersetNamespace, cursor.Subject.Namespace,
			},
			{
				sqf.schema.colUsersetObjectID, cursor.Subject.ObjectId,
			},
			{
				sqf.schema.colUsersetRelation, cursor.Subject.Relation,
			},
		},
		options.BySubject: {
			{
				sqf.schema.colUsersetNamespace, cursor.Subject.Namespace,
			},
			{
				sqf.schema.colUsersetObjectID, cursor.Subject.ObjectId,
			},
			{
				sqf.schema.colUsersetRelation, cursor.Subject.Relation,
			},
			{
				sqf.schema.colNamespace, cursor.ResourceAndRelation.Namespace,
			},
			{
				sqf.schema.colObjectID, cursor.ResourceAndRelation.ObjectId,
			},
			{
				sqf.schema.colRelation, cursor.ResourceAndRelation.Relation,
			},
		},
	}[order]

	switch sqf.schema.paginationFilterType {
	case TupleComparison:
		// For performance reasons, remove any column names that have static values in the query.
		columnNames := make([]string, 0, len(columnsAndValues))
		valueSlots := make([]any, 0, len(columnsAndValues))
		comparisonSlotCount := 0

		for _, cav := range columnsAndValues {
			if sqf.filteringColumnCounts[cav.name] != 1 {
				columnNames = append(columnNames, cav.name)
				valueSlots = append(valueSlots, cav.value)
				comparisonSlotCount++
			}
		}

		if comparisonSlotCount > 0 {
			comparisonTuple := "(" + strings.Join(columnNames, ",") + ") > (" + strings.Repeat(",?", comparisonSlotCount)[1:] + ")"
			sqf.queryBuilder = sqf.queryBuilder.Where(
				comparisonTuple,
				valueSlots...,
			)
		}

	case ExpandedLogicComparison:
		// For performance reasons, remove any column names that have static values in the query.
		orClause := sq.Or{}

		for index, cav := range columnsAndValues {
			if sqf.filteringColumnCounts[cav.name] != 1 {
				andClause := sq.And{}
				for _, previous := range columnsAndValues[0:index] {
					if sqf.filteringColumnCounts[previous.name] != 1 {
						andClause = append(andClause, sq.Eq{previous.name: previous.value})
					}
				}

				andClause = append(andClause, sq.Gt{cav.name: cav.value})
				orClause = append(orClause, andClause)
			}
		}

		if len(orClause) > 0 {
			sqf.queryBuilder = sqf.queryBuilder.Where(orClause)
		}
	}

	return sqf
}

// FilterToResourceType returns a new SchemaQueryFilterer that is limited to resources of the
// specified type.
func (sqf SchemaQueryFilterer) FilterToResourceType(resourceType string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colNamespace: resourceType})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjNamespaceNameKey.String(resourceType))
	sqf.recordColumnValue(sqf.schema.colNamespace)
	return sqf
}

func (sqf SchemaQueryFilterer) recordColumnValue(colName string) {
	if value, ok := sqf.filteringColumnCounts[colName]; ok {
		sqf.filteringColumnCounts[colName] = value + 1
		return
	}

	sqf.filteringColumnCounts[colName] = 1
}

// FilterToResourceID returns a new SchemaQueryFilterer that is limited to resources with the
// specified ID.
func (sqf SchemaQueryFilterer) FilterToResourceID(objectID string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colObjectID: objectID})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjIDKey.String(objectID))
	sqf.recordColumnValue(sqf.schema.colObjectID)
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

	inClause := sqf.schema.colObjectID + " IN ("
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
		sqf.recordColumnValue(sqf.schema.colObjectID)
	}

	sqf.queryBuilder = sqf.queryBuilder.Where(inClause+")", args...)
	return sqf, nil
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colRelation: relation})
	sqf.tracerAttributes = append(sqf.tracerAttributes, ObjRelationNameKey.String(relation))
	sqf.recordColumnValue(sqf.schema.colRelation)
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
			selectorClause = append(selectorClause, sq.Eq{sqf.schema.colUsersetNamespace: selector.OptionalSubjectType})
			sqf.tracerAttributes = append(sqf.tracerAttributes, SubNamespaceNameKey.String(selector.OptionalSubjectType))
			sqf.recordColumnValue(sqf.schema.colUsersetNamespace)
		}

		if len(selector.OptionalSubjectIds) > 0 {
			// TODO(jschorr): Change this panic into an automatic query split, if we find it necessary.
			if len(selector.OptionalSubjectIds) > int(datastore.FilterMaximumIDCount) {
				return sqf, spiceerrors.MustBugf("cannot have more than %d subject IDs in a single filter", datastore.FilterMaximumIDCount)
			}

			inClause := sqf.schema.colUsersetObjectID + " IN ("
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
				sqf.recordColumnValue(sqf.schema.colUsersetObjectID)
			}

			selectorClause = append(selectorClause, sq.Expr(inClause+")", args...))
		}

		if !selector.RelationFilter.IsEmpty() {
			if selector.RelationFilter.OnlyNonEllipsisRelations {
				selectorClause = append(selectorClause, sq.NotEq{sqf.schema.colUsersetRelation: datastore.Ellipsis})
				sqf.recordColumnValue(sqf.schema.colUsersetRelation)
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
					selectorClause = append(selectorClause, sq.Eq{sqf.schema.colUsersetRelation: relName})
					sqf.recordColumnValue(sqf.schema.colUsersetRelation)
				} else {
					orClause := sq.Or{}
					for _, relationName := range relations {
						dsRelationName := stringz.DefaultEmpty(relationName, datastore.Ellipsis)
						orClause = append(orClause, sq.Eq{sqf.schema.colUsersetRelation: dsRelationName})
						sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
						sqf.recordColumnValue(sqf.schema.colUsersetRelation)
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
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colUsersetNamespace: filter.SubjectType})
	sqf.tracerAttributes = append(sqf.tracerAttributes, SubNamespaceNameKey.String(filter.SubjectType))
	sqf.recordColumnValue(sqf.schema.colUsersetNamespace)

	if filter.OptionalSubjectId != "" {
		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colUsersetObjectID: filter.OptionalSubjectId})
		sqf.tracerAttributes = append(sqf.tracerAttributes, SubObjectIDKey.String(filter.OptionalSubjectId))
		sqf.recordColumnValue(sqf.schema.colUsersetObjectID)
	}

	if filter.OptionalRelation != nil {
		dsRelationName := stringz.DefaultEmpty(filter.OptionalRelation.Relation, datastore.Ellipsis)

		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colUsersetRelation: dsRelationName})
		sqf.tracerAttributes = append(sqf.tracerAttributes, SubRelationNameKey.String(dsRelationName))
		sqf.recordColumnValue(sqf.schema.colUsersetRelation)
	}

	return sqf
}

func (sqf SchemaQueryFilterer) FilterWithCaveatName(caveatName string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colCaveatName: caveatName})
	sqf.tracerAttributes = append(sqf.tracerAttributes, CaveatNameKey.String(caveatName))
	sqf.recordColumnValue(sqf.schema.colCaveatName)
	return sqf
}

// Limit returns a new SchemaQueryFilterer which is limited to the specified number of results.
func (sqf SchemaQueryFilterer) limit(limit uint64) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Limit(limit)
	sqf.tracerAttributes = append(sqf.tracerAttributes, limitKey.Int64(int64(limit)))
	return sqf
}

// QueryExecutor is a tuple query runner shared by SQL implementations of the datastore.
type QueryExecutor struct {
	Executor ExecuteQueryFunc
}

// ExecuteQuery executes the query.
func (tqs QueryExecutor) ExecuteQuery(
	ctx context.Context,
	query SchemaQueryFilterer,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	ctx, span := tracer.Start(ctx, "ExecuteQuery")
	defer span.End()
	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	query = query.TupleOrder(queryOpts.Sort)

	if queryOpts.After != nil {
		if queryOpts.Sort == options.Unsorted {
			return nil, datastore.ErrCursorsWithoutSorting
		}

		query = query.After(queryOpts.After, queryOpts.Sort)
	}

	limit := math.MaxInt
	if queryOpts.Limit != nil {
		limit = int(*queryOpts.Limit)
	}

	toExecute := query.limit(uint64(limit))
	sql, args, err := toExecute.queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	queryTuples, err := tqs.Executor(ctx, sql, args)
	if err != nil {
		return nil, err
	}

	if len(queryTuples) > limit {
		queryTuples = queryTuples[:limit]
	}

	return NewSliceRelationshipIterator(queryTuples, queryOpts.Sort), nil
}

// ExecuteQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteQueryFunc func(ctx context.Context, sql string, args []any) ([]*core.RelationTuple, error)

// TxCleanupFunc is a function that should be executed when the caller of
// TransactionFactory is done with the transaction.
type TxCleanupFunc func(context.Context)

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

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
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
	filterMaximumIDCount  uint16
}

// NewSchemaQueryFilterer creates a new SchemaQueryFilterer object.
func NewSchemaQueryFilterer(schema SchemaInformation, initialQuery sq.SelectBuilder, filterMaximumIDCount uint16) SchemaQueryFilterer {
	if filterMaximumIDCount == 0 {
		filterMaximumIDCount = 100
		log.Warn().Msg("SchemaQueryFilterer: filterMaximumIDCount not set, defaulting to 100")
	}

	return SchemaQueryFilterer{
		schema:                schema,
		queryBuilder:          initialQuery,
		filteringColumnCounts: map[string]int{},
		filterMaximumIDCount:  filterMaximumIDCount,
	}
}

func (sqf SchemaQueryFilterer) UnderlyingQueryBuilder() sq.SelectBuilder {
	return sqf.queryBuilder
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

func (sqf SchemaQueryFilterer) After(cursor options.Cursor, order options.SortOrder) SchemaQueryFilterer {
	spiceerrors.DebugAssert(func() bool {
		return cursor != nil
	}, "cursor cannot be nil")

	// NOTE: The ordering of these columns can affect query performance, be aware when changing.
	columnsAndValues := map[options.SortOrder][]nameAndValue{
		options.ByResource: {
			{
				sqf.schema.colNamespace, cursor.Resource.ObjectType,
			},
			{
				sqf.schema.colObjectID, cursor.Resource.ObjectID,
			},
			{
				sqf.schema.colRelation, cursor.Resource.Relation,
			},
			{
				sqf.schema.colUsersetNamespace, cursor.Subject.ObjectType,
			},
			{
				sqf.schema.colUsersetObjectID, cursor.Subject.ObjectID,
			},
			{
				sqf.schema.colUsersetRelation, cursor.Subject.Relation,
			},
		},
		options.BySubject: {
			{
				sqf.schema.colUsersetNamespace, cursor.Subject.ObjectType,
			},
			{
				sqf.schema.colUsersetObjectID, cursor.Subject.ObjectID,
			},
			{
				sqf.schema.colNamespace, cursor.Resource.ObjectType,
			},
			{
				sqf.schema.colObjectID, cursor.Resource.ObjectID,
			},
			{
				sqf.schema.colRelation, cursor.Resource.Relation,
			},
			{
				sqf.schema.colUsersetRelation, cursor.Subject.Relation,
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

// FilterWithResourceIDPrefix returns new SchemaQueryFilterer that is limited to resources whose ID
// starts with the specified prefix.
func (sqf SchemaQueryFilterer) FilterWithResourceIDPrefix(prefix string) (SchemaQueryFilterer, error) {
	if strings.Contains(prefix, "%") {
		return sqf, spiceerrors.MustBugf("prefix cannot contain the percent sign")
	}
	if prefix == "" {
		return sqf, spiceerrors.MustBugf("prefix cannot be empty")
	}

	prefix = strings.ReplaceAll(prefix, `\`, `\\`)
	prefix = strings.ReplaceAll(prefix, "_", `\_`)

	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Like{sqf.schema.colObjectID: prefix + "%"})

	// NOTE: we do *not* record the use of the resource ID column here, because it is not used
	// statically and thus is necessary for sorting operations.
	return sqf, nil
}

func (sqf SchemaQueryFilterer) MustFilterWithResourceIDPrefix(prefix string) SchemaQueryFilterer {
	updated, err := sqf.FilterWithResourceIDPrefix(prefix)
	if err != nil {
		panic(err)
	}
	return updated
}

// FilterToResourceIDs returns a new SchemaQueryFilterer that is limited to resources with any of the
// specified IDs.
func (sqf SchemaQueryFilterer) FilterToResourceIDs(resourceIds []string) (SchemaQueryFilterer, error) {
	spiceerrors.DebugAssert(func() bool {
		return len(resourceIds) <= int(sqf.filterMaximumIDCount)
	}, "cannot have more than %d resource IDs in a single filter", sqf.filterMaximumIDCount)

	var builder strings.Builder
	builder.WriteString(sqf.schema.colObjectID)
	builder.WriteString(" IN (")
	args := make([]any, 0, len(resourceIds))

	for _, resourceID := range resourceIds {
		if len(resourceID) == 0 {
			return sqf, spiceerrors.MustBugf("got empty resource ID")
		}

		args = append(args, resourceID)
		sqf.recordColumnValue(sqf.schema.colObjectID)
	}

	builder.WriteString("?")
	if len(resourceIds) > 1 {
		builder.WriteString(strings.Repeat(",?", len(resourceIds)-1))
	}
	builder.WriteString(")")

	sqf.queryBuilder = sqf.queryBuilder.Where(builder.String(), args...)
	return sqf, nil
}

// FilterToRelation returns a new SchemaQueryFilterer that is limited to resources with the
// specified relation.
func (sqf SchemaQueryFilterer) FilterToRelation(relation string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colRelation: relation})
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
	csqf := sqf

	if filter.OptionalResourceType != "" {
		csqf = csqf.FilterToResourceType(filter.OptionalResourceType)
	}

	if filter.OptionalResourceRelation != "" {
		csqf = csqf.FilterToRelation(filter.OptionalResourceRelation)
	}

	if len(filter.OptionalResourceIds) > 0 && filter.OptionalResourceIDPrefix != "" {
		return csqf, spiceerrors.MustBugf("cannot filter by both resource IDs and ID prefix")
	}

	if len(filter.OptionalResourceIds) > 0 {
		usqf, err := csqf.FilterToResourceIDs(filter.OptionalResourceIds)
		if err != nil {
			return csqf, err
		}
		csqf = usqf
	}

	if len(filter.OptionalResourceIDPrefix) > 0 {
		usqf, err := csqf.FilterWithResourceIDPrefix(filter.OptionalResourceIDPrefix)
		if err != nil {
			return csqf, err
		}
		csqf = usqf
	}

	if len(filter.OptionalSubjectsSelectors) > 0 {
		usqf, err := csqf.FilterWithSubjectsSelectors(filter.OptionalSubjectsSelectors...)
		if err != nil {
			return csqf, err
		}
		csqf = usqf
	}

	if filter.OptionalCaveatName != "" {
		csqf = csqf.FilterWithCaveatName(filter.OptionalCaveatName)
	}

	return csqf, nil
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
			sqf.recordColumnValue(sqf.schema.colUsersetNamespace)
		}

		if len(selector.OptionalSubjectIds) > 0 {
			spiceerrors.DebugAssert(func() bool {
				return len(selector.OptionalSubjectIds) <= int(sqf.filterMaximumIDCount)
			}, "cannot have more than %d subject IDs in a single filter", sqf.filterMaximumIDCount)

			var builder strings.Builder
			builder.WriteString(sqf.schema.colUsersetObjectID)
			builder.WriteString(" IN (")
			args := make([]any, 0, len(selector.OptionalSubjectIds))

			for _, subjectID := range selector.OptionalSubjectIds {
				if len(subjectID) == 0 {
					return sqf, spiceerrors.MustBugf("got empty subject ID")
				}

				args = append(args, subjectID)
				sqf.recordColumnValue(sqf.schema.colUsersetObjectID)
			}

			builder.WriteString("?")
			if len(selector.OptionalSubjectIds) > 1 {
				builder.WriteString(strings.Repeat(",?", len(selector.OptionalSubjectIds)-1))
			}

			builder.WriteString(")")
			selectorClause = append(selectorClause, sq.Expr(builder.String(), args...))
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
					selectorClause = append(selectorClause, sq.Eq{sqf.schema.colUsersetRelation: relName})
					sqf.recordColumnValue(sqf.schema.colUsersetRelation)
				} else {
					orClause := sq.Or{}
					for _, relationName := range relations {
						dsRelationName := stringz.DefaultEmpty(relationName, datastore.Ellipsis)
						orClause = append(orClause, sq.Eq{sqf.schema.colUsersetRelation: dsRelationName})
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
	sqf.recordColumnValue(sqf.schema.colUsersetNamespace)

	if filter.OptionalSubjectId != "" {
		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colUsersetObjectID: filter.OptionalSubjectId})
		sqf.recordColumnValue(sqf.schema.colUsersetObjectID)
	}

	if filter.OptionalRelation != nil {
		dsRelationName := stringz.DefaultEmpty(filter.OptionalRelation.Relation, datastore.Ellipsis)

		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colUsersetRelation: dsRelationName})
		sqf.recordColumnValue(sqf.schema.colUsersetRelation)
	}

	return sqf
}

func (sqf SchemaQueryFilterer) FilterWithCaveatName(caveatName string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.colCaveatName: caveatName})
	sqf.recordColumnValue(sqf.schema.colCaveatName)
	return sqf
}

// Limit returns a new SchemaQueryFilterer which is limited to the specified number of results.
func (sqf SchemaQueryFilterer) limit(limit uint64) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Limit(limit)
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
	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	query = query.TupleOrder(queryOpts.Sort)

	if queryOpts.After != nil {
		if queryOpts.Sort == options.Unsorted {
			return nil, datastore.ErrCursorsWithoutSorting
		}

		query = query.After(queryOpts.After, queryOpts.Sort)
	}

	var limit uint64
	// NOTE: we use a uint here because it lines up with the
	// assignments in this function, but we set it to MaxInt64
	// because that's the biggest value that postgres and friends
	// treat as valid.
	limit = math.MaxInt64
	if queryOpts.Limit != nil {
		limit = *queryOpts.Limit
	}

	toExecute := query.limit(limit)
	sql, args, err := toExecute.queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	queryTuples, err := tqs.Executor(ctx, sql, args)
	if err != nil {
		return nil, err
	}

	lenQueryTuples := uint64(len(queryTuples))
	if lenQueryTuples > limit {
		queryTuples = queryTuples[:limit]
	}

	return NewSliceRelationshipIterator(queryTuples, queryOpts.Sort), nil
}

// ExecuteQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteQueryFunc func(ctx context.Context, sql string, args []any) ([]tuple.Relationship, error)

// TxCleanupFunc is a function that should be executed when the caller of
// TransactionFactory is done with the transaction.
type TxCleanupFunc func(context.Context)

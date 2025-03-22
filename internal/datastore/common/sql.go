package common

import (
	"context"
	"maps"
	"math"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
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

	tracer = otel.Tracer("spicedb/internal/datastore/common")
)

// PaginationFilterType is an enumerator for pagination filter types.
type PaginationFilterType uint8

const (
	PaginationFilterTypeUnknown PaginationFilterType = iota

	// TupleComparison uses a comparison with a compound key,
	// e.g. (namespace, object_id, relation) > ('ns', '123', 'viewer')
	// which is not compatible with all datastores.
	TupleComparison = 1

	// ExpandedLogicComparison comparison uses a nested tree of ANDs and ORs to properly
	// filter out already received relationships. Useful for databases that do not support
	// tuple comparison, or do not execute it efficiently
	ExpandedLogicComparison = 2
)

// ColumnOptimizationOption is an enumerator for column optimization options.
type ColumnOptimizationOption int

const (
	ColumnOptimizationOptionUnknown ColumnOptimizationOption = iota

	// ColumnOptimizationOptionNone is the default option, which does not optimize the static columns.
	ColumnOptimizationOptionNone

	// ColumnOptimizationOptionStaticValues is an option that optimizes columns for static values.
	ColumnOptimizationOptionStaticValues
)

type columnTracker struct {
	SingleValue *string
}

type columnTrackerMap map[string]columnTracker

func (ctm columnTrackerMap) hasStaticValue(columnName string) bool {
	if r, ok := ctm[columnName]; ok && r.SingleValue != nil {
		return true
	}
	return false
}

// SchemaQueryFilterer wraps a SchemaInformation and SelectBuilder to give an opinionated
// way to build query objects.
type SchemaQueryFilterer struct {
	schema                 SchemaInformation
	queryBuilder           sq.SelectBuilder
	filteringColumnTracker columnTrackerMap
	filterMaximumIDCount   uint16
	isCustomQuery          bool
	extraFields            []string
	fromSuffix             string
}

// NewSchemaQueryFiltererForRelationshipsSelect creates a new SchemaQueryFilterer object for selecting
// relationships. This method will automatically filter the columns retrieved from the database, only
// selecting the columns that are not already specified with a single static value in the query.
func NewSchemaQueryFiltererForRelationshipsSelect(schema SchemaInformation, filterMaximumIDCount uint16, extraFields ...string) SchemaQueryFilterer {
	schema.debugValidate()

	if filterMaximumIDCount == 0 {
		filterMaximumIDCount = 100
		log.Warn().Msg("SchemaQueryFilterer: filterMaximumIDCount not set, defaulting to 100")
	}

	queryBuilder := sq.StatementBuilder.PlaceholderFormat(schema.PlaceholderFormat).Select()
	return SchemaQueryFilterer{
		schema:                 schema,
		queryBuilder:           queryBuilder,
		filteringColumnTracker: map[string]columnTracker{},
		filterMaximumIDCount:   filterMaximumIDCount,
		isCustomQuery:          false,
		extraFields:            extraFields,
	}
}

// NewSchemaQueryFiltererWithStartingQuery creates a new SchemaQueryFilterer object for selecting
// relationships, with a custom starting query. Unlike NewSchemaQueryFiltererForRelationshipsSelect,
// this method will not auto-filter the columns retrieved from the database.
func NewSchemaQueryFiltererWithStartingQuery(schema SchemaInformation, startingQuery sq.SelectBuilder, filterMaximumIDCount uint16) SchemaQueryFilterer {
	schema.debugValidate()

	if filterMaximumIDCount == 0 {
		filterMaximumIDCount = 100
		log.Warn().Msg("SchemaQueryFilterer: filterMaximumIDCount not set, defaulting to 100")
	}

	return SchemaQueryFilterer{
		schema:                 schema,
		queryBuilder:           startingQuery,
		filteringColumnTracker: map[string]columnTracker{},
		filterMaximumIDCount:   filterMaximumIDCount,
		isCustomQuery:          true,
		extraFields:            nil,
	}
}

// WithAdditionalFilter returns the SchemaQueryFilterer with an additional filter applied to the query.
func (sqf SchemaQueryFilterer) WithAdditionalFilter(filter func(original sq.SelectBuilder) sq.SelectBuilder) SchemaQueryFilterer {
	sqf.queryBuilder = filter(sqf.queryBuilder)
	return sqf
}

// WithFromSuffix returns the SchemaQueryFilterer with a suffix added to the FROM clause.
func (sqf SchemaQueryFilterer) WithFromSuffix(fromSuffix string) SchemaQueryFilterer {
	sqf.fromSuffix = fromSuffix
	return sqf
}

func (sqf SchemaQueryFilterer) UnderlyingQueryBuilder() sq.SelectBuilder {
	spiceerrors.DebugAssert(func() bool {
		return sqf.isCustomQuery
	}, "UnderlyingQueryBuilder should only be called on custom queries")
	return sqf.queryBuilderWithMaybeExpirationFilter(false)
}

// queryBuilderWithMaybeExpirationFilter returns the query builder with the expiration filter applied, when necessary.
// Note that this adds the clause to the existing builder.
func (sqf SchemaQueryFilterer) queryBuilderWithMaybeExpirationFilter(skipExpiration bool) sq.SelectBuilder {
	if sqf.schema.ExpirationDisabled || skipExpiration {
		return sqf.queryBuilder
	}

	// Filter out any expired relationships.
	return sqf.queryBuilder.Where(sq.Or{
		sq.Eq{sqf.schema.ColExpiration: nil},
		sq.Expr(sqf.schema.ColExpiration + " > " + sqf.schema.NowFunction + "()"),
	})
}

func (sqf SchemaQueryFilterer) TupleOrder(order options.SortOrder) SchemaQueryFilterer {
	switch order {
	case options.ByResource:
		sqf.queryBuilder = sqf.queryBuilder.OrderBy(
			sqf.schema.ColNamespace,
			sqf.schema.ColObjectID,
			sqf.schema.ColRelation,
			sqf.schema.ColUsersetNamespace,
			sqf.schema.ColUsersetObjectID,
			sqf.schema.ColUsersetRelation,
		)

	case options.BySubject:
		sqf.queryBuilder = sqf.queryBuilder.OrderBy(
			sqf.schema.ColUsersetNamespace,
			sqf.schema.ColUsersetObjectID,
			sqf.schema.ColUsersetRelation,
			sqf.schema.ColNamespace,
			sqf.schema.ColObjectID,
			sqf.schema.ColRelation,
		)
	}

	return sqf
}

type nameAndValue struct {
	name  string
	value string
}

func (sqf SchemaQueryFilterer) After(cursor options.Cursor, order options.SortOrder) SchemaQueryFilterer {
	spiceerrors.DebugAssertNotNil(cursor, "cursor cannot be nil")

	// NOTE: The ordering of these columns can affect query performance, be aware when changing.
	columnsAndValues := map[options.SortOrder][]nameAndValue{
		options.ByResource: {
			{
				sqf.schema.ColNamespace, cursor.Resource.ObjectType,
			},
			{
				sqf.schema.ColObjectID, cursor.Resource.ObjectID,
			},
			{
				sqf.schema.ColRelation, cursor.Resource.Relation,
			},
			{
				sqf.schema.ColUsersetNamespace, cursor.Subject.ObjectType,
			},
			{
				sqf.schema.ColUsersetObjectID, cursor.Subject.ObjectID,
			},
			{
				sqf.schema.ColUsersetRelation, cursor.Subject.Relation,
			},
		},
		options.BySubject: {
			{
				sqf.schema.ColUsersetNamespace, cursor.Subject.ObjectType,
			},
			{
				sqf.schema.ColUsersetObjectID, cursor.Subject.ObjectID,
			},
			{
				sqf.schema.ColNamespace, cursor.Resource.ObjectType,
			},
			{
				sqf.schema.ColObjectID, cursor.Resource.ObjectID,
			},
			{
				sqf.schema.ColRelation, cursor.Resource.Relation,
			},
			{
				sqf.schema.ColUsersetRelation, cursor.Subject.Relation,
			},
		},
	}[order]

	switch sqf.schema.PaginationFilterType {
	case TupleComparison:
		// For performance reasons, remove any column names that have static values in the query.
		columnNames := make([]string, 0, len(columnsAndValues))
		valueSlots := make([]any, 0, len(columnsAndValues))
		comparisonSlotCount := 0

		for _, cav := range columnsAndValues {
			if !sqf.filteringColumnTracker.hasStaticValue(cav.name) {
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
			if !sqf.filteringColumnTracker.hasStaticValue(cav.name) {
				andClause := sq.And{}
				for _, previous := range columnsAndValues[0:index] {
					if !sqf.filteringColumnTracker.hasStaticValue(previous.name) {
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
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColNamespace: resourceType})
	sqf.recordColumnValue(sqf.schema.ColNamespace, resourceType)
	return sqf
}

func (sqf SchemaQueryFilterer) recordColumnValue(colName string, colValue string) {
	existing, ok := sqf.filteringColumnTracker[colName]
	if ok {
		if existing.SingleValue != nil && *existing.SingleValue != colValue {
			sqf.filteringColumnTracker[colName] = columnTracker{SingleValue: nil}
		}
	} else {
		sqf.filteringColumnTracker[colName] = columnTracker{SingleValue: &colValue}
	}
}

func (sqf SchemaQueryFilterer) recordVaryingColumnValue(colName string) {
	sqf.filteringColumnTracker[colName] = columnTracker{SingleValue: nil}
}

// FilterToResourceID returns a new SchemaQueryFilterer that is limited to resources with the
// specified ID.
func (sqf SchemaQueryFilterer) FilterToResourceID(objectID string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColObjectID: objectID})
	sqf.recordColumnValue(sqf.schema.ColObjectID, objectID)
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

	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Like{sqf.schema.ColObjectID: prefix + "%"})

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
	builder.WriteString(sqf.schema.ColObjectID)
	builder.WriteString(" IN (")
	args := make([]any, 0, len(resourceIds))

	for _, resourceID := range resourceIds {
		if len(resourceID) == 0 {
			return sqf, spiceerrors.MustBugf("got empty resource ID")
		}

		args = append(args, resourceID)
		sqf.recordColumnValue(sqf.schema.ColObjectID, resourceID)
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
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColRelation: relation})
	sqf.recordColumnValue(sqf.schema.ColRelation, relation)
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

	if filter.OptionalExpirationOption == datastore.ExpirationFilterOptionHasExpiration {
		csqf.queryBuilder = csqf.queryBuilder.Where(sq.NotEq{csqf.schema.ColExpiration: nil})
		spiceerrors.DebugAssert(func() bool { return !sqf.schema.ExpirationDisabled }, "expiration filter requested but schema does not support expiration")
	} else if filter.OptionalExpirationOption == datastore.ExpirationFilterOptionNoExpiration {
		csqf.queryBuilder = csqf.queryBuilder.Where(sq.Eq{csqf.schema.ColExpiration: nil})
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

	// If there is more than a single filter, record all the subjects as varying, as the subjects returned
	// can differ for each branch.
	// TODO(jschorr): Optimize this further where applicable.
	if len(selectors) > 1 {
		sqf.recordVaryingColumnValue(sqf.schema.ColUsersetNamespace)
		sqf.recordVaryingColumnValue(sqf.schema.ColUsersetObjectID)
		sqf.recordVaryingColumnValue(sqf.schema.ColUsersetRelation)
	}

	for _, selector := range selectors {
		selectorClause := sq.And{}

		if len(selector.OptionalSubjectType) > 0 {
			selectorClause = append(selectorClause, sq.Eq{sqf.schema.ColUsersetNamespace: selector.OptionalSubjectType})
			sqf.recordColumnValue(sqf.schema.ColUsersetNamespace, selector.OptionalSubjectType)
		}

		if len(selector.OptionalSubjectIds) > 0 {
			spiceerrors.DebugAssert(func() bool {
				return len(selector.OptionalSubjectIds) <= int(sqf.filterMaximumIDCount)
			}, "cannot have more than %d subject IDs in a single filter", sqf.filterMaximumIDCount)

			var builder strings.Builder
			builder.WriteString(sqf.schema.ColUsersetObjectID)
			builder.WriteString(" IN (")
			args := make([]any, 0, len(selector.OptionalSubjectIds))

			for _, subjectID := range selector.OptionalSubjectIds {
				if len(subjectID) == 0 {
					return sqf, spiceerrors.MustBugf("got empty subject ID")
				}

				args = append(args, subjectID)
				sqf.recordColumnValue(sqf.schema.ColUsersetObjectID, subjectID)
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
				selectorClause = append(selectorClause, sq.NotEq{sqf.schema.ColUsersetRelation: datastore.Ellipsis})
				sqf.recordVaryingColumnValue(sqf.schema.ColUsersetRelation)
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
					selectorClause = append(selectorClause, sq.Eq{sqf.schema.ColUsersetRelation: relName})
					sqf.recordColumnValue(sqf.schema.ColUsersetRelation, relName)
				} else {
					orClause := sq.Or{}
					for _, relationName := range relations {
						dsRelationName := stringz.DefaultEmpty(relationName, datastore.Ellipsis)
						orClause = append(orClause, sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
						sqf.recordColumnValue(sqf.schema.ColUsersetRelation, dsRelationName)
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
	sqf.recordColumnValue(sqf.schema.ColUsersetNamespace, filter.SubjectType)

	if filter.OptionalSubjectId != "" {
		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetObjectID: filter.OptionalSubjectId})
		sqf.recordColumnValue(sqf.schema.ColUsersetObjectID, filter.OptionalSubjectId)
	}

	if filter.OptionalRelation != nil {
		dsRelationName := stringz.DefaultEmpty(filter.OptionalRelation.Relation, datastore.Ellipsis)

		sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColUsersetRelation: dsRelationName})
		sqf.recordColumnValue(sqf.schema.ColUsersetRelation, datastore.Ellipsis)
	}

	return sqf
}

func (sqf SchemaQueryFilterer) FilterWithCaveatName(caveatName string) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Where(sq.Eq{sqf.schema.ColCaveatName: caveatName})
	sqf.recordColumnValue(sqf.schema.ColCaveatName, caveatName)
	return sqf
}

// Limit returns a new SchemaQueryFilterer which is limited to the specified number of results.
func (sqf SchemaQueryFilterer) limit(limit uint64) SchemaQueryFilterer {
	sqf.queryBuilder = sqf.queryBuilder.Limit(limit)
	return sqf
}

// QueryRelationshipsExecutor is a relationships query runner shared by SQL implementations of the datastore.
type QueryRelationshipsExecutor struct {
	Executor ExecuteReadRelsQueryFunc
}

// ExecuteReadRelsQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteReadRelsQueryFunc func(ctx context.Context, builder RelationshipsQueryBuilder) (datastore.RelationshipIterator, error)

// ExecuteQuery executes the query.
func (exc QueryRelationshipsExecutor) ExecuteQuery(
	ctx context.Context,
	query SchemaQueryFilterer,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if query.isCustomQuery {
		return nil, spiceerrors.MustBugf("ExecuteQuery should not be called on custom queries")
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	// Add sort order.
	query = query.TupleOrder(queryOpts.Sort)

	// Add cursor.
	if queryOpts.After != nil {
		if queryOpts.Sort == options.Unsorted {
			return nil, datastore.ErrCursorsWithoutSorting
		}

		query = query.After(queryOpts.After, queryOpts.Sort)
	}

	// Add limit.
	var limit uint64
	// NOTE: we use a uint here because it lines up with the
	// assignments in this function, but we set it to MaxInt64
	// because that's the biggest value that postgres and friends
	// treat as valid.
	limit = math.MaxInt64
	if queryOpts.Limit != nil {
		limit = *queryOpts.Limit
	}

	if limit < math.MaxInt64 {
		query = query.limit(limit)
	}

	// Add FROM clause.
	from := query.schema.RelationshipTableName
	if query.fromSuffix != "" {
		from += " " + query.fromSuffix
	}

	query.queryBuilder = query.queryBuilder.From(from)

	builder := RelationshipsQueryBuilder{
		Schema:                    query.schema,
		SkipCaveats:               queryOpts.SkipCaveats,
		SkipExpiration:            queryOpts.SkipExpiration,
		SQLCheckAssertionForTest:  queryOpts.SQLCheckAssertionForTest,
		SQLExplainCallbackForTest: queryOpts.SQLExplainCallbackForTest,
		filteringValues:           query.filteringColumnTracker,
		queryShape:                queryOpts.QueryShape,
		baseQueryBuilder:          query,
	}

	return exc.Executor(ctx, builder)
}

// RelationshipsQueryBuilder is a builder for producing the SQL and arguments necessary for reading
// relationships.
type RelationshipsQueryBuilder struct {
	Schema         SchemaInformation
	SkipCaveats    bool
	SkipExpiration bool

	filteringValues           columnTrackerMap
	baseQueryBuilder          SchemaQueryFilterer
	SQLCheckAssertionForTest  options.SQLCheckAssertionForTest
	SQLExplainCallbackForTest options.SQLExplainCallbackForTest
	queryShape                queryshape.Shape
}

// withCaveats returns true if caveats should be included in the query.
func (b RelationshipsQueryBuilder) withCaveats() bool {
	return !b.SkipCaveats || b.Schema.ColumnOptimization == ColumnOptimizationOptionNone
}

// withExpiration returns true if expiration should be included in the query.
func (b RelationshipsQueryBuilder) withExpiration() bool {
	return !b.SkipExpiration && !b.Schema.ExpirationDisabled
}

// integrityEnabled returns true if integrity columns should be included in the query.
func (b RelationshipsQueryBuilder) integrityEnabled() bool {
	return b.Schema.IntegrityEnabled
}

// columnCount returns the number of columns that will be selected in the query.
func (b RelationshipsQueryBuilder) columnCount() int {
	columnCount := relationshipStandardColumnCount
	if b.withCaveats() {
		columnCount += relationshipCaveatColumnCount
	}
	if b.withExpiration() {
		columnCount += relationshipExpirationColumnCount
	}
	if b.integrityEnabled() {
		columnCount += relationshipIntegrityColumnCount
	}
	return columnCount
}

// SelectSQL returns the SQL and arguments necessary for reading relationships.
func (b RelationshipsQueryBuilder) SelectSQL() (string, []any, error) {
	// Set the column names to select.
	columnNamesToSelect := make([]string, 0, b.columnCount())

	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColNamespace)
	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColObjectID)
	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColRelation)
	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColUsersetNamespace)
	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColUsersetObjectID)
	columnNamesToSelect = b.checkColumn(columnNamesToSelect, b.Schema.ColUsersetRelation)

	if b.withCaveats() {
		columnNamesToSelect = append(columnNamesToSelect, b.Schema.ColCaveatName, b.Schema.ColCaveatContext)
	}

	if b.withExpiration() {
		columnNamesToSelect = append(columnNamesToSelect, b.Schema.ColExpiration)
	}

	if b.integrityEnabled() {
		columnNamesToSelect = append(columnNamesToSelect, b.Schema.ColIntegrityKeyID, b.Schema.ColIntegrityHash, b.Schema.ColIntegrityTimestamp)
	}

	if len(columnNamesToSelect) == 0 {
		columnNamesToSelect = append(columnNamesToSelect, "1")
	}

	sqlBuilder := b.baseQueryBuilder.queryBuilderWithMaybeExpirationFilter(b.SkipExpiration)
	sqlBuilder = sqlBuilder.Columns(columnNamesToSelect...)

	sql, args, err := sqlBuilder.ToSql()
	if err != nil {
		return "", nil, err
	}

	if b.SQLCheckAssertionForTest != nil {
		b.SQLCheckAssertionForTest(sql)
	}

	return sql, args, nil
}

// FilteringValuesForTesting returns the filtering values. For test use only.
func (b RelationshipsQueryBuilder) FilteringValuesForTesting() map[string]columnTracker {
	return maps.Clone(b.filteringValues)
}

func (b RelationshipsQueryBuilder) checkColumn(columns []string, colName string) []string {
	if b.Schema.ColumnOptimization == ColumnOptimizationOptionNone {
		return append(columns, colName)
	}

	if !b.filteringValues.hasStaticValue(colName) {
		return append(columns, colName)
	}

	return columns
}

func (b RelationshipsQueryBuilder) staticValueOrAddColumnForSelect(colsToSelect []any, colName string, field *string) []any {
	if b.Schema.ColumnOptimization == ColumnOptimizationOptionNone {
		// If column optimization is disabled, always add the column to the list of columns to select.
		colsToSelect = append(colsToSelect, field)
		return colsToSelect
	}

	// If the value is static, set the field to it and return.
	if found, ok := b.filteringValues[colName]; ok && found.SingleValue != nil {
		*field = *found.SingleValue
		return colsToSelect
	}

	// Otherwise, add the column to the list of columns to select, as the value is not static.
	colsToSelect = append(colsToSelect, field)
	return colsToSelect
}

// ColumnsToSelect returns the columns to select for a given query. The columns provided are
// the references to the slots in which the values for each relationship will be placed.
func ColumnsToSelect[CN any, CC any, EC any](
	b RelationshipsQueryBuilder,
	resourceObjectType *string,
	resourceObjectID *string,
	resourceRelation *string,
	subjectObjectType *string,
	subjectObjectID *string,
	subjectRelation *string,
	caveatName *CN,
	caveatCtx *CC,
	expiration EC,

	integrityKeyID *string,
	integrityHash *[]byte,
	timestamp *time.Time,
) ([]any, error) {
	colsToSelect := make([]any, 0, b.columnCount())

	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColNamespace, resourceObjectType)
	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColObjectID, resourceObjectID)
	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColRelation, resourceRelation)
	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColUsersetNamespace, subjectObjectType)
	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColUsersetObjectID, subjectObjectID)
	colsToSelect = b.staticValueOrAddColumnForSelect(colsToSelect, b.Schema.ColUsersetRelation, subjectRelation)

	if b.withCaveats() {
		colsToSelect = append(colsToSelect, caveatName, caveatCtx)
	}

	if b.withExpiration() {
		colsToSelect = append(colsToSelect, expiration)
	}

	if b.Schema.IntegrityEnabled {
		colsToSelect = append(colsToSelect, integrityKeyID, integrityHash, timestamp)
	}

	if len(colsToSelect) == 0 {
		var unused int
		colsToSelect = append(colsToSelect, &unused)
	}

	return colsToSelect, nil
}

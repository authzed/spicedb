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
	RelationshipTableName string
	ColNamespace          string
	ColObjectID           string
	ColRelation           string
	ColUsersetNamespace   string
	ColUsersetObjectID    string
	ColUsersetRelation    string
	ColCaveatName         string
	ColCaveatContext      string
	PaginationFilterType  PaginationFilterType
	PlaceholderFormat     sq.PlaceholderFormat

	// ExtaFields are additional fields that are not part of the core schema, but are
	// requested by the caller for this query.
	ExtraFields []string
}

// NewSchemaInformation creates a new SchemaInformation object for a query.
func NewSchemaInformation(
	relationshipTableName,
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatName string,
	colCaveatContext string,
	paginationFilterType PaginationFilterType,
	placeholderFormat sq.PlaceholderFormat,
	extraFields ...string,
) SchemaInformation {
	return SchemaInformation{
		relationshipTableName,
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatName,
		colCaveatContext,
		paginationFilterType,
		placeholderFormat,
		extraFields,
	}
}

type ColumnTracker struct {
	SingleValue *string
}

// SchemaQueryFilterer wraps a SchemaInformation and SelectBuilder to give an opinionated
// way to build query objects.
type SchemaQueryFilterer struct {
	schema                 SchemaInformation
	queryBuilder           sq.SelectBuilder
	filteringColumnTracker map[string]ColumnTracker
	filterMaximumIDCount   uint16
	isCustomQuery          bool
	extraFields            []string
	fromSuffix             string
}

// NewSchemaQueryFiltererForRelationshipsSelect creates a new SchemaQueryFilterer object for selecting
// relationships. This method will automatically filter the columns retrieved from the database, only
// selecting the columns that are not already specified with a single static value in the query.
func NewSchemaQueryFiltererForRelationshipsSelect(schema SchemaInformation, filterMaximumIDCount uint16, extraFields ...string) SchemaQueryFilterer {
	if filterMaximumIDCount == 0 {
		filterMaximumIDCount = 100
		log.Warn().Msg("SchemaQueryFilterer: filterMaximumIDCount not set, defaulting to 100")
	}

	return SchemaQueryFilterer{
		schema:                 schema,
		queryBuilder:           sq.StatementBuilder.PlaceholderFormat(schema.PlaceholderFormat).Select(),
		filteringColumnTracker: map[string]ColumnTracker{},
		filterMaximumIDCount:   filterMaximumIDCount,
		isCustomQuery:          false,
		extraFields:            extraFields,
	}
}

// NewSchemaQueryFiltererWithStartingQuery creates a new SchemaQueryFilterer object for selecting
// relationships, with a custom starting query. Unlike NewSchemaQueryFiltererForRelationshipsSelect,
// this method will not auto-filter the columns retrieved from the database.
func NewSchemaQueryFiltererWithStartingQuery(schema SchemaInformation, startingQuery sq.SelectBuilder, filterMaximumIDCount uint16) SchemaQueryFilterer {
	if filterMaximumIDCount == 0 {
		filterMaximumIDCount = 100
		log.Warn().Msg("SchemaQueryFilterer: filterMaximumIDCount not set, defaulting to 100")
	}

	return SchemaQueryFilterer{
		schema:                 schema,
		queryBuilder:           startingQuery,
		filteringColumnTracker: map[string]ColumnTracker{},
		filterMaximumIDCount:   filterMaximumIDCount,
		isCustomQuery:          true,
		extraFields:            nil,
	}
}

// WithAdditionalFilter returns a new SchemaQueryFilterer with an additional filter applied to the query.
func (sqf SchemaQueryFilterer) WithAdditionalFilter(filter func(original sq.SelectBuilder) sq.SelectBuilder) SchemaQueryFilterer {
	return SchemaQueryFilterer{
		schema:                 sqf.schema,
		queryBuilder:           filter(sqf.queryBuilder),
		filteringColumnTracker: sqf.filteringColumnTracker,
		filterMaximumIDCount:   sqf.filterMaximumIDCount,
		isCustomQuery:          sqf.isCustomQuery,
		extraFields:            sqf.extraFields,
	}
}

func (sqf SchemaQueryFilterer) WithFromSuffix(fromSuffix string) SchemaQueryFilterer {
	return SchemaQueryFilterer{
		schema:                 sqf.schema,
		queryBuilder:           sqf.queryBuilder,
		filteringColumnTracker: sqf.filteringColumnTracker,
		filterMaximumIDCount:   sqf.filterMaximumIDCount,
		isCustomQuery:          sqf.isCustomQuery,
		extraFields:            sqf.extraFields,
		fromSuffix:             fromSuffix,
	}
}

func (sqf SchemaQueryFilterer) UnderlyingQueryBuilder() sq.SelectBuilder {
	spiceerrors.DebugAssert(func() bool {
		return sqf.isCustomQuery
	}, "UnderlyingQueryBuilder should only be called on custom queries")
	return sqf.queryBuilder
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
			if r, ok := sqf.filteringColumnTracker[cav.name]; !ok || r.SingleValue == nil {
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
			if r, ok := sqf.filteringColumnTracker[cav.name]; !ok || r.SingleValue != nil {
				andClause := sq.And{}
				for _, previous := range columnsAndValues[0:index] {
					if r, ok := sqf.filteringColumnTracker[previous.name]; !ok || r.SingleValue != nil {
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
			sqf.filteringColumnTracker[colName] = ColumnTracker{SingleValue: nil}
		}
	} else {
		sqf.filteringColumnTracker[colName] = ColumnTracker{SingleValue: &colValue}
	}
}

func (sqf SchemaQueryFilterer) recordMutableColumnValue(colName string) {
	sqf.filteringColumnTracker[colName] = ColumnTracker{SingleValue: nil}
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

	// If there is more than a single filter, record all the subjects as mutable, as the subjects returned
	// can differ for each branch.
	// TODO(jschorr): Optimize this further where applicable.
	if len(selectors) > 1 {
		sqf.recordMutableColumnValue(sqf.schema.ColUsersetNamespace)
		sqf.recordMutableColumnValue(sqf.schema.ColUsersetObjectID)
		sqf.recordMutableColumnValue(sqf.schema.ColUsersetRelation)
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
				sqf.recordMutableColumnValue(sqf.schema.ColUsersetRelation)
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

// QueryExecutor is a tuple query runner shared by SQL implementations of the datastore.
type QueryExecutor struct {
	Executor ExecuteReadRelsQueryFunc
}

// ExecuteQuery executes the query.
func (tqs QueryExecutor) ExecuteQuery(
	ctx context.Context,
	query SchemaQueryFilterer,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if query.isCustomQuery {
		return nil, spiceerrors.MustBugf("ExecuteQuery should not be called on custom queries")
	}

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

	if limit < math.MaxInt64 {
		query = query.limit(limit)
	}

	toExecute := query

	// Set the column names to select.
	columnNamesToSelect := make([]string, 0, 8+len(query.extraFields))

	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColNamespace)
	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColObjectID)
	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColRelation)
	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColUsersetNamespace)
	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColUsersetObjectID)
	columnNamesToSelect = checkColumn(columnNamesToSelect, query.filteringColumnTracker, query.schema.ColUsersetRelation)

	columnNamesToSelect = append(columnNamesToSelect, query.schema.ColCaveatName, query.schema.ColCaveatContext)
	columnNamesToSelect = append(columnNamesToSelect, query.schema.ExtraFields...)

	toExecute.queryBuilder = toExecute.queryBuilder.Columns(columnNamesToSelect...)

	from := query.schema.RelationshipTableName
	if query.fromSuffix != "" {
		from += " " + query.fromSuffix
	}

	toExecute.queryBuilder = toExecute.queryBuilder.From(from)

	sql, args, err := toExecute.queryBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	return tqs.Executor(ctx, QueryInfo{query.schema, query.filteringColumnTracker}, sql, args)
}

func checkColumn(columns []string, tracker map[string]ColumnTracker, colName string) []string {
	if r, ok := tracker[colName]; !ok || r.SingleValue == nil {
		return append(columns, colName)
	}
	return columns
}

// QueryInfo holds the schema information and filtering values for a query.
type QueryInfo struct {
	Schema          SchemaInformation
	FilteringValues map[string]ColumnTracker
}

// ExecuteReadRelsQueryFunc is a function that can be used to execute a single rendered SQL query.
type ExecuteReadRelsQueryFunc func(ctx context.Context, queryInfo QueryInfo, sql string, args []any) (datastore.RelationshipIterator, error)

// TxCleanupFunc is a function that should be executed when the caller of
// TransactionFactory is done with the transaction.
type TxCleanupFunc func(context.Context)

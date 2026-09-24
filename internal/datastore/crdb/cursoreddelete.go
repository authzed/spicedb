package crdb

import (
	"cmp"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// cursorOrderForIndex returns the cursor sort order matching the physical key
// order of the given index, along with the index to actually force.
//
// An index whose key order does not match a cursor order cannot serve a
// cursored delete: the ORDER BY would not be served by the index scan, forcing
// CockroachDB to sort the entire matching set before applying LIMIT, which is a
// full scan on every batch — exactly what cursoring exists to avoid. Such
// indexes, and the no-forced-index case, fall back to the primary key.
func cursorOrderForIndex(index *common.IndexDefinition) (options.SortOrder, *common.IndexDefinition) {
	if index == nil {
		return options.ByResource, &schema.IndexPrimaryKey
	}

	switch index.Name {
	case schema.IndexPrimaryKey.Name:
		return options.ByResource, &schema.IndexPrimaryKey

	case schema.IndexRelationshipWithIntegrity.Name:
		return options.ByResource, &schema.IndexRelationshipWithIntegrity

	case schema.IndexRelationshipBySubject.Name:
		// Key order plus CockroachDB's implicit primary-key suffix is
		// (userset_object_id, userset_namespace, userset_relation, namespace,
		// relation, object_id), which is exactly SortBySubjectColumnOrder.
		return options.BySubject, &schema.IndexRelationshipBySubject

	default:
		return options.ByResource, &schema.IndexPrimaryKey
	}
}

// buildCursoredDeleteQuery builds the CTE that deletes one ordered batch of
// relationships and returns both the number deleted and the last row in delete
// order, so the caller can resume after it.
func buildCursoredDeleteQuery(
	schemaInfo common.SchemaInformation,
	filter *v1.RelationshipFilter,
	delOpts *options.DeleteOptions,
) (string, []any, options.SortOrder, error) {
	if err := delOpts.ValidateCursoredDelete(); err != nil {
		return "", nil, options.Unsorted, err
	}

	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
	if err != nil {
		return "", nil, options.Unsorted, fmt.Errorf("unable to translate relationship filter: %w", err)
	}

	candidate, err := schema.IndexForFilter(schemaInfo, dsFilter)
	if err != nil {
		return "", nil, options.Unsorted, fmt.Errorf("unable to determine index for filter: %w", err)
	}

	order, index := cursorOrderForIndex(candidate)

	// AVOID_FULL_SCAN penalizes full scans of the forced index, so the optimizer
	// prefers the constrained span built from the equality filters and cursor
	// bounds below. It is the soft counterpart of NO_FULL_SCAN: an unconstrained
	// filter (delete-everything, or the first batch before a cursor exists)
	// legitimately scans from the start, and NO_FULL_SCAN would error that valid
	// case. Applied only on this cursored path, never to ordinary deletes.
	from := schemaInfo.RelationshipTableName +
		"@{FORCE_INDEX=" + index.Name + ", AVOID_FULL_SCAN}"

	query := psql.Delete(from)

	// staticColumns records the columns pinned to a single value by an equality
	// below, so they can be dropped from the ordering and from the cursor
	// comparison.
	staticColumns := map[string]struct{}{}
	eq := func(column, value string) {
		query = query.Where(sq.Eq{column: value})
		staticColumns[column] = struct{}{}
	}

	if filter.ResourceType != "" {
		eq(schema.ColNamespace, filter.ResourceType)
	}
	if filter.OptionalResourceId != "" {
		eq(schema.ColObjectID, filter.OptionalResourceId)
	}
	if filter.OptionalRelation != "" {
		eq(schema.ColRelation, filter.OptionalRelation)
	}
	if filter.OptionalResourceIdPrefix != "" {
		likeClause, err := common.BuildLikePrefixClause(schema.ColObjectID, filter.OptionalResourceIdPrefix)
		if err != nil {
			return "", nil, options.Unsorted, fmt.Errorf("unable to build like clause: %w", err)
		}
		// A prefix is not a single value, so object_id is NOT static here: it
		// must stay in the ordering and in the cursor comparison.
		query = query.Where(likeClause)
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		eq(schema.ColUsersetNamespace, subjectFilter.SubjectType)
		if subjectFilter.OptionalSubjectId != "" {
			eq(schema.ColUsersetObjectID, subjectFilter.OptionalSubjectId)
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			eq(schema.ColUsersetRelation, cmp.Or(relationFilter.Relation, datastore.Ellipsis))
		}
	}

	if delOpts.DeleteAfter != nil {
		expr, err := common.CursorComparisonExpr(
			schemaInfo, order, delOpts.DeleteAfter,
			// Deliberately TupleComparison, not schemaInfo.PaginationFilterType:
			// CockroachDB's optimizer turns the tuple form directly into a
			// constrained span over the forced index.
			common.TupleComparison,
			func(name string) bool {
				_, ok := staticColumns[name]
				return ok
			},
		)
		if err != nil {
			return "", nil, options.Unsorted, err
		}
		if expr != nil {
			query = query.Where(expr)
		}
	}

	orderColumns, err := cursorOrderColumns(schemaInfo, order, staticColumns)
	if err != nil {
		return "", nil, options.Unsorted, err
	}

	returningColumns := relationshipKeyColumns(schemaInfo)

	descending := make([]string, 0, len(orderColumns))
	for _, column := range orderColumns {
		descending = append(descending, column+" DESC")
	}

	query = query.
		OrderBy(orderColumns...).
		Limit(*delOpts.DeleteLimit).
		Prefix("WITH deleted AS (").
		Suffix("RETURNING " + strings.Join(returningColumns, ", ") + ")" +
			" SELECT " + strings.Join(returningColumns, ", ") +
			", count(*) OVER () AS total FROM deleted" +
			" ORDER BY " + strings.Join(descending, ", ") +
			" LIMIT 1")

	sql, args, err := query.ToSql()
	if err != nil {
		return "", nil, options.Unsorted, err
	}

	return sql, args, order, nil
}

// cursorOrderColumns returns the ORDER BY columns for a cursored delete,
// omitting columns already pinned to a single value.
func cursorOrderColumns(
	schemaInfo common.SchemaInformation,
	order options.SortOrder,
	staticColumns map[string]struct{},
) ([]string, error) {
	all, err := schemaInfo.CursorColumns(order)
	if err != nil {
		return nil, err
	}

	ordered := make([]string, 0, len(all))
	for _, column := range all {
		if _, ok := staticColumns[column]; ok {
			continue
		}
		ordered = append(ordered, column)
	}

	if len(ordered) == 0 {
		// Every key column is pinned by an equality, so the filter matches at
		// most one row and there is nothing left to page through. Order by the
		// full (unfiltered) key column list instead of erroring: it is redundant
		// but still valid and index-served, and it keeps the CTE shape intact so
		// the statement still RETURNs a row and yields a cursor. The resumed
		// batch's cursor comparison then has no non-static columns either, so it
		// degenerates to no predicate at all — the same filter re-runs, matches
		// nothing (the row is already gone), and the loop terminates.
		return all, nil
	}

	return ordered, nil
}

// relationshipKeyColumns returns the six primary-key columns, in resource
// order, for RETURNING and for reconstructing the cursor.
func relationshipKeyColumns(schemaInfo common.SchemaInformation) []string {
	return []string{
		schemaInfo.ColNamespace,
		schemaInfo.ColObjectID,
		schemaInfo.ColRelation,
		schemaInfo.ColUsersetNamespace,
		schemaInfo.ColUsersetObjectID,
		schemaInfo.ColUsersetRelation,
	}
}

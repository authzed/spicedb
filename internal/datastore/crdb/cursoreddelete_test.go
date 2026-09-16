package crdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func testSchemaInfo(t *testing.T) common.SchemaInformation {
	t.Helper()
	return *schema.Schema(common.ColumnOptimizationOptionStaticValues, false, false)
}

func TestBuildCursoredDeleteQueryFirstBatch(t *testing.T) {
	limit := uint64(1000)
	sql, args, order, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		&v1.RelationshipFilter{ResourceType: "document"},
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithCursoredDelete(true),
		),
	)
	require.NoError(t, err)
	require.Equal(t, options.ByResource, order)
	require.Equal(t, []any{"document"}, args)

	t.Log(sql)

	require.Contains(t, sql, "WITH deleted AS ( DELETE FROM relation_tuple@{FORCE_INDEX=pk_relation_tuple, AVOID_FULL_SCAN}")
	require.Contains(t, sql, "WHERE namespace = $1")
	require.Contains(t, sql, "ORDER BY object_id, relation, userset_namespace, userset_object_id, userset_relation")
	require.Contains(t, sql, "LIMIT 1000")
	require.Contains(t, sql, "RETURNING namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)")
	require.Contains(t, sql, "count(*) OVER () AS total FROM deleted")
	require.Contains(t, sql, "ORDER BY object_id DESC, relation DESC, userset_namespace DESC, userset_object_id DESC, userset_relation DESC LIMIT 1")
}

func TestBuildCursoredDeleteQueryResumedBatch(t *testing.T) {
	limit := uint64(1000)
	cursor := options.ToCursor(tuple.MustParse("document:doc1#viewer@user:alice"))

	sql, args, _, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		&v1.RelationshipFilter{ResourceType: "document"},
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithDeleteAfter(cursor),
		),
	)
	require.NoError(t, err)

	// namespace is pinned by the equality, so it is excluded from the tuple
	// comparison; the remaining five primary-key columns are compared.
	require.Equal(t, []any{"document", "doc1", "viewer", "user", "alice", "..."}, args)
	require.Contains(t, sql,
		"(object_id,relation,userset_namespace,userset_object_id,userset_relation) > ($2,$3,$4,$5,$6)")
}

func TestBuildCursoredDeleteQueryUsesTupleComparisonNotSchemaDefault(t *testing.T) {
	// CockroachDB's schema declares ExpandedLogicComparison for reads; the
	// cursored delete must still emit the tuple form, which the optimizer turns
	// into a constrained span over the forced index.
	require.EqualValues(t, common.ExpandedLogicComparison, testSchemaInfo(t).PaginationFilterType)

	limit := uint64(10)
	cursor := options.ToCursor(tuple.MustParse("document:doc1#viewer@user:alice"))
	sql, _, _, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		&v1.RelationshipFilter{ResourceType: "document"},
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithDeleteAfter(cursor),
		),
	)
	require.NoError(t, err)
	require.NotContains(t, sql, " OR ")
}

func TestBuildCursoredDeleteQuerySubjectFilterUsesSubjectIndex(t *testing.T) {
	limit := uint64(10)
	sql, _, order, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		&v1.RelationshipFilter{
			OptionalSubjectFilter: &v1.SubjectFilter{
				SubjectType:       "user",
				OptionalSubjectId: "alice",
			},
		},
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithCursoredDelete(true),
		),
	)
	require.NoError(t, err)
	require.Equal(t, options.BySubject, order)
	require.Contains(t, sql, "FORCE_INDEX=ix_relation_tuple_by_subject,")
}

func TestBuildCursoredDeleteQueryRequiresLimit(t *testing.T) {
	_, _, _, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		&v1.RelationshipFilter{ResourceType: "document"},
		options.NewDeleteOptionsWithOptions(options.WithCursoredDelete(true)),
	)
	require.ErrorContains(t, err, "requires a delete limit")
}

func TestBuildCursoredDeleteQueryFullyPinnedFilterFallsBackToFullOrdering(t *testing.T) {
	// A filter that pins every primary-key column by equality -- e.g. an
	// operator deleting one precisely-identified relationship via the CLI,
	// which requires all seven filter flags -- leaves no non-static column to
	// order by. This must not error: it must fall back to ordering by the
	// full (redundant but valid, and still index-served) key column list, so
	// the CTE shape is unchanged and the statement still RETURNs a row and
	// yields a cursor.
	fullyPinnedFilter := &v1.RelationshipFilter{
		ResourceType:       "document",
		OptionalResourceId: "doc1",
		OptionalRelation:   "viewer",
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       "user",
			OptionalSubjectId: "alice",
			OptionalRelation:  &v1.SubjectFilter_RelationFilter{Relation: ""},
		},
	}

	limit := uint64(1)
	sql, args, order, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		fullyPinnedFilter,
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithCursoredDelete(true),
		),
	)
	require.NoError(t, err)
	require.Equal(t, options.ByResource, order)
	require.Equal(t, []any{"document", "doc1", "viewer", "user", "alice", "..."}, args)

	t.Log(sql)

	require.Contains(t, sql, "WITH deleted AS ( DELETE FROM relation_tuple@{FORCE_INDEX=pk_relation_tuple, AVOID_FULL_SCAN}")
	require.Contains(t, sql, "ORDER BY namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation")
	require.Contains(t, sql, "LIMIT 1")
	require.Contains(t, sql, "RETURNING namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)")
	require.Contains(t, sql, "ORDER BY namespace DESC, object_id DESC, relation DESC, userset_namespace DESC, userset_object_id DESC, userset_relation DESC LIMIT 1")

	// The second batch (as issued after the row is deleted and its cursor
	// returned) has every column static, so the tuple comparison degenerates
	// to no predicate at all: the unchanged filter re-runs, matches nothing
	// because the row is already gone, and the loop terminates instead of
	// looping forever.
	cursor := options.ToCursor(tuple.MustParse("document:doc1#viewer@user:alice"))
	sql2, args2, _, err := buildCursoredDeleteQuery(
		testSchemaInfo(t),
		fullyPinnedFilter,
		options.NewDeleteOptionsWithOptions(
			options.WithDeleteLimit(&limit),
			options.WithDeleteAfter(cursor),
		),
	)
	require.NoError(t, err)
	require.Equal(t, []any{"document", "doc1", "viewer", "user", "alice", "..."}, args2)
	require.NotContains(t, sql2, ") > (", "with every column static, no cursor comparison predicate should be emitted")
}

func TestCursorOrderForIndexRejectsUnalignedIndex(t *testing.T) {
	// ix_relation_tuple_by_subject_relation declares BySubject as its preferred
	// sort order, but its key order leads with userset_namespace while the
	// subject cursor order leads with userset_object_id. Ordering by a cursor
	// order the index cannot serve would force a sort of the whole matching set
	// before LIMIT, so it must fall back to the primary key.
	order, index := cursorOrderForIndex(&schema.IndexRelationshipBySubjectRelation)
	require.Equal(t, options.ByResource, order)
	require.Equal(t, schema.IndexPrimaryKey.Name, index.Name)

	order, index = cursorOrderForIndex(nil)
	require.Equal(t, options.ByResource, order)
	require.Equal(t, schema.IndexPrimaryKey.Name, index.Name)

	order, index = cursorOrderForIndex(&schema.IndexRelationshipBySubject)
	require.Equal(t, options.BySubject, order)
	require.Equal(t, schema.IndexRelationshipBySubject.Name, index.Name)
}

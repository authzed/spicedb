package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

// aliasOutline wraps child in an AliasIteratorType outline for the given relation name.
func aliasOutline(relationName string, child query.Outline) query.Outline {
	return query.Outline{
		Type:        query.AliasIteratorType,
		Args:        &query.IteratorArgs{RelationName: relationName},
		SubOutlines: []query.Outline{child},
	}
}

// dsOutlineNoCaveat returns a minimal DatastoreIteratorType outline for testing.
func dsOutlineNoCaveat() query.Outline {
	rel := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", "", false)
	return query.Outline{
		Type: query.DatastoreIteratorType,
		Args: &query.IteratorArgs{Relation: rel},
	}
}

// applyCollapse runs aliasChainCollapse bottom-up over outline via MutateOutline.
func applyCollapse(outline query.Outline) query.Outline {
	return query.MutateOutline(outline, []query.OutlineMutation{collapseAliasChain})
}

func TestAliasChainCollapse(t *testing.T) {
	t.Run("collapses two-level alias chain", func(t *testing.T) {
		// Alias("viewer")(Alias("owner")(DS))
		input := aliasOutline("viewer",
			aliasOutline("owner", dsOutlineNoCaveat()),
		)

		result := applyCollapse(input)

		// → Alias{RelationName:"owner", AliasedAs:["viewer"]}(DS)
		// Inner identity preserved for caching; outer name appended to the
		// alias chain so the user-facing relation and self-edge can use "viewer".
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "owner", result.Args.RelationName)
		require.Equal(t, []string{"viewer"}, result.Args.AliasedAs)
		require.Len(t, result.SubOutlines, 1)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("collapses three-level alias chain", func(t *testing.T) {
		// Alias("perm")(Alias("viewer")(Alias("owner")(DS)))
		input := aliasOutline("perm",
			aliasOutline("viewer",
				aliasOutline("owner", dsOutlineNoCaveat()),
			),
		)

		result := applyCollapse(input)

		// → Alias{RelationName:"owner", AliasedAs:["viewer","perm"]}(DS)
		// Inner-to-outer order; outermost name is last and is the user-facing relation.
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "owner", result.Args.RelationName)
		require.Equal(t, []string{"viewer", "perm"}, result.Args.AliasedAs)
		require.Len(t, result.SubOutlines, 1)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("leaves single alias unchanged", func(t *testing.T) {
		// Alias("viewer")(DS)
		input := aliasOutline("viewer", dsOutlineNoCaveat())

		result := applyCollapse(input)

		// → Alias("viewer")(DS) — unchanged, no chain extension
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "viewer", result.Args.RelationName)
		require.Empty(t, result.Args.AliasedAs)
		require.Len(t, result.SubOutlines, 1)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("leaves non-alias node unchanged", func(t *testing.T) {
		// DS — no alias at all
		input := dsOutlineNoCaveat()

		result := applyCollapse(input)

		require.Equal(t, query.DatastoreIteratorType, result.Type)
	})

	t.Run("collapses alias chain inside union", func(t *testing.T) {
		// Union[Alias("viewer")(Alias("owner")(DS)), DS]
		left := aliasOutline("viewer", aliasOutline("owner", dsOutlineNoCaveat()))
		input := query.Outline{
			Type:        query.UnionIteratorType,
			SubOutlines: []query.Outline{left, dsOutlineNoCaveat()},
		}

		result := applyCollapse(input)

		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		// Left branch: collapsed to Alias{RelationName:"owner", AliasedAs:["viewer"]}(DS)
		require.Equal(t, query.AliasIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, "owner", result.SubOutlines[0].Args.RelationName)
		require.Equal(t, []string{"viewer"}, result.SubOutlines[0].Args.AliasedAs)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].SubOutlines[0].Type)
		// Right branch: unchanged DS
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("collapses alias chain inside arrow", func(t *testing.T) {
		// Alias("viewer")(Alias("owner")(DS)) -> DS
		left := aliasOutline("viewer", aliasOutline("owner", dsOutlineNoCaveat()))
		input := query.Outline{
			Type:        query.ArrowIteratorType,
			SubOutlines: []query.Outline{left, dsOutlineNoCaveat()},
		}

		result := applyCollapse(input)

		require.Equal(t, query.ArrowIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		// Left child: collapsed to Alias{RelationName:"owner", AliasedAs:["viewer"]}(DS)
		require.Equal(t, query.AliasIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, "owner", result.SubOutlines[0].Args.RelationName)
		require.Equal(t, []string{"viewer"}, result.SubOutlines[0].Args.AliasedAs)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].SubOutlines[0].Type)
		// Right child: unchanged DS
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("clears outer node ID so canonical key gets recomputed", func(t *testing.T) {
		outer := aliasOutline("viewer",
			aliasOutline("owner", dsOutlineNoCaveat()),
		)
		outer.ID = 100
		outer.SubOutlines[0].ID = 200

		result := applyCollapse(outer)

		// The collapsed node's structure differs from the pre-collapse outline
		// (now has AliasedAs set), so the old canonical key would be stale.
		// The optimizer drops the ID; FillMissingNodeIDs reassigns it after
		// the optimizer pass.
		require.Equal(t, query.OutlineNodeID(0), result.ID)
		require.Equal(t, "owner", result.Args.RelationName)
		require.Equal(t, []string{"viewer"}, result.Args.AliasedAs)
	})

	t.Run("does not collapse alias over non-alias child", func(t *testing.T) {
		// Alias("viewer")(Union[DS, DS]) — child is a union, not an alias
		union := query.Outline{
			Type:        query.UnionIteratorType,
			SubOutlines: []query.Outline{dsOutlineNoCaveat(), dsOutlineNoCaveat()},
		}
		input := aliasOutline("viewer", union)

		result := applyCollapse(input)

		// → unchanged: Alias("viewer")(Union[DS, DS])
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "viewer", result.Args.RelationName)
		require.Equal(t, query.UnionIteratorType, result.SubOutlines[0].Type)
	})
}

func TestAliasChainCollapseViaRegister(t *testing.T) {
	// Verify the optimizer works when invoked through the registered API.
	opt, err := GetOptimization("alias-chain-collapse")
	require.NoError(t, err)

	transform := opt.NewTransform(RequestParams{})

	input := aliasOutline("outer",
		aliasOutline("inner", dsOutlineNoCaveat()),
	)

	result := transform(input)

	require.Equal(t, query.AliasIteratorType, result.Type)
	require.Equal(t, "inner", result.Args.RelationName)
	require.Equal(t, []string{"outer"}, result.Args.AliasedAs)
	require.Len(t, result.SubOutlines, 1)
	require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
}

func hasOptimizer(opts []Optimizer, name string) bool {
	for _, opt := range opts {
		if opt.Name == name {
			return true
		}
	}
	return false
}

func TestOptimizersForRequest(t *testing.T) {
	tests := []struct {
		name            string
		params          RequestParams
		includeCollapse bool
	}{
		{
			name:            "Check with empty subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: ""},
			includeCollapse: true,
		},
		{
			name:            "Check with specific subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: "viewer"},
			includeCollapse: true,
		},
		{
			name:            "Check with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationCheck, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterResources with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterResources with specific subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: "viewer"},
			includeCollapse: false,
		},
		{
			name:            "IterResources with empty subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterResources, SubjectRelation: ""},
			includeCollapse: false,
		},
		{
			name:            "IterSubjects with ellipsis subject relation includes collapse",
			params:          RequestParams{Operation: query.OperationIterSubjects, SubjectRelation: "..."},
			includeCollapse: true,
		},
		{
			name:            "IterSubjects with specific subject relation excludes collapse",
			params:          RequestParams{Operation: query.OperationIterSubjects, SubjectRelation: "owner"},
			includeCollapse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := OptimizersForRequest(tc.params)
			if tc.includeCollapse {
				require.True(t, hasOptimizer(opts, "alias-chain-collapse"))
			} else {
				require.False(t, hasOptimizer(opts, "alias-chain-collapse"))
			}
		})
	}
}

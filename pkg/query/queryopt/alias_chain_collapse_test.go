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

		// → Alias("owner")(DS)
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "owner", result.Args.RelationName)
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

		// → Alias("owner")(DS)
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "owner", result.Args.RelationName)
		require.Len(t, result.SubOutlines, 1)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
	})

	t.Run("leaves single alias unchanged", func(t *testing.T) {
		// Alias("viewer")(DS)
		input := aliasOutline("viewer", dsOutlineNoCaveat())

		result := applyCollapse(input)

		// → Alias("viewer")(DS) — unchanged
		require.Equal(t, query.AliasIteratorType, result.Type)
		require.Equal(t, "viewer", result.Args.RelationName)
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
		// Left branch: collapsed to Alias("owner")(DS)
		require.Equal(t, query.AliasIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, "owner", result.SubOutlines[0].Args.RelationName)
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
		// Left child: collapsed to Alias("owner")(DS)
		require.Equal(t, query.AliasIteratorType, result.SubOutlines[0].Type)
		require.Equal(t, "owner", result.SubOutlines[0].Args.RelationName)
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].SubOutlines[0].Type)
		// Right child: unchanged DS
		require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[1].Type)
	})

	t.Run("preserves outer node ID after collapse", func(t *testing.T) {
		outer := aliasOutline("viewer",
			aliasOutline("owner", dsOutlineNoCaveat()),
		)
		outer.ID = 100
		outer.SubOutlines[0].ID = 200

		result := applyCollapse(outer)

		// The collapsed node should carry the outer node's ID.
		require.Equal(t, query.OutlineNodeID(100), result.ID)
		// And the inner alias's relation name.
		require.Equal(t, "owner", result.Args.RelationName)
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
	require.Len(t, result.SubOutlines, 1)
	require.Equal(t, query.DatastoreIteratorType, result.SubOutlines[0].Type)
}

func TestCheckOptimizationsIncludesAliasChainCollapse(t *testing.T) {
	// Verify that CheckOptimizations includes alias-chain-collapse.
	found := false
	for _, name := range CheckOptimizations {
		if name == "alias-chain-collapse" {
			found = true
			break
		}
	}
	require.True(t, found, "CheckOptimizations should include alias-chain-collapse")
}

func TestLookupResourcesOptimizationsExcludesAliasChainCollapse(t *testing.T) {
	// Verify that LookupResourcesOptimizations does NOT include alias-chain-collapse.
	for _, name := range LookupResourcesOptimizations {
		require.NotEqual(t, "alias-chain-collapse", name,
			"LookupResourcesOptimizations should NOT include alias-chain-collapse")
	}
}

func TestLookupSubjectsOptimizationsExcludesAliasChainCollapse(t *testing.T) {
	// Verify that LookupSubjectsOptimizations does NOT include alias-chain-collapse.
	for _, name := range LookupSubjectsOptimizations {
		require.NotEqual(t, "alias-chain-collapse", name,
			"LookupSubjectsOptimizations should NOT include alias-chain-collapse")
	}
}

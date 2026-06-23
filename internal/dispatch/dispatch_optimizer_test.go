package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
)

func aliasOutline(defName, relName string, child query.Outline) query.Outline {
	return query.Outline{
		Type: query.AliasIteratorType,
		Args: &query.IteratorArgs{
			DefinitionName: defName,
			RelationName:   relName,
		},
		SubOutlines: []query.Outline{child},
	}
}

func recursiveOutline(defName, relName string, child query.Outline) query.Outline {
	return query.Outline{
		Type: query.RecursiveIteratorType,
		Args: &query.IteratorArgs{
			DefinitionName: defName,
			RelationName:   relName,
		},
		SubOutlines: []query.Outline{child},
	}
}

func sentinelOutline(defName, relName string) query.Outline {
	return query.Outline{
		Type: query.RecursiveSentinelIteratorType,
		Args: &query.IteratorArgs{
			DefinitionName: defName,
			RelationName:   relName,
		},
	}
}

func dsOutline() query.Outline {
	rel := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", "", "", false)
	return query.Outline{
		Type: query.DatastoreIteratorType,
		Args: &query.IteratorArgs{Relation: rel},
	}
}

// runDispatchWrap canonicalizes outline then runs only the dispatch-wrap
// optimization on it, returning the resulting root outline.
func runDispatchWrap(t *testing.T, outline query.Outline) query.Outline {
	t.Helper()
	co, err := query.CanonicalizeOutline(outline)
	require.NoError(t, err)
	res, err := ApplyDispatchWrap(co, queryopt.RequestParams{})
	require.NoError(t, err)
	return res.Root
}

func TestDispatchWrapOptimizer(t *testing.T) {
	t.Run("wraps a single alias", func(t *testing.T) {
		input := aliasOutline("document", "viewer", dsOutline())
		got := runDispatchWrap(t, input)

		require.Equal(t, DispatchIteratorType, got.Type)
		require.Len(t, got.SubOutlines, 1)
		require.Equal(t, query.AliasIteratorType, got.SubOutlines[0].Type)
		require.Equal(t, "viewer", got.SubOutlines[0].Args.RelationName)
		// child unchanged
		require.Len(t, got.SubOutlines[0].SubOutlines, 1)
		require.Equal(t, query.DatastoreIteratorType, got.SubOutlines[0].SubOutlines[0].Type)
	})

	t.Run("leaves non-alias outlines alone", func(t *testing.T) {
		input := dsOutline()
		got := runDispatchWrap(t, input)
		require.Equal(t, query.DatastoreIteratorType, got.Type)
	})

	t.Run("wraps nested aliases at every level", func(t *testing.T) {
		// Alias("perm")(Union[ Alias("view")(DS), DS ])
		inner := aliasOutline("document", "view", dsOutline())
		union := query.Outline{
			Type:        query.UnionIteratorType,
			SubOutlines: []query.Outline{inner, dsOutline()},
		}
		input := aliasOutline("document", "perm", union)

		got := runDispatchWrap(t, input)

		// Outer alias is wrapped
		require.Equal(t, DispatchIteratorType, got.Type)
		outerAlias := got.SubOutlines[0]
		require.Equal(t, query.AliasIteratorType, outerAlias.Type)
		require.Equal(t, "perm", outerAlias.Args.RelationName)

		// Inner alias inside the union is also wrapped
		gotUnion := outerAlias.SubOutlines[0]
		require.Equal(t, query.UnionIteratorType, gotUnion.Type)
		require.Len(t, gotUnion.SubOutlines, 2)
		require.Equal(t, DispatchIteratorType, gotUnion.SubOutlines[0].Type)
		require.Equal(t, query.AliasIteratorType, gotUnion.SubOutlines[0].SubOutlines[0].Type)
		require.Equal(t, "view", gotUnion.SubOutlines[0].SubOutlines[0].Args.RelationName)
	})

	t.Run("wraps alias whose subtree has a matched sentinel/iterator pair", func(t *testing.T) {
		// Alias("view")(Recursive("folder","viewer")(Sentinel("folder","viewer")))
		// The recursive iterator inside the alias matches the sentinel's key, so
		// the dispatch boundary is safe.
		input := aliasOutline(
			"document", "view",
			recursiveOutline("folder", "viewer", sentinelOutline("folder", "viewer")),
		)
		got := runDispatchWrap(t, input)
		require.Equal(t, DispatchIteratorType, got.Type, "expected wrap; matched sentinel/iterator pair is dispatch-safe")
	})

	t.Run("does NOT wrap alias whose subtree contains an unmatched sentinel", func(t *testing.T) {
		// Alias("view")(Union[ Sentinel("folder","viewer"), DS ])
		// No RecursiveIterator with key folder#viewer exists in the alias's
		// subtree, so dispatching the alias would sever the sentinel from its
		// collection context.
		input := aliasOutline(
			"document", "view",
			query.Outline{
				Type: query.UnionIteratorType,
				SubOutlines: []query.Outline{
					sentinelOutline("folder", "viewer"),
					dsOutline(),
				},
			},
		)
		got := runDispatchWrap(t, input)
		require.Equal(t, query.AliasIteratorType, got.Type,
			"alias with unmatched sentinel should remain unwrapped to preserve sentinel collection context")
	})

	t.Run("assigns IDs and canonical keys to newly-wrapped nodes", func(t *testing.T) {
		input := aliasOutline("document", "viewer", dsOutline())
		co, err := query.CanonicalizeOutline(input)
		require.NoError(t, err)
		res, err := ApplyDispatchWrap(co, queryopt.RequestParams{})
		require.NoError(t, err)

		require.NotZero(t, res.Root.ID, "Dispatch root must have an ID assigned by FillMissingNodeIDs")
		key, ok := res.CanonicalKeys[res.Root.ID]
		require.True(t, ok, "Dispatch root must have a canonical key recorded")
		require.NotEmpty(t, key)
	})

	t.Run("optimized outline compiles to DispatchIterator above AliasIterator", func(t *testing.T) {
		input := aliasOutline("document", "viewer", dsOutline())
		co, err := query.CanonicalizeOutline(input)
		require.NoError(t, err)
		res, err := ApplyDispatchWrap(co, queryopt.RequestParams{})
		require.NoError(t, err)

		it, err := res.Compile()
		require.NoError(t, err)

		d, ok := it.(*DispatchIterator)
		require.True(t, ok, "expected root to compile to *DispatchIterator, got %T", it)
		subs := d.Subiterators()
		require.Len(t, subs, 1)
		_, ok = subs[0].(*query.AliasIterator)
		require.True(t, ok, "expected DispatchIterator child to be *query.AliasIterator, got %T", subs[0])
	})
}

package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestExclusionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	// Create test relations
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#viewer@user:bob")
	rel3 := tuple.MustParse("document:doc3#viewer@user:charlie")

	t.Run("Basic Exclusion", func(t *testing.T) {
		t.Parallel()
		// Main set: rel1, rel2, rel3
		// Excluded set: rel2
		// Expected result: rel1, rel3
		mainSet := NewFixedIterator(rel1, rel2, rel3)
		excludedSet := NewFixedIterator(rel2)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1", "doc2", "doc3"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should exclude doc2#viewer@user:bob but keep doc1#viewer@user:alice")
		require.Equal(rel1, rels[0])
	})

	t.Run("Empty Main Set", func(t *testing.T) {
		t.Parallel()
		// Main set: empty
		// Excluded set: rel1
		// Expected result: empty
		mainSet := NewFixedIterator()
		excludedSet := NewFixedIterator(rel1)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "Empty main set should result in empty exclusion")
	})

	t.Run("Empty Excluded Set", func(t *testing.T) {
		t.Parallel()
		// Main set: rel1, rel2
		// Excluded set: empty
		// Expected result: rel1, rel2 (nothing to exclude)
		mainSet := NewFixedIterator(rel1, rel2)
		excludedSet := NewFixedIterator()

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1", "doc2"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main set when nothing to exclude")
		require.Equal(rel1, rels[0])
	})

	t.Run("No Overlap", func(t *testing.T) {
		t.Parallel()
		// Create relations with truly different endpoints
		mainRel1 := tuple.MustParse("document:doc1#viewer@user:alice")
		mainRel2 := tuple.MustParse("document:doc2#viewer@user:bob")
		excludeRel1 := tuple.MustParse("document:doc3#viewer@user:charlie")
		excludeRel2 := tuple.MustParse("document:doc4#viewer@user:dave")

		// Main set: doc1:alice, doc2:bob
		// Excluded set: doc3:charlie, doc4:dave
		// Expected result: both main relations (no endpoint overlap)
		mainSet := NewFixedIterator(mainRel1, mainRel2)
		excludedSet := NewFixedIterator(excludeRel1, excludeRel2)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1", "doc2"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main relations matching subject when no endpoint overlap")
		require.Equal(mainRel1, rels[0]) // Only alice matches the subject filter
	})

	t.Run("Complete Exclusion", func(t *testing.T) {
		t.Parallel()
		// Main set: rel1, rel2
		// Excluded set: rel1, rel2, rel3
		// Expected result: empty (all main set relations are excluded)
		mainSet := NewFixedIterator(rel1, rel2)
		excludedSet := NewFixedIterator(rel1, rel2, rel3)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1", "doc2"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "Should return empty when all main relations are excluded")
	})

	t.Run("Partial Exclusion", func(t *testing.T) {
		t.Parallel()
		// Create relations with different relations but same endpoints to test exclusion logic
		relA := tuple.MustParse("document:doc1#viewer@user:alice")
		relB := tuple.MustParse("document:doc2#viewer@user:alice")
		relC := tuple.MustParse("document:doc3#viewer@user:alice")
		relD := tuple.MustParse("document:doc4#viewer@user:alice")

		// Create excluded relations with different relations but same endpoints as relB and relD
		excludeB := tuple.MustParse("document:doc2#editor@user:alice") // Same endpoint as relB
		excludeD := tuple.MustParse("document:doc4#owner@user:alice")  // Same endpoint as relD

		// Main set: relA, relB, relC, relD
		// Excluded set: excludeB (matches doc2:alice), excludeD (matches doc4:alice)
		// Expected result: relA, relC (relB and relD excluded due to matching endpoints)
		mainSet := NewFixedIterator(relA, relB, relC, relD)
		excludedSet := NewFixedIterator(excludeB, excludeD)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1", "doc2", "doc3", "doc4"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 2, "Should exclude relations with endpoints matching excluded set")

		// Check that we got the expected relations
		foundRelA := false
		foundRelC := false
		for _, rel := range rels {
			if rel.Resource.ObjectID == "doc1" {
				foundRelA = true
			}
			if rel.Resource.ObjectID == "doc3" {
				foundRelC = true
			}
		}
		require.True(foundRelA, "Should contain relA (doc1)")
		require.True(foundRelC, "Should contain relC (doc3)")
	})

	t.Run("Clone", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(rel1, rel2)
		excludedSet := NewFixedIterator(rel2)

		original := NewExclusion(mainSet, excludedSet)
		cloned := original.Clone()

		require.NotNil(cloned)
		require.IsType(&Exclusion{}, cloned)

		// Test that cloned exclusion works the same as original
		relSeq, err := cloned.CheckImpl(ctx, []string{"doc1", "doc2"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 1, "Cloned exclusion should work the same as original")
		require.Equal(rel1, rels[0])
	})

	t.Run("Explain", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(rel1)
		excludedSet := NewFixedIterator(rel2)

		exclusion := NewExclusion(mainSet, excludedSet)

		explain := exclusion.Explain()
		require.Equal("Exclusion", explain.Info)
		require.Len(explain.SubExplain, 2, "Should have two sub-explanations")

		explainStr := explain.String()
		require.Contains(explainStr, "Exclusion")
		require.Contains(explainStr, "Fixed")
	})
}

func TestExclusionWithEmptyIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	ctx := &Context{
		Context:   t.Context(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  revision,
	}

	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")

	t.Run("Empty as Main Set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewEmptyFixedIterator()
		excludedSet := NewFixedIterator(rel1)

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "Empty main set should result in empty exclusion")
	})

	t.Run("Empty as Excluded Set", func(t *testing.T) {
		t.Parallel()
		mainSet := NewFixedIterator(rel1)
		excludedSet := NewEmptyFixedIterator()

		exclusion := NewExclusion(mainSet, excludedSet)

		relSeq, err := exclusion.CheckImpl(ctx, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Len(rels, 1, "Should return main set when excluded set is empty")
		require.Equal(rel1, rels[0])
	})
}

package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

func TestArrowIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test iterators using fixed helpers
	// Left side: document parent relationships to folders
	leftRels := NewFolderHierarchyFixedIterator()

	// Right side: folder viewer relationships
	rightRels := NewDocumentAccessFixedIterator()

	arrow := query.NewArrow(leftRels, rightRels)

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		// Test arrow operation: find resources where left side connects to right side
		// This looks for documents whose parent folder has viewers
		relSeq, err := arrow.Check(nil, []string{"spec1", "spec2"}, "alice")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)

		t.Logf("Arrow Check results: %+v", rels)
		// Results depend on the specific relations in our test data
	})

	t.Run("Check_EmptyResources", func(t *testing.T) {
		t.Parallel()

		relSeq, err := arrow.Check(nil, []string{}, "alice")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})

	t.Run("Check_NonexistentResource", func(t *testing.T) {
		t.Parallel()

		relSeq, err := arrow.Check(nil, []string{"nonexistent"}, "alice")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		// Should be empty since resource doesn't exist
		require.Empty(rels, "nonexistent resource should return no results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		relSeq, err := arrow.Check(nil, []string{"spec1"}, "nonexistent")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		// Should be empty since subject doesn't exist
		require.Empty(rels, "nonexistent subject should return no results")
	})

	t.Run("LookupSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		_, err := arrow.LookupSubjects(nil, "spec1")
		require.ErrorIs(err, query.ErrUnimplemented)
	})

	t.Run("LookupResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		_, err := arrow.LookupResources(nil, "alice")
		require.ErrorIs(err, query.ErrUnimplemented)
	})
}

func TestArrowIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test iterators using fixed helpers
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	original := query.NewArrow(leftRels, rightRels)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Test that both iterators produce the same results
	resourceIDs := []string{"spec1"}
	subjectID := "alice"

	// Collect results from original iterator
	originalSeq, err := original.Check(nil, resourceIDs, subjectID)
	require.NoError(err)
	originalResults, err := query.CollectAll(originalSeq)
	require.NoError(err)

	// Collect results from cloned iterator
	clonedSeq, err := cloned.Check(nil, resourceIDs, subjectID)
	require.NoError(err)
	clonedResults, err := query.CollectAll(clonedSeq)
	require.NoError(err)

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
	t.Logf("Original results: %+v", originalResults)
	t.Logf("Cloned results: %+v", clonedResults)
}

func TestArrowIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	arrow := query.NewArrow(leftRels, rightRels)

	explain := arrow.Explain()
	require.Equal("Arrow", explain.Info)
	require.Len(explain.SubExplain, 2, "arrow should have exactly 2 sub-explains (left and right)")

	explainStr := explain.String()
	require.Contains(explainStr, "Arrow")
	require.NotEmpty(explainStr)
	t.Logf("Arrow explain: %s", explainStr)
}

func TestArrowIteratorMultipleResources(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	arrow := query.NewArrow(leftRels, rightRels)

	// Test with multiple resource IDs
	relSeq, err := arrow.Check(nil, []string{"spec1", "spec2", "nonexistent"}, "alice")
	require.NoError(err)

	rels, err := query.CollectAll(relSeq)
	require.NoError(err)

	t.Logf("Multiple resources results: %+v", rels)
	// The result should include all valid arrow relationships found across all resources
}

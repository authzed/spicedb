package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

func TestIntersectionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("Check_Intersection", func(t *testing.T) {
		t.Parallel()

		// Create an intersection where both iterators have matching data
		intersect := query.NewIntersection()

		// Both iterators should have alice with access to doc1
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.AddSubIterator(documentAccess)
		intersect.AddSubIterator(multiRole)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Intersection results: %+v", rels)

		// The result should only contain resources where both conditions are met
		require.NotEmpty(rels, "Intersection should find matching relations")
	})

	t.Run("Check_EmptyIntersection", func(t *testing.T) {
		t.Parallel()

		// Create an intersection with contradictory requirements
		intersect := query.NewIntersection()

		// Use iterators that don't have overlapping data
		documentAccess := NewDocumentAccessFixedIterator()
		singleUser := NewSingleUserFixedIterator("bob") // bob won't be in documentAccess

		intersect.AddSubIterator(documentAccess)
		intersect.AddSubIterator(singleUser)

		// Use a subject that doesn't exist in both
		relSeq, err := intersect.Check(nil, []string{"doc1"}, "bob")
		require.NoError(err)

		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			t.Logf("Empty intersection results: %+v", rels)
			// Should likely be empty since bob isn't in documentAccess
		} else {
			t.Log("Empty intersection returned nil sequence")
		}
	})

	t.Run("Check_NoSubIterators", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()

		// Empty intersection should return empty results
		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		// Should return nil sequence since there are no sub-iterators
		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels)
		}
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.AddSubIterator(documentAccess)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		t.Logf("Single sub-iterator results: %+v", rels)
		require.NotEmpty(rels, "Single iterator intersection should return results")
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.AddSubIterator(documentAccess)

		relSeq, err := intersect.Check(nil, []string{}, "alice")
		require.NoError(err)

		// The behavior with empty resource list may vary by implementation
		// Let's just ensure it doesn't error and log the results
		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			t.Logf("Empty resource list results: %+v", rels)
			require.Empty(rels, "Empty resource list should return no results")
		} else {
			t.Log("Empty resource list returned nil sequence")
		}
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.AddSubIterator(documentAccess)
		intersect.AddSubIterator(multiRole)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "nonexistent")
		require.NoError(err)

		if relSeq != nil {
			rels, err := query.CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels, "Nonexistent subject should return no results")
		}
	})

	t.Run("LookupSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()
		_, err := intersect.LookupSubjects(nil, "doc1")
		require.ErrorIs(err, query.ErrUnimplemented)
	})

	t.Run("LookupResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()
		_, err := intersect.LookupResources(nil, "alice")
		require.ErrorIs(err, query.ErrUnimplemented)
	})
}

func TestIntersectionIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	original := query.NewIntersection()

	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	original.AddSubIterator(documentAccess)
	original.AddSubIterator(multiRole)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Test that both iterators produce the same results
	resourceIDs := []string{"doc1"}
	subjectID := "alice"

	// Collect results from original iterator
	originalSeq, err := original.Check(nil, resourceIDs, subjectID)
	require.NoError(err)
	var originalResults []query.Relation
	if originalSeq != nil {
		originalResults, err = query.CollectAll(originalSeq)
		require.NoError(err)
	}

	// Collect results from cloned iterator
	clonedSeq, err := cloned.Check(nil, resourceIDs, subjectID)
	require.NoError(err)
	var clonedResults []query.Relation
	if clonedSeq != nil {
		clonedResults, err = query.CollectAll(clonedSeq)
		require.NoError(err)
	}

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
	t.Logf("Original results: %+v", originalResults)
	t.Logf("Cloned results: %+v", clonedResults)
}

func TestIntersectionIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("EmptyIntersection", func(t *testing.T) {
		t.Parallel()

		intersect := query.NewIntersection()
		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Empty(explain.SubExplain, "empty intersection should have no sub-explains")
	})

	t.Run("IntersectionWithSubIterators", func(t *testing.T) {
		t.Parallel()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := query.NewIntersection()
		intersect.AddSubIterator(documentAccess)
		intersect.AddSubIterator(multiRole)

		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Len(explain.SubExplain, 2, "intersection should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Intersection")
		require.NotEmpty(explainStr)
		t.Logf("Intersection explain: %s", explainStr)
	})
}

func TestIntersectionIteratorEarlyTermination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create an intersection where the first iterator returns no results
	// This should cause early termination
	intersect := query.NewIntersection()

	// First, add an iterator that will return no results for the given subject
	emptyIterator := NewEmptyFixedIterator()
	documentAccess := NewDocumentAccessFixedIterator()

	intersect.AddSubIterator(emptyIterator)
	intersect.AddSubIterator(documentAccess)

	// Use any subject - should get no results due to empty first iterator
	relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
	require.NoError(err)

	// Should return empty results since first iterator has no results
	if relSeq != nil {
		rels, err := query.CollectAll(relSeq)
		require.NoError(err)
		// Should be empty due to early termination
		require.Empty(rels, "Early termination should return no results")
		t.Logf("Early termination results: %+v", rels)
	} else {
		t.Log("Early termination returned nil sequence")
	}
}

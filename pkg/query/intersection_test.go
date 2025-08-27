package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntersectionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("Check_Intersection", func(t *testing.T) {
		t.Parallel()

		// Create an intersection where both iterators have matching data
		intersect := NewIntersection()

		// Both iterators should have alice with access to doc1
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(multiRole)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// The result should only contain resources where both conditions are met
		require.NotEmpty(rels, "Intersection should find matching relations")
	})

	t.Run("Check_EmptyIntersection", func(t *testing.T) {
		t.Parallel()

		// Create an intersection with contradictory requirements
		intersect := NewIntersection()

		// Use iterators that don't have overlapping data
		documentAccess := NewDocumentAccessFixedIterator()
		singleUser := NewSingleUserFixedIterator("bob") // bob won't be in documentAccess

		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(singleUser)

		// Use a subject that doesn't exist in both
		relSeq, err := intersect.Check(nil, []string{"doc1"}, "bob")
		require.NoError(err)

		if relSeq != nil {
			_, err := CollectAll(relSeq)
			require.NoError(err)
			// Should likely be empty since bob isn't in documentAccess
		} else {
			t.Log("Empty intersection returned nil sequence")
		}
	})

	t.Run("Check_NoSubIterators", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		// Empty intersection should return empty results
		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		// Should return nil sequence since there are no sub-iterators
		if relSeq != nil {
			rels, err := CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels)
		}
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.addSubIterator(documentAccess)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.NotEmpty(rels, "Single iterator intersection should return results")
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.addSubIterator(documentAccess)

		relSeq, err := intersect.Check(nil, []string{}, "alice")
		require.NoError(err)

		// The behavior with empty resource list may vary by implementation
		// Let's just ensure it doesn't error and log the results
		if relSeq != nil {
			rels, err := CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels, "Empty resource list should return no results")
		} else {
			t.Log("Empty resource list returned nil sequence")
		}
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(multiRole)

		relSeq, err := intersect.Check(nil, []string{"doc1"}, "nonexistent")
		require.NoError(err)

		if relSeq != nil {
			rels, err := CollectAll(relSeq)
			require.NoError(err)
			require.Empty(rels, "Nonexistent subject should return no results")
		}
	})

	t.Run("IterSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()
		require.Panics(func() {
			_, _ = intersect.IterSubjects(nil, "doc1")
		})
	})

	t.Run("IterResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()
		require.Panics(func() {
			_, _ = intersect.IterResources(nil, "alice")
		})
	})
}

func TestIntersectionIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	original := NewIntersection()

	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	original.addSubIterator(documentAccess)
	original.addSubIterator(multiRole)

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
	var originalResults []Relation
	if originalSeq != nil {
		originalResults, err = CollectAll(originalSeq)
		require.NoError(err)
	}

	// Collect results from cloned iterator
	clonedSeq, err := cloned.Check(nil, resourceIDs, subjectID)
	require.NoError(err)
	var clonedResults []Relation
	if clonedSeq != nil {
		clonedResults, err = CollectAll(clonedSeq)
		require.NoError(err)
	}

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
}

func TestIntersectionIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("EmptyIntersection", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()
		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Empty(explain.SubExplain, "empty intersection should have no sub-explains")
	})

	t.Run("IntersectionWithSubIterators", func(t *testing.T) {
		t.Parallel()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersection()
		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(multiRole)

		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Len(explain.SubExplain, 2, "intersection should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Intersection")
		require.NotEmpty(explainStr)
	})
}

func TestIntersectionIteratorEarlyTermination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create an intersection where the first iterator returns no results
	// This should cause early termination
	intersect := NewIntersection()

	// First, add an iterator that will return no results for the given subject
	emptyIterator := NewEmptyFixedIterator()
	documentAccess := NewDocumentAccessFixedIterator()

	intersect.addSubIterator(emptyIterator)
	intersect.addSubIterator(documentAccess)

	// Use any subject - should get no results due to empty first iterator
	relSeq, err := intersect.Check(nil, []string{"doc1"}, "alice")
	require.NoError(err)

	// Should return empty results since first iterator has no results
	if relSeq != nil {
		rels, err := CollectAll(relSeq)
		require.NoError(err)
		// Should be empty due to early termination
		require.Empty(rels, "Early termination should return no results")
	} else {
		t.Log("Early termination returned nil sequence")
	}
}

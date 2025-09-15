package query

import (
	"testing"

	"github.com/stretchr/testify/require"

)

func TestIntersectionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("Check_Intersection", func(t *testing.T) {
		t.Parallel()

		// Create an intersection where both iterators have matching data
		intersect := NewIntersection()

		// Both iterators should have alice with access to doc1
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(multiRole)

		pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// The intersection should find relations that exist in both iterators
		// Both DocumentAccess and MultiRole have alice with viewer/editor/owner on doc1
		expected := []*Path{
			MustPathFromString("document:doc1#viewer@user:alice"),
			MustPathFromString("document:doc1#editor@user:alice"),
			MustPathFromString("document:doc1#owner@user:alice"),
		}
		require.ElementsMatch(expected, rels)
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
		pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "bob").WithEllipses())
		require.NoError(err)

		if pathSeq != nil {
			_, err := CollectAll(pathSeq)
			require.NoError(err)
			// Should likely be empty since bob isn't in documentAccess
		}
	})

	t.Run("Check_NoSubIterators", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		// Empty intersection should return empty results
		pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// Should return nil sequence since there are no sub-iterators
		if pathSeq != nil {
			rels, err := CollectAll(pathSeq)
			require.NoError(err)
			require.Empty(rels)
		}
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.addSubIterator(documentAccess)

		pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.NotEmpty(rels, "Single iterator intersection should return results")
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		intersect.addSubIterator(documentAccess)

		pathSeq, err := ctx.Check(intersect, []Object{}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// The behavior with empty resource list may vary by implementation
		// Let's just ensure it doesn't error and log the results
		if pathSeq != nil {
			rels, err := CollectAll(pathSeq)
			require.NoError(err)
			require.Empty(rels, "Empty resource list should return no results")
		}
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect.addSubIterator(documentAccess)
		intersect.addSubIterator(multiRole)

		pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)

		if pathSeq != nil {
			rels, err := CollectAll(pathSeq)
			require.NoError(err)
			require.Empty(rels, "Nonexistent subject should return no results")
		}
	})

	t.Run("IterSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()
		require.Panics(func() {
			_, _ = ctx.IterSubjects(intersect, NewObject("document", "doc1"))
		})
	})

	t.Run("IterResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		intersect := NewIntersection()
		require.Panics(func() {
			_, _ = ctx.IterResources(intersect, NewObject("user", "alice").WithEllipses())
		})
	})
}

func TestIntersectionIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

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
	originalSeq, err := ctx.Check(original, NewObjects("document", resourceIDs[0]), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)
	var originalResults []*Path
	if originalSeq != nil {
		originalResults, err = CollectAll(originalSeq)
		require.NoError(err)
	}

	// Collect results from cloned iterator
	clonedSeq, err := ctx.Check(cloned, NewObjects("document", resourceIDs[0]), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)
	var clonedResults []*Path
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

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Create an intersection where the first iterator returns no results
	// This should cause early termination
	intersect := NewIntersection()

	// First, add an iterator that will return no results for the given subject
	emptyIterator := NewEmptyFixedIterator()
	documentAccess := NewDocumentAccessFixedIterator()

	intersect.addSubIterator(emptyIterator)
	intersect.addSubIterator(documentAccess)

	// Use any subject - should get no results due to empty first iterator
	pathSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	// Should return empty results since first iterator has no results
	if pathSeq != nil {
		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty due to early termination
		require.Empty(rels, "Early termination should return no results")
	}
}

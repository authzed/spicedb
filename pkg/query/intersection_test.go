package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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

		// The intersection should merge relations that exist in both iterators for the same endpoint
		// Both DocumentAccess and MultiRole have alice with viewer/editor/owner on doc1
		// These get merged into a single path with no specific relation (since relations differ)
		require.Len(rels, 1, "Should return one merged path for the endpoint")

		// Verify the combined path has the correct endpoint
		path := rels[0]
		require.True(path.Resource.Equals(NewObject("document", "doc1")), "Resource should match")
		require.True(tuple.ONREqual(path.Subject, NewObject("user", "alice").WithEllipses()), "Subject should match")
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
			rels, err := CollectAll(pathSeq)
			require.NoError(err)
			require.Empty(rels)
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
	var originalResults []Path
	if originalSeq != nil {
		originalResults, err = CollectAll(originalSeq)
		require.NoError(err)
	}

	// Collect results from cloned iterator
	clonedSeq, err := ctx.Check(cloned, NewObjects("document", resourceIDs[0]), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)
	var clonedResults []Path
	if clonedSeq != nil {
		clonedResults, err = CollectAll(clonedSeq)
		require.NoError(err)
	}

	// Both iterators should produce identical results (order may vary)
	require.Len(clonedResults, len(originalResults), "cloned iterator should produce same number of results")
	for _, expectedPath := range originalResults {
		found := false
		for _, actualPath := range clonedResults {
			if expectedPath.Equals(actualPath) {
				found = true
				break
			}
		}
		require.True(found, "Expected path %v should be found in cloned results", expectedPath)
	}
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

func TestIntersectionIteratorCaveatCombination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// Both iterators return the same path but with different caveats
		// Should combine with AND logic
		pathWithCaveat1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathWithCaveat1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathWithCaveat2 := MustPathFromString("document:doc1#viewer@user:alice")
		pathWithCaveat2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(pathWithCaveat1)
		iter2 := NewFixedIterator(pathWithCaveat2)

		intersect := NewIntersection()
		intersect.addSubIterator(iter1)
		intersect.addSubIterator(iter2)

		relSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Intersection should return one combined relation")
		relCaveat := rels[0].Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(relCaveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children")
	})

	t.Run("OneCaveat_One_NoCaveat_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// One path has caveat, one doesn't - caveat should be preserved (AND logic)
		pathWithCaveat := MustPathFromString("document:doc1#viewer@user:alice")
		pathWithCaveat.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
				},
			},
		}

		pathNoCaveat := MustPathFromString("document:doc1#viewer@user:alice")

		iter1 := NewFixedIterator(pathWithCaveat)
		iter2 := NewFixedIterator(pathNoCaveat)

		intersect := NewIntersection()
		intersect.addSubIterator(iter1)
		intersect.addSubIterator(iter2)

		relSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Intersection should return one relation")
		require.NotNil(rels[0].Caveat, "Caveat should be preserved in AND logic")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Different_Relations_Same_Endpoint", func(t *testing.T) {
		t.Parallel()

		// Two iterators with different relations on same endpoint - should merge into one path
		pathViewer := MustPathFromString("document:doc1#viewer@user:alice")
		pathViewer.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathEditor := MustPathFromString("document:doc1#editor@user:alice")
		pathEditor.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		// Create iterators that return both paths
		iter1 := NewFixedIterator(pathViewer, pathEditor)
		iter2 := NewFixedIterator(pathViewer, pathEditor)

		intersect := NewIntersection()
		intersect.addSubIterator(iter1)
		intersect.addSubIterator(iter2)

		relSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Should return one merged path since intersection works on endpoints, not individual relations")

		// The merged path should combine both relations and caveats
		path := rels[0]
		require.NotNil(path.Caveat, "Merged path should have combined caveat")
		require.Equal("document", path.Resource.ObjectType)
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("user", path.Subject.ObjectType)
		require.Equal("alice", path.Subject.ObjectID)
		// The relation should be empty since different relations were merged (per Path.mergeFrom logic)
		require.Equal("", path.Relation, "Different relations should result in empty relation")
	})

	t.Run("No_Common_Endpoints", func(t *testing.T) {
		t.Parallel()

		// Iterators with no common endpoints - should return empty
		pathDoc1Alice := MustPathFromString("document:doc1#viewer@user:alice")
		pathDoc1Alice.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathDoc2Bob := MustPathFromString("document:doc2#editor@user:bob")
		pathDoc2Bob.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(pathDoc1Alice)
		iter2 := NewFixedIterator(pathDoc2Bob)

		intersect := NewIntersection()
		intersect.addSubIterator(iter1)
		intersect.addSubIterator(iter2)

		relSeq, err := ctx.Check(intersect, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 0, "Different endpoints should not intersect - should return empty")
	})

	t.Run("Three_Iterators_Mixed_Caveats", func(t *testing.T) {
		t.Parallel()

		// Three iterators with the same path but different caveat combinations
		pathCaveat1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathCaveat1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathNoCaveat := MustPathFromString("document:doc1#viewer@user:alice")

		pathCaveat2 := MustPathFromString("document:doc1#viewer@user:alice")
		pathCaveat2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(pathCaveat1)
		iter2 := NewFixedIterator(pathNoCaveat)
		iter3 := NewFixedIterator(pathCaveat2)

		intersect := NewIntersection()
		intersect.addSubIterator(iter1)
		intersect.addSubIterator(iter2)
		intersect.addSubIterator(iter3)

		relSeq, err := ctx.Check(intersect, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Should return one intersected path")
		relCaveat := rels[0].Caveat
		require.NotNil(relCaveat, "Final result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(relCaveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children (caveat1 and caveat2)")
	})
}

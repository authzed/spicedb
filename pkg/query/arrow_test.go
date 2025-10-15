package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestArrowIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test iterators using fixed helpers
	// Left side: document parent relationships to folders
	leftRels := NewFolderHierarchyFixedIterator()

	// Right side: folder viewer relationships (same as left for folder access)
	rightRels := NewFolderHierarchyFixedIterator()

	arrow := NewArrow(leftRels, rightRels)

	t.Run("Check", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test arrow operation: find resources where left side connects to right side
		// This looks for documents whose parent folder has viewers
		pathSeq, err := ctx.Check(arrow, NewObjects("document", "spec1", "spec2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Expected: spec1 should match because alice has viewer access to project1 (spec1's parent)
		// spec2 should NOT match because alice does not have access to project2 (spec2's parent)
		expected := []Path{
			MustPathFromString("document:spec1#parent@user:alice"),
		}
		require.Equal(expected, rels)
	})

	t.Run("Check_EmptyResources", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		pathSeq, err := ctx.Check(arrow, []Object{}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})

	t.Run("Check_NonexistentResource", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		pathSeq, err := ctx.Check(arrow, NewObjects("document", "nonexistent"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty since resource doesn't exist
		require.Empty(rels, "nonexistent resource should return no results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		pathSeq, err := ctx.Check(arrow, NewObjects("document", "spec1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty since subject doesn't exist
		require.Empty(rels, "nonexistent subject should return no results")
	})

	t.Run("IterSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		require.Panics(func() {
			_, _ = ctx.IterSubjects(arrow, NewObject("document", "spec1"))
		})
	})

	t.Run("IterResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		require.Panics(func() {
			_, _ = ctx.IterResources(arrow, NewObject("user", "alice").WithEllipses())
		})
	})
}

func TestArrowIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test iterators using fixed helpers
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	original := NewArrow(leftRels, rightRels)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Create context with LocalExecutor
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Test that both iterators produce the same results
	resourceIDs := []string{"spec1"}
	subjectID := "alice"

	// Collect results from original iterator
	originalSeq, err := ctx.Check(original, NewObjects("document", resourceIDs[0]), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)
	originalResults, err := CollectAll(originalSeq)
	require.NoError(err)

	// Collect results from cloned iterator
	clonedSeq, err := ctx.Check(cloned, NewObjects("document", resourceIDs[0]), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)
	clonedResults, err := CollectAll(clonedSeq)
	require.NoError(err)

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
}

func TestArrowIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	arrow := NewArrow(leftRels, rightRels)

	explain := arrow.Explain()
	require.Equal("Arrow", explain.Info)
	require.Len(explain.SubExplain, 2, "arrow should have exactly 2 sub-explains (left and right)")

	explainStr := explain.String()
	require.Contains(explainStr, "Arrow")
	require.NotEmpty(explainStr)
}

func TestArrowIteratorMultipleResources(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	leftRels := NewFolderHierarchyFixedIterator()  // Documents -> Folders
	rightRels := NewFolderHierarchyFixedIterator() // Folders -> Users (alice has access to project1)

	arrow := NewArrow(leftRels, rightRels)

	// Create context with LocalExecutor
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Test with multiple resource IDs
	pathSeq, err := ctx.Check(arrow, NewObjects("document", "spec1", "spec2", "nonexistent"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	rels, err := CollectAll(pathSeq)
	require.NoError(err)

	// The result should include valid arrow relationships found across all resources
	// Arrow operation: for each document, find its parent folder, then check if alice has access to that folder
	// Based on the test data:
	// - spec1 parent folder:project1, alice has viewer access to project1 -> should match
	// - spec2 parent folder:project2, alice does NOT have access to project2 -> should NOT match
	// - nonexistent doesn't exist -> should NOT match

	// We expect exactly 1 result: spec1 with alice as subject
	expected := []Path{
		MustPathFromString("document:spec1#parent@user:alice"),
	}
	require.Equal(expected, rels)
}

func TestArrowIteratorCaveatCombination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// Left side path with caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		// Right side path with different caveat (matching the left side's target)
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Arrow should return one combined relation")
		relCaveat := rels[0].Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(relCaveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children")
	})

	t.Run("LeftCaveat_Right_NoCaveat", func(t *testing.T) {
		t.Parallel()

		// Left side path with caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		// Right side path with no caveat
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Arrow should return one relation")
		require.NotNil(rels[0].Caveat, "Left caveat should be preserved")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Left_NoCaveat_Right_Caveat", func(t *testing.T) {
		t.Parallel()

		// Left side path with no caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")

		// Right side path with caveat
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Arrow should return one relation")
		require.NotNil(rels[0].Caveat, "Right caveat should be preserved")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Neither_Side_Has_Caveat", func(t *testing.T) {
		t.Parallel()

		// Left side path with no caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")

		// Right side path with no caveat
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Arrow should return one relation")
		require.Nil(rels[0].Caveat, "No caveat should result in no caveat")
	})

	t.Run("Multiple_Relations_Mixed_Caveats", func(t *testing.T) {
		t.Parallel()

		// Left side has multiple paths, some with caveats
		leftPath1 := MustPathFromString("document:doc1#parent@folder:folder1")
		leftPath1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat1",
				},
			},
		}

		leftPath2 := MustPathFromString("document:doc2#parent@folder:folder2")

		// Right side paths with mixed caveats
		rightPath1 := MustPathFromString("folder:folder1#viewer@user:alice")
		rightPath1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat1",
				},
			},
		}

		rightPath2 := MustPathFromString("folder:folder2#viewer@user:alice")
		rightPath2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat2",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath1, leftPath2)
		rightIter := NewFixedIterator(rightPath1, rightPath2)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 2, "Arrow should return two relations")

		// Check that caveats are combined properly
		for _, path := range rels {
			require.NotNil(path.Caveat, "Each result should have a caveat")
			// Note: Exact caveat validation would require parsing CaveatExpression
			// Just verify that caveats are present for combined results
		}
	})

	t.Run("No_Matching_Arrow_Relations", func(t *testing.T) {
		t.Parallel()

		// Left side points to folder1, but right side only has folder2
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		rightPath := MustPathFromString("folder:folder2#viewer@user:alice") // Different folder
		rightPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Empty(rels, "No matching arrow relations should result in empty result")
	})
}

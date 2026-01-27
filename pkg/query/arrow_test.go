package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// testArrowBothDirections runs the same test with both arrow directions
func testArrowBothDirections(t *testing.T, name string, testFn func(t *testing.T, direction arrowDirection)) {
	t.Run(name+"_LTR", func(t *testing.T) {
		t.Parallel()
		testFn(t, leftToRight)
	})

	t.Run(name+"_RTL", func(t *testing.T) {
		t.Parallel()
		testFn(t, rightToLeft)
	})
}

func TestArrowIterator(t *testing.T) {
	t.Parallel()

	// Create test iterators using fixed helpers
	// Left side: document parent relationships to folders
	leftRels := NewFolderHierarchyFixedIterator()

	// Right side: folder viewer relationships (same as left for folder access)
	rightRels := NewFolderHierarchyFixedIterator()

	arrow := NewArrow(leftRels, rightRels)

	t.Run("Check", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context())

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
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context())

		pathSeq, err := ctx.Check(arrow, []Object{}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})

	t.Run("Check_NonexistentResource", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context())

		pathSeq, err := ctx.Check(arrow, NewObjects("document", "nonexistent"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty since resource doesn't exist
		require.Empty(rels, "nonexistent resource should return no results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context())

		pathSeq, err := ctx.Check(arrow, NewObjects("document", "spec1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty since subject doesn't exist
		require.Empty(rels, "nonexistent subject should return no results")
	})

	t.Run("IterSubjects", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context())

		// Test arrow IterSubjects: find all subjects for a resource through the arrow
		// This finds subjects who have access to a resource via the left->right path
		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "spec1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Expected: spec1 parent is project1, and alice has viewer access to project1
		// So alice should be returned as a subject
		require.NotEmpty(paths, "Should find subjects through arrow")

		// Verify alice is in the results
		foundAlice := false
		for _, path := range paths {
			if path.Subject.ObjectID == "alice" {
				foundAlice = true
				break
			}
		}
		require.True(foundAlice, "Should find alice as a subject")
	})

	t.Run("IterResources", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		logger := NewTraceLogger()

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context(), WithTraceLogger(logger))

		// Test arrow IterSubjects: find all resources for a subject through the arrow
		// This finds which resources a given subject has access to via the left->right path
		pathSeq, err := ctx.IterResources(arrow, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Expected: spec1 parent is project1, and alice has viewer access to project1
		// So spec1 should be returned as a resource
		require.NotEmpty(paths, "Should find resources through arrow")

		// Verify spec1 is in the results
		resourceIds := make([]string, 0, len(paths))
		for _, path := range paths {
			resourceIds = append(resourceIds, path.Resource.ObjectID)
		}
		require.Contains(resourceIds, "spec1", "Should find spec1 as a resource")
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
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// Create context with LocalExecutor
	ctx := NewLocalContext(t.Context())

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
	require.Equal("Arrow(LTR)", explain.Info)
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
	ctx := NewLocalContext(t.Context())

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
	ctx := NewLocalContext(t.Context())

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
		require.Equal(core.CaveatOperation_AND, relCaveat.GetOperation().Op, "Caveat should be an AND")
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

func TestArrowIterSubjects(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("SimpleArrow", func(t *testing.T) {
		t.Parallel()

		// Left: doc1 -> folder1
		// Right: folder1 -> alice
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find one subject through arrow")
		require.Equal("alice", paths[0].Subject.ObjectID)
		require.Equal("doc1", paths[0].Resource.ObjectID)
	})

	t.Run("MultipleSubjects", func(t *testing.T) {
		t.Parallel()

		// Left: doc1 -> folder1
		// Right: folder1 -> alice, folder1 -> bob
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		rightPath1 := MustPathFromString("folder:folder1#viewer@user:alice")
		rightPath2 := MustPathFromString("folder:folder1#viewer@user:bob")

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath1, rightPath2)

		arrow := NewArrow(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 2, "Should find two subjects through arrow")

		subjectIDs := make(map[string]bool)
		for _, path := range paths {
			subjectIDs[path.Subject.ObjectID] = true
		}
		require.Contains(subjectIDs, "alice")
		require.Contains(subjectIDs, "bob")
	})

	t.Run("NoLeftPaths", func(t *testing.T) {
		t.Parallel()

		// Empty left side
		leftIter := NewFixedIterator()
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "No left paths should result in no subjects")
	})

	t.Run("NoRightPaths", func(t *testing.T) {
		t.Parallel()

		// Left exists but right is empty
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator()

		arrow := NewArrow(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "No right paths should result in no subjects")
	})

	t.Run("CaveatCombination", func(t *testing.T) {
		t.Parallel()

		// Left with caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		// Right with caveat
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

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"))
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find one subject")
		require.NotNil(paths[0].Caveat, "Should have combined caveat")
		require.NotNil(paths[0].Caveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, paths[0].Caveat.GetOperation().Op, "Caveat should be AND")
	})
}

func TestArrowIteratorBidirectional(t *testing.T) {
	t.Parallel()

	testArrowBothDirections(t, "BasicCheck", func(t *testing.T, direction arrowDirection) {
		require := require.New(t)

		// Left side: document parent relationships to folders
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(leftPath)

		// Right side: folder viewer relationships
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)
		arrow.direction = direction // Set the direction explicitly

		// Create context
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test arrow operation
		pathSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Both directions should find the same relationship
		require.Len(rels, 1, "Arrow should return one relation")
		require.Equal("doc1", rels[0].Resource.ObjectID)
		require.Equal("alice", rels[0].Subject.ObjectID)
	})

	testArrowBothDirections(t, "NoMatch", func(t *testing.T, direction arrowDirection) {
		require := require.New(t)

		// Left side points to folder1
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(leftPath)

		// Right side only has folder2 (no match)
		rightPath := MustPathFromString("folder:folder2#viewer@user:alice")
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)
		arrow.direction = direction

		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		pathSeq, err := ctx.Check(arrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(pathSeq)
		require.NoError(err)

		// Both directions should return empty when no match
		require.Empty(rels, "No matching arrow relations should result in empty result")
	})
}

func TestArrow_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create left and right iterators
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(leftPath)

		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		resourceType, err := arrow.ResourceType()
		require.NoError(err)
		require.Equal("document", resourceType.Type) // From left iterator
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create left and right iterators
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(leftPath)

		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(rightPath)

		arrow := NewArrow(leftIter, rightIter)

		subjectTypes, err := arrow.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1) // From right iterator
		require.Equal("user", subjectTypes[0].Type)
	})
}

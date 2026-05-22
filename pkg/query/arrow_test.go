package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestNewSchemaArrow(t *testing.T) {
	left := NewEmptyFixedIterator()
	right := NewEmptyFixedIterator()

	arrow := NewSchemaArrow(left, right)
	require.NotNil(t, arrow)
	require.Same(t, left, arrow.left)
	require.Same(t, right, arrow.right)
	require.Equal(t, leftToRight, arrow.direction)
	require.True(t, arrow.isSchemaArrow, "NewSchemaArrow should mark isSchemaArrow=true")

	// NewArrowIterator should, by contrast, mark isSchemaArrow=false.
	subrelArrow := NewArrowIterator(left, right)
	require.False(t, subrelArrow.isSchemaArrow)
}

// testArrowBothDirections runs the same test with both arrow directions
func testArrowBothDirections(t *testing.T, name string, testFn func(t *testing.T, direction arrowDirection)) {
	t.Run(name+"_LTR", func(t *testing.T) {
		testFn(t, leftToRight)
	})

	t.Run(name+"_RTL", func(t *testing.T) {
		testFn(t, rightToLeft)
	})
}

func TestArrowIterator(t *testing.T) {
	// Create test iterators using fixed helpers
	// Left side: document parent relationships to folders
	leftRels := NewFolderHierarchyFixedIterator()

	// Right side: folder viewer relationships (same as left for folder access)
	rightRels := NewFolderHierarchyFixedIterator()

	arrow := NewArrowIterator(leftRels, rightRels)

	t.Run("Check_spec1_alice", func(t *testing.T) {
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewTestContext(t)

		// spec1's parent is project1, alice has viewer access to project1 -> should match
		path, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "spec1 should match because alice has viewer access to project1")
		require.Equal("spec1", path.Resource.ObjectID)
		require.Equal("alice", path.Subject.ObjectID)
	})

	t.Run("Check_spec2_alice_no_match", func(t *testing.T) {
		require := require.New(t)

		ctx := NewTestContext(t)

		// spec2's parent is project2, alice does NOT have access to project2 -> no match
		path, err := ctx.Check(arrow, NewObject("document", "spec2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path, "spec2 should not match because alice does not have access to project2")
	})

	t.Run("Check_NonexistentResource", func(t *testing.T) {
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewTestContext(t)

		path, err := ctx.Check(arrow, NewObject("document", "nonexistent"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path, "nonexistent resource should return no results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewTestContext(t)

		path, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)
		require.Nil(path, "nonexistent subject should return no results")
	})

	t.Run("IterSubjects", func(t *testing.T) {
		require := require.New(t)

		// Create context with LocalExecutor
		ctx := NewTestContext(t)

		// Test arrow IterSubjects: find all subjects for a resource through the arrow
		// This finds subjects who have access to a resource via the left->right path
		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "spec1"), NoObjectFilter())
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
		require := require.New(t)

		logger := NewTraceLogger()

		// Create context with LocalExecutor
		ctx := NewLocalContext(t.Context(), WithTraceLogger(logger))

		// Test arrow IterSubjects: find all resources for a subject through the arrow
		// This finds which resources a given subject has access to via the left->right path
		pathSeq, err := ctx.IterResources(arrow, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
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
	require := require.New(t)

	// Create test iterators using fixed helpers
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	original := NewArrowIterator(leftRels, rightRels)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// Create context with LocalExecutor
	ctx := NewTestContext(t)

	// Test that both iterators produce the same results
	subjectID := "alice"

	// Collect result from original iterator
	originalPath, err := ctx.Check(original, NewObject("document", "spec1"), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)

	// Collect result from cloned iterator
	clonedPath, err := ctx.Check(cloned, NewObject("document", "spec1"), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)

	// Both iterators should produce identical results (both nil or both non-nil)
	require.Equal(originalPath == nil, clonedPath == nil, "original and cloned iterators should agree on nil-ness")
}

func TestArrowIteratorExplain(t *testing.T) {
	require := require.New(t)
	leftRels := NewFolderHierarchyFixedIterator()
	rightRels := NewDocumentAccessFixedIterator()
	arrow := NewArrowIterator(leftRels, rightRels)

	explain := arrow.Explain()
	require.Equal("Arrow(LTR)", explain.Info)
	require.Len(explain.SubExplain, 2, "arrow should have exactly 2 sub-explains (left and right)")

	explainStr := explain.String()
	require.Contains(explainStr, "Arrow")
	require.NotEmpty(explainStr)
}

func TestArrowIteratorMultipleResources(t *testing.T) {
	require := require.New(t)

	leftRels := NewFolderHierarchyFixedIterator()  // Documents -> Folders
	rightRels := NewFolderHierarchyFixedIterator() // Folders -> Users (alice has access to project1)

	arrow := NewArrowIterator(leftRels, rightRels)

	// Create context with LocalExecutor
	ctx := NewTestContext(t)

	// Test with multiple resource IDs (now via separate Check calls)
	// Arrow operation: for each document, find its parent folder, then check if alice has access to that folder
	// Based on the test data:
	// - spec1 parent folder:project1, alice has viewer access to project1 -> should match
	// - spec2 parent folder:project2, alice does NOT have access to project2 -> should NOT match
	// - nonexistent doesn't exist -> should NOT match

	path1, err := ctx.Check(arrow, NewObject("document", "spec1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	require.NotNil(path1, "spec1 should match")

	path2, err := ctx.Check(arrow, NewObject("document", "spec2"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	require.Nil(path2, "spec2 should not match")

	path3, err := ctx.Check(arrow, NewObject("document", "nonexistent"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	require.Nil(path3, "nonexistent should not match")
}

func TestArrowIteratorCaveatCombination(t *testing.T) {
	require := require.New(t)

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		rel, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		require.NotNil(rel, "Arrow should return combined relation")
		relCaveat := rel.Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, relCaveat.GetOperation().Op, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children")
	})

	t.Run("LeftCaveat_Right_NoCaveat", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		rel, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		require.NotNil(rel, "Arrow should return one relation")
		require.NotNil(rel.Caveat, "Left caveat should be preserved")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Left_NoCaveat_Right_Caveat", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		rel, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		require.NotNil(rel, "Arrow should return one relation")
		require.NotNil(rel.Caveat, "Right caveat should be preserved")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Neither_Side_Has_Caveat", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Left side path with no caveat
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")

		// Right side path with no caveat
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		rel, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		require.NotNil(rel, "Arrow should return one relation")
		require.Nil(rel.Caveat, "No caveat should result in no caveat")
	})

	t.Run("Multiple_Relations_Mixed_Caveats", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		leftIter := NewFixedIterator(*leftPath1, *leftPath2)
		rightIter := NewFixedIterator(*rightPath1, *rightPath2)

		arrow := NewArrowIterator(leftIter, rightIter)

		// Check doc1 and doc2 separately
		relDoc1, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(relDoc1, "Arrow should return relation for doc1")
		require.NotNil(relDoc1.Caveat, "doc1 result should have a caveat")

		relDoc2, err := ctx.Check(arrow, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(relDoc2, "Arrow should return relation for doc2")
		require.NotNil(relDoc2.Caveat, "doc2 result should have a caveat")
	})

	t.Run("No_Matching_Arrow_Relations", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		rel, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		require.Nil(rel, "No matching arrow relations should result in nil")
	})
}

func TestArrowIterSubjects(t *testing.T) {
	require := require.New(t)

	t.Run("SimpleArrow", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Left: doc1 -> folder1
		// Right: folder1 -> alice
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find one subject through arrow")
		require.Equal("alice", paths[0].Subject.ObjectID)
		require.Equal("doc1", paths[0].Resource.ObjectID)
	})

	t.Run("MultipleSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Left: doc1 -> folder1
		// Right: folder1 -> alice, folder1 -> bob
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		rightPath1 := MustPathFromString("folder:folder1#viewer@user:alice")
		rightPath2 := MustPathFromString("folder:folder1#viewer@user:bob")

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath1, *rightPath2)

		arrow := NewArrowIterator(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
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
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Empty left side
		leftIter := NewFixedIterator()
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "No left paths should result in no subjects")
	})

	t.Run("NoRightPaths", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Left exists but right is empty
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator()

		arrow := NewArrowIterator(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "No right paths should result in no subjects")
	})

	t.Run("CaveatCombination", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

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

		leftIter := NewFixedIterator(*leftPath)
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		pathSeq, err := ctx.IterSubjects(arrow, NewObject("document", "doc1"), NoObjectFilter())
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
	testArrowBothDirections(t, "BasicCheck", func(t *testing.T, direction arrowDirection) {
		require := require.New(t)

		// Left side: document parent relationships to folders
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(*leftPath)

		// Right side: folder viewer relationships
		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)
		arrow.direction = direction // Set the direction explicitly

		// Create context
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test arrow operation
		path, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// Both directions should find the same relationship
		require.NotNil(path, "Arrow should return one relation")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("alice", path.Subject.ObjectID)
	})

	testArrowBothDirections(t, "NoMatch", func(t *testing.T, direction arrowDirection) {
		require := require.New(t)

		// Left side points to folder1
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(*leftPath)

		// Right side only has folder2 (no match)
		rightPath := MustPathFromString("folder:folder2#viewer@user:alice")
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)
		arrow.direction = direction

		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		path, err := ctx.Check(arrow, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		// Both directions should return nil when no match
		require.Nil(path, "No matching arrow relations should result in nil")
	})
}

func TestArrow_Types(t *testing.T) {
	t.Run("ResourceType", func(t *testing.T) {
		require := require.New(t)

		// Create left and right iterators
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(*leftPath)

		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		resourceType, err := arrow.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("document", resourceType[0].Type) // From left iterator
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		require := require.New(t)

		// Create left and right iterators
		leftPath := MustPathFromString("document:doc1#parent@folder:folder1")
		leftIter := NewFixedIterator(*leftPath)

		rightPath := MustPathFromString("folder:folder1#viewer@user:alice")
		rightIter := NewFixedIterator(*rightPath)

		arrow := NewArrowIterator(leftIter, rightIter)

		subjectTypes, err := arrow.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1) // From right iterator
		require.Equal("user", subjectTypes[0].Type)
	})
}

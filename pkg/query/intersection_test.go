package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestIntersectionIterator(t *testing.T) {
	require := require.New(t)

	t.Run("Check_Intersection", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Create an intersection where both iterators have matching data
		// Both iterators should have alice with access to doc1
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersectionIterator(documentAccess, multiRole)

		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Should return one merged path for the endpoint")
		require.True(path.Resource.Equals(NewObject("document", "doc1")), "Resource should match")
		require.True(tuple.ONREqual(path.Subject, NewObject("user", "alice").WithEllipses()), "Subject should match")
	})

	t.Run("Check_EmptyIntersection", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Create an intersection with contradictory requirements
		// Use iterators that don't have overlapping data
		documentAccess := NewDocumentAccessFixedIterator()
		singleUser := NewSingleUserFixedIterator("bob") // bob won't be in documentAccess

		intersect := NewIntersectionIterator(documentAccess, singleUser)

		// Use a subject that doesn't exist in both
		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "bob").WithEllipses())
		require.NoError(err)
		require.Nil(path)
	})

	t.Run("Check_NoSubIterators", func(t *testing.T) {
		ctx := NewTestContext(t)
		intersect := NewIntersectionIterator()

		// Empty intersection should return nil
		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path)
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		ctx := NewTestContext(t)
		documentAccess := NewDocumentAccessFixedIterator()
		intersect := NewIntersectionIterator(documentAccess)

		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Single iterator intersection should return results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		ctx := NewTestContext(t)
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersectionIterator(documentAccess, multiRole)

		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)
		require.Nil(path, "Nonexistent subject should return nil")
	})

	t.Run("IterSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Add test iterators
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersectionIterator(documentAccess, multiRole)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return subjects that are in BOTH iterators
		require.NotEmpty(paths, "Intersection should find common subjects")
	})

	t.Run("IterResources", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Add test iterators
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersectionIterator(documentAccess, multiRole)

		pathSeq, err := ctx.IterResources(intersect, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return resources that are in BOTH iterators
		require.NotEmpty(paths, "Intersection should find common resources")
	})
}

func TestIntersectionIteratorClone(t *testing.T) {
	require := require.New(t)

	// Create test context
	ctx := NewTestContext(t)

	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	original := NewIntersectionIterator(documentAccess, multiRole)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// Test that both iterators produce the same results
	subjectID := "alice"

	// Get result from original iterator
	originalPath, err := ctx.Check(original, NewObject("document", "doc1"), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)

	// Get result from cloned iterator
	clonedPath, err := ctx.Check(cloned, NewObject("document", "doc1"), NewObject("user", subjectID).WithEllipses())
	require.NoError(err)

	// Both should agree on whether doc1:alice is found
	require.Equal(originalPath != nil, clonedPath != nil, "cloned iterator should produce same result as original")
}

func TestIntersectionIteratorExplain(t *testing.T) {
	require := require.New(t)

	t.Run("EmptyIntersection", func(t *testing.T) {
		// NewIntersectionIterator() with no args returns empty FixedIterator (canonical form)
		intersect := NewIntersectionIterator()
		_, isFixed := intersect.(*FixedIterator)
		require.True(isFixed, "empty intersection should be canonicalized to FixedIterator")

		explain := intersect.Explain()
		require.Contains(explain.Info, "Fixed", "empty intersection should be a FixedIterator")
		require.Empty(explain.SubExplain, "empty fixed iterator should have no sub-explains")
	})

	t.Run("IntersectionWithSubIterators", func(t *testing.T) {
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		intersect := NewIntersectionIterator(documentAccess, multiRole)

		explain := intersect.Explain()
		require.Equal("Intersection", explain.Info)
		require.Len(explain.SubExplain, 2, "intersection should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Intersection")
		require.NotEmpty(explainStr)
	})
}

func TestIntersectionIteratorEarlyTermination(t *testing.T) {
	require := require.New(t)

	// Create test context
	ctx := NewTestContext(t)

	// Create an intersection where the first iterator returns no results
	// This should cause early termination
	// First, add an iterator that will return no results for the given subject
	emptyIterator := NewEmptyFixedIterator()
	documentAccess := NewDocumentAccessFixedIterator()

	intersect := NewIntersectionIterator(emptyIterator, documentAccess)

	// Use any subject - should get nil due to empty first iterator
	path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	require.Nil(path, "Early termination should return nil")
}

func TestIntersectionIteratorCaveatCombination(t *testing.T) {
	require := require.New(t)

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		iter1 := NewFixedIterator(*pathWithCaveat1)
		iter2 := NewFixedIterator(*pathWithCaveat2)

		intersect := NewIntersectionIterator(iter1, iter2)

		rel, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Intersection should return one combined relation")
		relCaveat := rel.Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, relCaveat.GetOperation().Op, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children")
	})

	t.Run("OneCaveat_One_NoCaveat_AND_Logic", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		iter1 := NewFixedIterator(*pathWithCaveat)
		iter2 := NewFixedIterator(*pathNoCaveat)

		intersect := NewIntersectionIterator(iter1, iter2)

		rel, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Intersection should return one relation")
		require.NotNil(rel.Caveat, "Caveat should be preserved in AND logic")
		// Note: Checking exact caveat content would require parsing the CaveatExpression
	})

	t.Run("Different_Relations_Same_Endpoint", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		// Each iterator returns a single path with a different relation
		// so that the intersection merges viewer AND editor → empty relation
		iter1 := NewFixedIterator(*pathViewer)
		iter2 := NewFixedIterator(*pathEditor)

		intersect := NewIntersectionIterator(iter1, iter2)

		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Should return one merged path since intersection works on endpoints, not individual relations")

		// The merged path should combine both relations and caveats
		require.NotNil(path.Caveat, "Merged path should have combined caveat")
		require.Equal("document", path.Resource.ObjectType)
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("user", path.Subject.ObjectType)
		require.Equal("alice", path.Subject.ObjectID)
		// The relation should be empty since different relations were merged (per Path.mergeFrom logic)
		require.Empty(path.Relation, "Different relations should result in empty relation")
	})

	t.Run("No_Common_Endpoints", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Iterators with no common endpoints - should return nil
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

		iter1 := NewFixedIterator(*pathDoc1Alice)
		iter2 := NewFixedIterator(*pathDoc2Bob)

		intersect := NewIntersectionIterator(iter1, iter2)

		// iter2 has doc2 for bob; iter1 has doc1 for alice — no common endpoint for alice on doc1
		path, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path, "No common endpoints should return nil")
	})

	t.Run("Three_Iterators_Mixed_Caveats", func(t *testing.T) {
		ctx := NewTestContext(t)
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

		iter1 := NewFixedIterator(*pathCaveat1)
		iter2 := NewFixedIterator(*pathNoCaveat)
		iter3 := NewFixedIterator(*pathCaveat2)

		intersect := NewIntersectionIterator(iter1, iter2, iter3)

		rel, err := ctx.Check(intersect, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Should return one intersected path")
		relCaveat := rel.Caveat
		require.NotNil(relCaveat, "Final result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, relCaveat.GetOperation().Op, "Caveat should be an AND")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children (caveat1 and caveat2)")
	})
}

func TestIntersectionIterSubjects(t *testing.T) {
	require := require.New(t)

	t.Run("SimpleIntersection", func(t *testing.T) {
		ctx := NewTestContext(t)

		// Both iterators return alice for doc1
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:alice")

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find alice in intersection")
		require.Equal("alice", paths[0].Subject.ObjectID)
	})

	t.Run("NoCommonSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)

		// Different subjects in each iterator
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:bob")

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "No common subjects should result in empty")
	})

	t.Run("EmptyIterator", func(t *testing.T) {
		ctx := NewTestContext(t)

		// One iterator is empty
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator()

		intersect := NewIntersectionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "Empty iterator should short-circuit to empty result")
	})

	t.Run("ThreeIteratorsWithCommonSubject", func(t *testing.T) {
		ctx := NewTestContext(t)

		// Alice appears in all three
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:alice")
		path3 := MustPathFromString("document:doc1#owner@user:alice")

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)
		iter3 := NewFixedIterator(*path3)

		intersect := NewIntersectionIterator(iter1, iter2, iter3)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find alice in all three")
		require.Equal("alice", paths[0].Subject.ObjectID)
	})

	t.Run("CaveatCombination", func(t *testing.T) {
		ctx := NewTestContext(t)

		// Both have alice with different caveats
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		path2 := MustPathFromString("document:doc1#editor@user:alice")
		path2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(intersect, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "Should find alice with combined caveats")
		require.NotNil(paths[0].Caveat, "Should have combined caveat")
		require.NotNil(paths[0].Caveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, paths[0].Caveat.GetOperation().Op, "Caveat should be AND")
	})
}

func TestIntersection_Types(t *testing.T) {
	t.Run("ResourceType", func(t *testing.T) {
		require := require.New(t)

		// Create intersection with fixed iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:alice")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		resourceType, err := intersect.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("document", resourceType[0].Type)
	})

	t.Run("ResourceType_EmptyIntersection", func(t *testing.T) {
		require := require.New(t)

		intersect := NewIntersectionIterator()

		resourceType, err := intersect.ResourceType()
		require.NoError(err)
		require.Empty(resourceType)
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		require := require.New(t)

		// Create intersection with different subject types across iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@group:engineers#member")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		subjectTypes, err := intersect.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 2, "Should collect all subject types from all iterators")
		require.Contains(subjectTypes, ObjectType{Type: "user", Subrelation: tuple.Ellipsis})
		require.Contains(subjectTypes, ObjectType{Type: "group", Subrelation: "member"})
	})

	t.Run("SubjectTypes_Deduplication", func(t *testing.T) {
		require := require.New(t)

		// Create intersection where multiple iterators have same subject types
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:bob")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		subjectTypes, err := intersect.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1, "Should deduplicate subject types")
		require.Equal("user", subjectTypes[0].Type)
	})

	t.Run("SubjectTypes_EmptyIntersection", func(t *testing.T) {
		require := require.New(t)

		intersect := NewIntersectionIterator()

		subjectTypes, err := intersect.SubjectTypes()
		require.NoError(err)
		require.Empty(subjectTypes)
	})

	t.Run("ResourceType_Mismatch", func(t *testing.T) {
		require := require.New(t)

		// Create intersection with mismatched resource types
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("folder:folder1#viewer@user:bob")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		intersect := NewIntersectionIterator(iter1, iter2)

		// Intersection returns common types - with mismatched types, should be empty
		resourceTypes, err := intersect.ResourceType()
		require.NoError(err)
		require.Empty(resourceTypes, "Intersection of document and folder should be empty")
	})
}

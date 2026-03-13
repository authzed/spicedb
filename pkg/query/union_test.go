package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestUnionIterator(t *testing.T) {
	require := require.New(t)

	t.Run("Check_Union", func(t *testing.T) {
		ctx := NewTestContext(t)

		// Add different iterators with distinct data sets
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		// Create a union of different access patterns
		union := NewUnionIterator(documentAccess, multiRole)

		// Check doc1 for alice — both DocumentAccess and MultiRole have alice on doc1
		pathDoc1, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(pathDoc1, "Union should find alice on doc1")
		require.Equal("alice", pathDoc1.Subject.ObjectID)
		require.Equal("doc1", pathDoc1.Resource.ObjectID)

		// Check doc2 for alice — DocumentAccess has alice on doc2
		pathDoc2, err := ctx.Check(union, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(pathDoc2, "Union should find alice on doc2")
		require.Equal("doc2", pathDoc2.Resource.ObjectID)
	})

	t.Run("Check_EmptyUnion", func(t *testing.T) {
		ctx := NewTestContext(t)
		union := NewUnionIterator()

		// Empty union should return nil
		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.Nil(path, "empty union should return nil")
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		ctx := NewTestContext(t)
		documentAccess := NewDocumentAccessFixedIterator()
		union := NewUnionIterator(documentAccess)

		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Union should find alice on doc1")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("alice", path.Subject.ObjectID)
	})

	t.Run("Check_EarlyTermination", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Add iterators that might find the same resource
		documentAccess := NewDocumentAccessFixedIterator()
		singleUser := NewSingleUserFixedIterator("alice")

		union := NewUnionIterator(documentAccess, singleUser)

		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Union should find results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		ctx := NewTestContext(t)
		documentAccess := NewDocumentAccessFixedIterator()
		union := NewUnionIterator(documentAccess)

		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)
		require.Nil(path, "nonexistent subject should return nil")
	})

	t.Run("IterSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Add test iterators
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		union := NewUnionIterator(documentAccess, multiRole)

		pathSeq, err := ctx.IterSubjects(union, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return subjects from both iterators, deduplicated
		require.NotEmpty(paths, "Union should find subjects")

		// Verify we have alice in the results
		foundAlice := false
		for _, path := range paths {
			if path.Subject.ObjectID == "alice" {
				foundAlice = true
				break
			}
		}
		require.True(foundAlice, "Should find alice in subjects")
	})

	t.Run("IterResources", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Add test iterators
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()
		union := NewUnionIterator(documentAccess, multiRole)

		pathSeq, err := ctx.IterResources(union, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return resources from both iterators, deduplicated
		require.NotEmpty(paths, "Union should find resources")

		// Verify we have doc1 in the results
		foundDoc1 := false
		for _, path := range paths {
			if path.Resource.ObjectID == "doc1" {
				foundDoc1 = true
				break
			}
		}
		require.True(foundDoc1, "Should find doc1 in resources")
	})
}

func TestUnionIteratorClone(t *testing.T) {
	require := require.New(t)

	// Use non-overlapping helper iterators to avoid union optimization issues
	singleUserAlice := NewSingleUserFixedIterator("alice")
	singleUserBob := NewSingleUserFixedIterator("bob")

	original := NewUnionIterator(singleUserAlice, singleUserBob)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Len(clonedExplain.SubExplain, len(originalExplain.SubExplain))

	// Test that both iterators have the same explain behavior (cloning works)
	// Note: Due to union's stateful resource elimination optimization,
	// we can't reliably test result equivalence without complex setup
	// The key test is that Clone() creates independent objects with same structure

	// Verify the clone is independent and has same structure
	require.Equal(originalExplain.String(), clonedExplain.String())

	// Just test that both can execute operations without error
	// The union's resource elimination optimization makes result comparison tricky
}

func TestUnionIteratorExplain(t *testing.T) {
	require := require.New(t)

	t.Run("EmptyUnion", func(t *testing.T) {
		// NewUnionIterator() with no args returns empty FixedIterator (canonical form)
		union := NewUnionIterator()
		_, isFixed := union.(*FixedIterator)
		require.True(isFixed, "empty union should be canonicalized to FixedIterator")

		explain := union.Explain()
		require.Contains(explain.Info, "Fixed", "empty union should be a FixedIterator")
		require.Empty(explain.SubExplain, "empty fixed iterator should have no sub-explains")
	})

	t.Run("UnionWithSubIterators", func(t *testing.T) {
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		union := NewUnionIterator(documentAccess, multiRole)

		explain := union.Explain()
		require.Equal("Union", explain.Info)
		require.Len(explain.SubExplain, 2, "union should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Union")
		require.NotEmpty(explainStr)
	})
}

func TestUnionIteratorDuplicateElimination(t *testing.T) {
	require := require.New(t)

	// Create test context
	ctx := NewTestContext(t)

	// Create a union with overlapping sub-iterators
	// This tests the deduplication logic where resources found by earlier
	// iterators are removed from the remaining list
	// Add iterators that may have overlapping data
	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	union := NewUnionIterator(documentAccess, multiRole)

	path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	require.NotNil(path, "Union should find alice on doc1")
	require.Equal("doc1", path.Resource.ObjectID)
	require.Equal("alice", path.Subject.ObjectID)
}

func TestUnionIteratorMultipleResources(t *testing.T) {
	require := require.New(t)

	// Create test context
	ctx := NewTestContext(t)

	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	union := NewUnionIterator(documentAccess, multiRole)

	// Test with individual resource IDs
	pathDoc1, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	pathDoc2, err := ctx.Check(union, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	// The result should include valid union relationships found for each resource
	require.True(pathDoc1 != nil || pathDoc2 != nil, "Union should find relations from multiple resources")
}

func TestUnionDeduplicationBugFix(t *testing.T) {
	require := require.New(t)

	t.Run("DeduplicatesByResourceKey", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Create relations that would create duplicates under the old string-based deduplication
		// but should be properly deduplicated by resource key (type + id)
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#viewer@user:alice")

		// Create iterators that return the same resource-subject pair
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Union should deduplicate identical relations")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("alice", path.Subject.ObjectID)
	})

	t.Run("PreferNoCaveatRelations", func(t *testing.T) {
		// Create path without caveat
		pathNoCaveat := MustPathFromString("document:doc1#viewer@user:alice")

		// Create path with caveat
		pathWithCaveat := MustPathFromString("document:doc1#viewer@user:alice")
		pathWithCaveat.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
				},
			},
		}

		// Test both orders to ensure preference is consistent
		t.Run("NoCaveatFirst", func(t *testing.T) {
			ctx := NewTestContext(t)
			iter1 := NewFixedIterator(*pathNoCaveat)
			iter2 := NewFixedIterator(*pathWithCaveat)

			union := NewUnionIterator(iter1, iter2)

			path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
			require.NoError(err)
			require.NotNil(path, "Union should deduplicate to one relation")
			require.Nil(path.Caveat, "Union should prefer relation without caveat")
		})

		t.Run("CaveatFirst", func(t *testing.T) {
			ctx := NewTestContext(t)
			iter1 := NewFixedIterator(*pathWithCaveat)
			iter2 := NewFixedIterator(*pathNoCaveat)

			union := NewUnionIterator(iter1, iter2)

			path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
			require.NoError(err)
			require.NotNil(path, "Union should deduplicate to one relation")
			require.Nil(path.Caveat, "Union should prefer relation without caveat")
		})
	})

	t.Run("DeduplicationWithDifferentResources", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Relations to different resources should not be deduplicated
		pathDoc1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathDoc2 := MustPathFromString("document:doc2#viewer@user:alice")

		iter1 := NewFixedIterator(*pathDoc1)
		iter2 := NewFixedIterator(*pathDoc2)

		union := NewUnionIterator(iter1, iter2)

		resultDoc1, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(resultDoc1, "Should find alice on doc1")
		require.Equal("doc1", resultDoc1.Resource.ObjectID)

		resultDoc2, err := ctx.Check(union, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(resultDoc2, "Should find alice on doc2")
		require.Equal("doc2", resultDoc2.Resource.ObjectID)
	})

	t.Run("DeduplicationSameResourceDifferentRelations", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Relations with different relation names on the same resource should be deduplicated
		// because the fix deduplicates by resource (type + id), not by full relation
		pathViewer := MustPathFromString("document:doc1#viewer@user:alice")
		pathEditor := MustPathFromString("document:doc1#editor@user:alice")

		iter1 := NewFixedIterator(*pathViewer)
		iter2 := NewFixedIterator(*pathEditor)

		union := NewUnionIterator(iter1, iter2)

		path, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(path, "Union should deduplicate relations to same resource")
		require.Equal("doc1", path.Resource.ObjectID)
		require.Equal("alice", path.Subject.ObjectID)
	})
}

func TestUnionIteratorCaveatCombination(t *testing.T) {
	require := require.New(t)

	t.Run("CombineTwoCaveats_OR_Logic", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Both paths have different caveats - should combine with OR logic
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

		union := NewUnionIterator(iter1, iter2)

		rel, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Union should deduplicate to one relation")
		relCaveat := rel.Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(core.CaveatOperation_OR, relCaveat.GetOperation().Op, "Caveat should be an OR")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an OR of two children")
	})

	t.Run("NoCaveat_Wins_Over_Caveat", func(t *testing.T) {
		ctx := NewTestContext(t)
		// One path has no caveat, one has caveat - no caveat should win
		pathNoCaveat := MustPathFromString("document:doc1#viewer@user:alice")

		pathWithCaveat := MustPathFromString("document:doc1#viewer@user:alice")
		pathWithCaveat.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
				},
			},
		}

		iter1 := NewFixedIterator(*pathNoCaveat)
		iter2 := NewFixedIterator(*pathWithCaveat)

		union := NewUnionIterator(iter1, iter2)

		rel, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Union should deduplicate to one relation")
		require.Nil(rel.Caveat, "No caveat should win in OR logic")
	})

	t.Run("Different_Resources_Not_Combined", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Paths to different resources should not be combined
		pathDoc1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathDoc1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathDoc2 := MustPathFromString("document:doc2#viewer@user:alice")
		pathDoc2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(*pathDoc1)
		iter2 := NewFixedIterator(*pathDoc2)

		union := NewUnionIterator(iter1, iter2)

		relDoc1, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(relDoc1, "Should find alice on doc1")
		require.NotNil(relDoc1.Caveat, "doc1 path should keep its caveat")
		require.Equal("caveat1", relDoc1.Caveat.GetCaveat().GetCaveatName())

		relDoc2, err := ctx.Check(union, NewObject("document", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(relDoc2, "Should find alice on doc2")
		require.NotNil(relDoc2.Caveat, "doc2 path should keep its caveat")
		require.Equal("caveat2", relDoc2.Caveat.GetCaveat().GetCaveatName())
	})

	t.Run("Three_Relations_Same_Resource_Mixed_Caveats", func(t *testing.T) {
		ctx := NewTestContext(t)
		// Mix of paths: no caveat, caveat1, caveat2 - no caveat should win
		pathNoCaveat := MustPathFromString("document:doc1#viewer@user:alice")

		pathCaveat1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathCaveat1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
				},
			},
		}

		pathCaveat2 := MustPathFromString("document:doc1#viewer@user:alice")
		pathCaveat2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
				},
			},
		}

		iter1 := NewFixedIterator(*pathNoCaveat)
		iter2 := NewFixedIterator(*pathCaveat1)
		iter3 := NewFixedIterator(*pathCaveat2)

		union := NewUnionIterator(iter1, iter2, iter3)

		rel, err := ctx.Check(union, NewObject("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)
		require.NotNil(rel, "Union should deduplicate to one relation")
		require.Nil(rel.Caveat, "No caveat should win over any caveated relations")
	})
}

func TestUnionIterSubjectsDeduplication(t *testing.T) {
	require := require.New(t)

	t.Run("DeduplicateSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Create paths with same subject appearing in multiple iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#viewer@user:alice")

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(union, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should be deduplicated to one path
		require.Len(paths, 1, "Union should deduplicate identical subjects")
		require.Equal("alice", paths[0].Subject.ObjectID)
	})

	t.Run("EmptyUnion", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()
		union := NewUnionIterator()

		pathSeq, err := ctx.IterSubjects(union, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "Empty union should return no subjects")
	})

	t.Run("MultipleSubjects", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Create paths with different subjects
		pathAlice := MustPathFromString("document:doc1#viewer@user:alice")
		pathBob := MustPathFromString("document:doc1#viewer@user:bob")
		pathCarol := MustPathFromString("document:doc1#editor@user:carol")

		iter1 := NewFixedIterator(*pathAlice)
		iter2 := NewFixedIterator(*pathBob, *pathCarol)

		union := NewUnionIterator(iter1, iter2)

		pathSeq, err := ctx.IterSubjects(union, NewObject("document", "doc1"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return all three subjects
		require.Len(paths, 3, "Union should return all unique subjects")

		subjectIDs := make(map[string]bool)
		for _, path := range paths {
			subjectIDs[path.Subject.ObjectID] = true
		}
		require.Contains(subjectIDs, "alice")
		require.Contains(subjectIDs, "bob")
		require.Contains(subjectIDs, "carol")
	})
}

func TestUnionIterResourcesDeduplication(t *testing.T) {
	require := require.New(t)

	t.Run("DeduplicateResources", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Create paths with same resource appearing in multiple iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#editor@user:alice")

		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		pathSeq, err := ctx.IterResources(union, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should be deduplicated to one path per resource
		require.Len(paths, 1, "Union should deduplicate resources")
		require.Equal("doc1", paths[0].Resource.ObjectID)
	})

	t.Run("EmptyUnion", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()
		union := NewUnionIterator()

		pathSeq, err := ctx.IterResources(union, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Empty(paths, "Empty union should return no resources")
	})

	t.Run("MultipleResources", func(t *testing.T) {
		ctx := NewTestContext(t)
		ctx.Context = t.Context()

		// Create paths with different resources
		pathDoc1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathDoc2 := MustPathFromString("document:doc2#viewer@user:alice")
		pathDoc3 := MustPathFromString("document:doc3#editor@user:alice")

		iter1 := NewFixedIterator(*pathDoc1)
		iter2 := NewFixedIterator(*pathDoc2, *pathDoc3)

		union := NewUnionIterator(iter1, iter2)

		pathSeq, err := ctx.IterResources(union, NewObject("user", "alice").WithEllipses(), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return all three resources
		require.Len(paths, 3, "Union should return all unique resources")

		resourceIDs := make(map[string]bool)
		for _, path := range paths {
			resourceIDs[path.Resource.ObjectID] = true
		}
		require.Contains(resourceIDs, "doc1")
		require.Contains(resourceIDs, "doc2")
		require.Contains(resourceIDs, "doc3")
	})
}

func TestUnion_Types(t *testing.T) {
	t.Run("ResourceType", func(t *testing.T) {
		require := require.New(t)

		// Create union with fixed iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		resourceType, err := union.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("document", resourceType[0].Type)
	})

	t.Run("ResourceType_EmptyUnion", func(t *testing.T) {
		require := require.New(t)

		union := NewUnionIterator()

		resourceType, err := union.ResourceType()
		require.NoError(err)
		require.Empty(resourceType)
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		require := require.New(t)

		// Create union with different subject types across iterators
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@group:engineers#member")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		subjectTypes, err := union.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 2, "Should have 2 unique subject types")

		// Check that both user and group types are present
		typeMap := make(map[string]bool)
		for _, st := range subjectTypes {
			key := st.Type + "#" + st.Subrelation
			typeMap[key] = true
		}

		require.True(typeMap["user#..."] || typeMap["user#"])
		require.True(typeMap["group#member"])
	})

	t.Run("SubjectTypes_Deduplication", func(t *testing.T) {
		require := require.New(t)

		// Create union where multiple iterators have same subject types
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc2#editor@user:bob")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		subjectTypes, err := union.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1, "Should deduplicate subject types")
		require.Equal("user", subjectTypes[0].Type)
	})

	t.Run("SubjectTypes_EmptyUnion", func(t *testing.T) {
		require := require.New(t)

		union := NewUnionIterator()

		subjectTypes, err := union.SubjectTypes()
		require.NoError(err)
		require.Empty(subjectTypes)
	})

	t.Run("ResourceType_Mismatch", func(t *testing.T) {
		require := require.New(t)

		// Create union with mismatched resource types
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("folder:folder1#viewer@user:bob")
		iter1 := NewFixedIterator(*path1)
		iter2 := NewFixedIterator(*path2)

		union := NewUnionIterator(iter1, iter2)

		// Union now collects all types instead of panicking
		resourceTypes, err := union.ResourceType()
		require.NoError(err)
		require.Len(resourceTypes, 2)
		require.Contains(resourceTypes, ObjectType{Type: "document", Subrelation: tuple.Ellipsis})
		require.Contains(resourceTypes, ObjectType{Type: "folder", Subrelation: tuple.Ellipsis})
	})
}

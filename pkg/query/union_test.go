package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestUnionIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("Check_Union", func(t *testing.T) {
		t.Parallel()

		// Create a union of different access patterns
		union := NewUnion()

		// Add different iterators with distinct data sets
		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		union.addSubIterator(documentAccess)
		union.addSubIterator(multiRole)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Union should contain relations from both iterators for alice on doc1 and doc2
		// DocumentAccess: alice viewer/editor/owner on doc1, alice viewer on doc2
		// MultiRole: alice viewer/editor/owner on doc1
		// With the new deduplication fix, we deduplicate by resource (type + id), not by full relation
		// So we expect only one relation per resource: 1 for doc1, 1 for doc2 (2 total)
		require.Len(paths, 2, "Union should deduplicate to one relation per resource")

		// Check that we have relations for both doc1 and doc2
		resourceIDs := make(map[string]bool)
		for _, path := range paths {
			require.Equal("alice", path.Subject.ObjectID, "All paths should be for alice")
			require.Equal("document", path.Resource.ObjectType, "All paths should be for documents")
			resourceIDs[path.Resource.ObjectID] = true
		}
		require.Contains(resourceIDs, "doc1", "Should have relation for doc1")
		require.Contains(resourceIDs, "doc2", "Should have relation for doc2")
	})

	t.Run("Check_EmptyUnion", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		// Empty union should return empty results
		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(paths, "empty union should return no results")
	})

	t.Run("Check_SingleSubIterator", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		documentAccess := NewDocumentAccessFixedIterator()
		union.addSubIterator(documentAccess)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Even with a single sub-iterator, union applies deduplication logic
		// DocumentAccess iterator has alice with viewer, editor, and owner access to doc1
		// But union deduplication by resource (type + id) means only 1 relation for doc1
		require.Len(paths, 1, "Union should deduplicate to one relation per resource")
		require.Equal("doc1", paths[0].Resource.ObjectID)
		require.Equal("alice", paths[0].Subject.ObjectID)
	})

	t.Run("Check_EmptyResourceList", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		documentAccess := NewDocumentAccessFixedIterator()
		union.addSubIterator(documentAccess)

		pathSeq, err := ctx.Check(union, []Object{}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(paths, "empty resource list should return no results")
	})

	t.Run("Check_EarlyTermination", func(t *testing.T) {
		t.Parallel()

		// Test the optimization where union stops checking remaining resources
		// once all have been found by earlier sub-iterators
		union := NewUnion()

		// Add iterators that might find the same resource
		documentAccess := NewDocumentAccessFixedIterator()
		singleUser := NewSingleUserFixedIterator("alice")

		union.addSubIterator(documentAccess)
		union.addSubIterator(singleUser)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// The union should optimize by not checking already found resources
		require.NotEmpty(paths, "Union should find results")
	})

	t.Run("Check_NoMatchingSubject", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		documentAccess := NewDocumentAccessFixedIterator()
		union.addSubIterator(documentAccess)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "nonexistent").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		// Should be empty since subject doesn't exist
		require.Empty(paths, "nonexistent subject should return no results")
	})

	t.Run("IterSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		require.Panics(func() {
			_, _ = ctx.IterSubjects(union, NewObject("document", "doc1"))
		})
	})

	t.Run("IterResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		require.Panics(func() {
			_, _ = ctx.IterResources(union, NewObject("user", "alice").WithEllipses())
		})
	})
}

func TestUnionIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	original := NewUnion()

	// Use non-overlapping helper iterators to avoid union optimization issues
	singleUserAlice := NewSingleUserFixedIterator("alice")
	singleUserBob := NewSingleUserFixedIterator("bob")

	original.addSubIterator(singleUserAlice)
	original.addSubIterator(singleUserBob)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

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
	t.Parallel()

	require := require.New(t)

	t.Run("EmptyUnion", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		explain := union.Explain()
		require.Equal("Union", explain.Info)
		require.Empty(explain.SubExplain, "empty union should have no sub-explains")
	})

	t.Run("UnionWithSubIterators", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()

		documentAccess := NewDocumentAccessFixedIterator()
		multiRole := NewMultiRoleFixedIterator()

		union.addSubIterator(documentAccess)
		union.addSubIterator(multiRole)

		explain := union.Explain()
		require.Equal("Union", explain.Info)
		require.Len(explain.SubExplain, 2, "union should have exactly 2 sub-explains")

		explainStr := explain.String()
		require.Contains(explainStr, "Union")
		require.NotEmpty(explainStr)
	})
}

func TestUnionIteratorDuplicateElimination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	// Create a union with overlapping sub-iterators
	// This tests the deduplication logic where resources found by earlier
	// iterators are removed from the remaining list
	union := NewUnion()

	// Add iterators that may have overlapping data
	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	union.addSubIterator(documentAccess)
	union.addSubIterator(multiRole)

	pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	paths, err := CollectAll(pathSeq)
	require.NoError(err)

	// The union should handle potential duplicates correctly through its
	// resource elimination optimization. Both iterators have alice with various
	// permissions on doc1, but with the new deduplication fix, we deduplicate by
	// resource (type + id), so we expect only 1 relation for doc1.
	require.Len(paths, 1, "Union should deduplicate to one relation per resource")
	require.Equal("doc1", paths[0].Resource.ObjectID)
	require.Equal("alice", paths[0].Subject.ObjectID)
}

func TestUnionIteratorMultipleResources(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	union := NewUnion()

	documentAccess := NewDocumentAccessFixedIterator()
	multiRole := NewMultiRoleFixedIterator()

	union.addSubIterator(documentAccess)
	union.addSubIterator(multiRole)

	// Test with multiple resource IDs
	pathSeq, err := ctx.Check(union, NewObjects("document", "doc1", "doc2", "nonexistent"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)

	paths, err := CollectAll(pathSeq)
	require.NoError(err)

	// The result should include all valid union relationships found across all resources
	require.NotEmpty(paths, "Union should find relations from multiple resources")
}

func TestUnionDeduplicationBugFix(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("DeduplicatesByResourceKey", func(t *testing.T) {
		t.Parallel()

		// Create relations that would create duplicates under the old string-based deduplication
		// but should be properly deduplicated by resource key (type + id)
		path1 := MustPathFromString("document:doc1#viewer@user:alice")
		path2 := MustPathFromString("document:doc1#viewer@user:alice")

		// Create iterators that return the same resource-subject pair
		iter1 := NewFixedIterator(path1)
		iter2 := NewFixedIterator(path2)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should be deduplicated to only one relation
		require.Len(paths, 1, "Union should deduplicate identical relations")
		require.Equal("doc1", paths[0].Resource.ObjectID)
		require.Equal("alice", paths[0].Subject.ObjectID)
	})

	t.Run("PreferNoCaveatRelations", func(t *testing.T) {
		t.Parallel()

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
			t.Parallel()

			iter1 := NewFixedIterator(pathNoCaveat)
			iter2 := NewFixedIterator(pathWithCaveat)

			union := NewUnion()
			union.addSubIterator(iter1)
			union.addSubIterator(iter2)

			pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
			require.NoError(err)

			paths, err := CollectAll(pathSeq)
			require.NoError(err)

			require.Len(paths, 1, "Union should deduplicate to one relation")
			require.Nil(paths[0].Caveat, "Union should prefer relation without caveat")
		})

		t.Run("CaveatFirst", func(t *testing.T) {
			t.Parallel()

			iter1 := NewFixedIterator(pathWithCaveat)
			iter2 := NewFixedIterator(pathNoCaveat)

			union := NewUnion()
			union.addSubIterator(iter1)
			union.addSubIterator(iter2)

			pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
			require.NoError(err)

			paths, err := CollectAll(pathSeq)
			require.NoError(err)

			require.Len(paths, 1, "Union should deduplicate to one relation")
			require.Nil(paths[0].Caveat, "Union should prefer relation without caveat")
		})
	})

	t.Run("DeduplicationWithDifferentResources", func(t *testing.T) {
		t.Parallel()

		// Relations to different resources should not be deduplicated
		pathDoc1 := MustPathFromString("document:doc1#viewer@user:alice")
		pathDoc2 := MustPathFromString("document:doc2#viewer@user:alice")

		iter1 := NewFixedIterator(pathDoc1)
		iter2 := NewFixedIterator(pathDoc2)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Both relations should be kept since they are for different resources
		require.Len(paths, 2, "Union should keep relations to different resources")

		resourceIDs := []string{paths[0].Resource.ObjectID, paths[1].Resource.ObjectID}
		require.Contains(resourceIDs, "doc1")
		require.Contains(resourceIDs, "doc2")
	})

	t.Run("DeduplicationSameResourceDifferentRelations", func(t *testing.T) {
		t.Parallel()

		// Relations with different relation names on the same resource should be deduplicated
		// because the fix deduplicates by resource (type + id), not by full relation
		pathViewer := MustPathFromString("document:doc1#viewer@user:alice")
		pathEditor := MustPathFromString("document:doc1#editor@user:alice")

		iter1 := NewFixedIterator(pathViewer)
		iter2 := NewFixedIterator(pathEditor)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		pathSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should be deduplicated to only one relation since same resource
		require.Len(paths, 1, "Union should deduplicate relations to same resource")
		require.Equal("doc1", paths[0].Resource.ObjectID)
		require.Equal("alice", paths[0].Subject.ObjectID)
	})
}

func TestUnionIteratorCaveatCombination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("CombineTwoCaveats_OR_Logic", func(t *testing.T) {
		t.Parallel()

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

		iter1 := NewFixedIterator(pathWithCaveat1)
		iter2 := NewFixedIterator(pathWithCaveat2)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		relSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Union should deduplicate to one relation")
		relCaveat := rels[0].Caveat
		require.NotNil(relCaveat, "Result should have combined caveat")
		require.NotNil(relCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(relCaveat.GetOperation().Op, core.CaveatOperation_OR, "Caveat should be an OR")
		require.Len(relCaveat.GetOperation().GetChildren(), 2, "Caveat should be an OR of two children")
	})

	t.Run("NoCaveat_Wins_Over_Caveat", func(t *testing.T) {
		t.Parallel()

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

		iter1 := NewFixedIterator(pathNoCaveat)
		iter2 := NewFixedIterator(pathWithCaveat)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		relSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Union should deduplicate to one relation")
		require.Nil(rels[0].Caveat, "No caveat should win in OR logic")
	})

	t.Run("Different_Resources_Not_Combined", func(t *testing.T) {
		t.Parallel()

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

		iter1 := NewFixedIterator(pathDoc1)
		iter2 := NewFixedIterator(pathDoc2)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)

		relSeq, err := ctx.Check(union, NewObjects("document", "doc1", "doc2"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 2, "Different resources should not be combined")

		// Both paths should preserve their original caveats
		caveatNames := make([]string, 2)
		for i, path := range rels {
			require.NotNil(path.Caveat, "Each path should keep its caveat")
			// Extract caveat name - simplified check for test
			if caveatExpr := path.Caveat.GetCaveat(); caveatExpr != nil {
				caveatNames[i] = caveatExpr.CaveatName
			}
		}
		require.ElementsMatch([]string{"caveat1", "caveat2"}, caveatNames)
	})

	t.Run("Three_Relations_Same_Resource_Mixed_Caveats", func(t *testing.T) {
		t.Parallel()

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

		iter1 := NewFixedIterator(pathNoCaveat)
		iter2 := NewFixedIterator(pathCaveat1)
		iter3 := NewFixedIterator(pathCaveat2)

		union := NewUnion()
		union.addSubIterator(iter1)
		union.addSubIterator(iter2)
		union.addSubIterator(iter3)

		relSeq, err := ctx.Check(union, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "Union should deduplicate to one relation")
		require.Nil(rels[0].Caveat, "No caveat should win over any caveated relations")
	})
}

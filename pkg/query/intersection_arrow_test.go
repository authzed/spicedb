package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestIntersectionArrowIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("AllSubjectsSatisfyCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1 and team2
		leftRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team1", "..."),
			},
		}
		leftRel2 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team2", "..."),
			},
		}

		// Right side: alice is member of both team1 and team2
		rightRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
		}
		rightRel2 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team2", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
		}

		leftIter := NewFixedIterator(leftRel1, leftRel2)
		rightIter := NewFixedIterator(rightRel1, rightRel2)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test: alice should have access because she's a member of ALL teams (team1 and team2)
		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should return results since alice is in ALL teams
		require.Len(rels, 2, "Should return relations for both teams since alice is in all of them")
		
		// Verify the results - all results should have alice as the subject
		for _, rel := range rels {
			require.Equal("alice", rel.Subject.ObjectID, "All results should have alice as the subject")
		}
	})

	t.Run("NotAllSubjectsSatisfyCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1 and team2
		leftRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team1", "..."),
			},
		}
		leftRel2 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team2", "..."),
			},
		}

		// Right side: alice is member of team1 but NOT team2
		rightRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
		}
		// Note: no rightRel2 for team2, so alice is not in team2

		leftIter := NewFixedIterator(leftRel1, leftRel2)
		rightIter := NewFixedIterator(rightRel1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test: alice should NOT have access because she's not a member of ALL teams
		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should return empty since alice is not in ALL teams
		require.Empty(rels, "Should return no results since alice is not in all teams")
	})

	t.Run("SingleSubjectSatisfiesCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has only team1
		leftRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team1", "..."),
			},
		}

		// Right side: alice is member of team1
		rightRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
		}

		leftIter := NewFixedIterator(leftRel1)
		rightIter := NewFixedIterator(rightRel1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		// Test: alice should have access because she's a member of the only team
		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should return result since alice is in the only team
		require.Len(rels, 1, "Should return one result since alice is in the single team")
		require.Equal("alice", rels[0].Subject.ObjectID)
	})

	t.Run("NoLeftSubjects", func(t *testing.T) {
		t.Parallel()

		// Left side: document has no teams
		leftIter := NewFixedIterator() // Empty

		// Right side: alice is member of some team
		rightRel1 := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
		}
		rightIter := NewFixedIterator(rightRel1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should return empty since there are no left subjects
		require.Empty(rels, "Should return no results when there are no left subjects")
	})

	t.Run("ThreeTeamsAllSatisfied", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1, team2, and team3
		leftRels := []tuple.Relationship{
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("document", "doc1", "team"),
					Subject:  tuple.ONR("team", "team1", "..."),
				},
			},
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("document", "doc1", "team"),
					Subject:  tuple.ONR("team", "team2", "..."),
				},
			},
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("document", "doc1", "team"),
					Subject:  tuple.ONR("team", "team3", "..."),
				},
			},
		}

		// Right side: alice is member of all three teams
		rightRels := []tuple.Relationship{
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("team", "team1", "member"),
					Subject:  tuple.ONR("user", "alice", "..."),
				},
			},
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("team", "team2", "member"),
					Subject:  tuple.ONR("user", "alice", "..."),
				},
			},
			{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ONR("team", "team3", "member"),
					Subject:  tuple.ONR("user", "alice", "..."),
				},
			},
		}

		leftIter := NewFixedIterator(leftRels...)
		rightIter := NewFixedIterator(rightRels...)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		// Should return results for all three teams
		require.Len(rels, 3, "Should return relations for all three teams")
		
		// Verify all results have alice as the subject
		for _, rel := range rels {
			require.Equal("alice", rel.Subject.ObjectID, "All results should have alice as the subject")
		}
	})

	t.Run("EmptyResources", func(t *testing.T) {
		t.Parallel()

		leftIter := NewFixedIterator()
		rightIter := NewFixedIterator()
		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create context with LocalExecutor
		ctx := &Context{
			Context:  t.Context(),
			Executor: LocalExecutor{},
		}

		relSeq, err := ctx.Check(intersectionArrow, []Object{}, NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)
		require.Empty(rels, "empty resource list should return no results")
	})
}

func TestIntersectionArrowIteratorCaveatCombination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// Left side relation with caveat
		leftRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team1", "..."),
			},
			OptionalCaveat: &core.ContextualizedCaveat{
				CaveatName: "left_caveat",
			},
		}

		// Right side relation with different caveat
		rightRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
			OptionalCaveat: &core.ContextualizedCaveat{
				CaveatName: "right_caveat",
			},
		}

		leftIter := NewFixedIterator(leftRel)
		rightIter := NewFixedIterator(rightRel)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "IntersectionArrow should return one combined relation")
		require.NotNil(rels[0].OptionalCaveat, "Result should have combined caveat")
		// The combination logic should combine both caveats with AND logic
	})

	t.Run("LeftCaveat_Right_NoCaveat", func(t *testing.T) {
		t.Parallel()

		// Left side relation with caveat
		leftRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("document", "doc1", "team"),
				Subject:  tuple.ONR("team", "team1", "..."),
			},
			OptionalCaveat: &core.ContextualizedCaveat{
				CaveatName: "left_caveat",
			},
		}

		// Right side relation with no caveat
		rightRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ONR("team", "team1", "member"),
				Subject:  tuple.ONR("user", "alice", "..."),
			},
			OptionalCaveat: nil,
		}

		leftIter := NewFixedIterator(leftRel)
		rightIter := NewFixedIterator(rightRel)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		relSeq, err := ctx.Check(intersectionArrow, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
		require.NoError(err)

		rels, err := CollectAll(relSeq)
		require.NoError(err)

		require.Len(rels, 1, "IntersectionArrow should return one relation")
		require.NotNil(rels[0].OptionalCaveat, "Left caveat should be preserved")
		require.Equal("left_caveat", rels[0].OptionalCaveat.CaveatName)
	})
}

func TestIntersectionArrowIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test iterators
	leftRel := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ONR("document", "doc1", "team"),
			Subject:  tuple.ONR("team", "team1", "..."),
		},
	}
	rightRel := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ONR("team", "team1", "member"),
			Subject:  tuple.ONR("user", "alice", "..."),
		},
	}

	leftIter := NewFixedIterator(leftRel)
	rightIter := NewFixedIterator(rightRel)
	original := NewIntersectionArrow(leftIter, rightIter)

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
	originalSeq, err := ctx.Check(original, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	originalResults, err := CollectAll(originalSeq)
	require.NoError(err)

	// Collect results from cloned iterator
	clonedSeq, err := ctx.Check(cloned, NewObjects("document", "doc1"), NewObject("user", "alice").WithEllipses())
	require.NoError(err)
	clonedResults, err := CollectAll(clonedSeq)
	require.NoError(err)

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
}

func TestIntersectionArrowIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	
	leftRel := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ONR("document", "doc1", "team"),
			Subject:  tuple.ONR("team", "team1", "..."),
		},
	}
	rightRel := tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ONR("team", "team1", "member"),
			Subject:  tuple.ONR("user", "alice", "..."),
		},
	}

	leftIter := NewFixedIterator(leftRel)
	rightIter := NewFixedIterator(rightRel)
	intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

	explain := intersectionArrow.Explain()
	require.Equal("IntersectionArrow", explain.Info)
	require.Len(explain.SubExplain, 2, "intersection arrow should have exactly 2 sub-explains (left and right)")

	explainStr := explain.String()
	require.Contains(explainStr, "IntersectionArrow")
	require.NotEmpty(explainStr)
}

func TestIntersectionArrowIteratorUnimplementedMethods(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	leftIter := NewFixedIterator()
	rightIter := NewFixedIterator()
	intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

	// Create context with LocalExecutor
	ctx := &Context{
		Context:  t.Context(),
		Executor: LocalExecutor{},
	}

	t.Run("IterSubjects_Unimplemented", func(t *testing.T) {
		t.Parallel()

		require.Panics(func() {
			_, _ = ctx.IterSubjects(intersectionArrow, NewObject("document", "doc1"))
		})
	})

	t.Run("IterResources_Unimplemented", func(t *testing.T) {
		t.Parallel()

		require.Panics(func() {
			_, _ = ctx.IterResources(intersectionArrow, NewObject("user", "alice").WithEllipses())
		})
	})
}
package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestIntersectionArrowIterator(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	t.Run("AllSubjectsSatisfyCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1 and team2
		leftPath1 := MustPathFromString("document:doc1#team@team:team1")
		leftPath2 := MustPathFromString("document:doc1#team@team:team2")

		// Right side: alice is member of both team1 and team2
		rightPath1 := MustPathFromString("team:team1#member@user:alice")
		rightPath2 := MustPathFromString("team:team2#member@user:alice")

		leftIter := NewFixedIterator(leftPath1, leftPath2)
		rightIter := NewFixedIterator(rightPath1, rightPath2)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.HeadRevision(context.Background())
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		// Test: alice should have access because she's a member of ALL teams (team1 and team2)
		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return a single result since alice is in ALL teams (intersection semantics)
		require.Len(paths, 1, "Should return single path representing the intersection")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")
	})

	t.Run("NotAllSubjectsSatisfyCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1 and team2
		leftPath1 := MustPathFromString("document:doc1#team@team:team1")
		leftPath2 := MustPathFromString("document:doc1#team@team:team2")

		// Right side: alice is member of team1 but NOT team2
		rightPath1 := MustPathFromString("team:team1#member@user:alice")
		// Note: no rightPath2 for team2, so alice is not in team2

		leftIter := NewFixedIterator(leftPath1, leftPath2)
		rightIter := NewFixedIterator(rightPath1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		// Test: alice should NOT have access because she's not a member of ALL teams
		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return empty since alice is not in ALL teams
		require.Empty(paths, "Should return no results since alice is not in all teams")
	})

	t.Run("SingleSubjectSatisfiesCondition", func(t *testing.T) {
		t.Parallel()

		// Left side: document has only team1
		leftPath1 := MustPathFromString("document:doc1#team@team:team1")

		// Right side: alice is member of team1
		rightPath1 := MustPathFromString("team:team1#member@user:alice")

		leftIter := NewFixedIterator(leftPath1)
		rightIter := NewFixedIterator(rightPath1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		// Test: alice should have access because she's a member of the only team
		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return result since alice is in the only team
		require.Len(paths, 1, "Should return one result since alice is in the single team")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")
	})

	t.Run("NoLeftSubjects", func(t *testing.T) {
		t.Parallel()

		// Left side: document has no teams
		leftIter := NewFixedIterator() // Empty

		// Right side: alice is member of some team
		rightPath1 := MustPathFromString("team:team1#member@user:alice")
		rightIter := NewFixedIterator(rightPath1)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return empty since there are no left subjects
		require.Empty(paths, "Should return no results when there are no left subjects")
	})

	t.Run("ThreeTeamsAllSatisfied", func(t *testing.T) {
		t.Parallel()

		// Left side: document has teams team1, team2, and team3
		leftPath1 := MustPathFromString("document:doc1#team@team:team1")
		leftPath2 := MustPathFromString("document:doc1#team@team:team2")
		leftPath3 := MustPathFromString("document:doc1#team@team:team3")

		// Right side: alice is member of all three teams
		rightPath1 := MustPathFromString("team:team1#member@user:alice")
		rightPath2 := MustPathFromString("team:team2#member@user:alice")
		rightPath3 := MustPathFromString("team:team3#member@user:alice")

		leftIter := NewFixedIterator(leftPath1, leftPath2, leftPath3)
		rightIter := NewFixedIterator(rightPath1, rightPath2, rightPath3)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		// Should return a single result representing the intersection of all three teams
		require.Len(paths, 1, "Should return single path representing the intersection")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")
	})

	t.Run("EmptyResources", func(t *testing.T) {
		t.Parallel()

		leftIter := NewFixedIterator()
		rightIter := NewFixedIterator()
		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		// Create test context
		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(err)

		ctx := &Context{
			Context:  context.Background(),
			Executor: LocalExecutor{},
			Reader:   ds.SnapshotReader(revision),
		}

		resources := []Object{}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)
		require.Empty(paths, "empty resource list should return no results")
	})
}

func TestIntersectionArrowIteratorCaveatCombination(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test context
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	t.Run("CombineTwoCaveats_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// Left side path with caveat
		leftPath := MustPathFromString("document:doc1#team@team:team1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		// Right side path with different caveat
		rightPath := MustPathFromString("team:team1#member@user:alice")
		rightPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "IntersectionArrow should return one combined path")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")

		// Verify caveat combination
		pathCaveat := paths[0].Caveat
		require.NotNil(pathCaveat, "Result should have combined caveat")
		require.NotNil(pathCaveat.GetOperation(), "Caveat should be an operation")
		require.Equal(pathCaveat.GetOperation().Op, core.CaveatOperation_AND, "Caveat should be an AND")
		require.Len(pathCaveat.GetOperation().GetChildren(), 2, "Caveat should be an AND of two children")
	})

	t.Run("LeftCaveat_Right_NoCaveat", func(t *testing.T) {
		t.Parallel()

		// Left side path with caveat
		leftPath := MustPathFromString("document:doc1#team@team:team1")
		leftPath.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat",
				},
			},
		}

		// Right side path with no caveat
		rightPath := MustPathFromString("team:team1#member@user:alice")
		rightPath.Caveat = nil

		leftIter := NewFixedIterator(leftPath)
		rightIter := NewFixedIterator(rightPath)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "IntersectionArrow should return one path")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")

		// Verify caveat preservation
		require.NotNil(paths[0].Caveat, "Left caveat should be preserved")
		require.Equal("left_caveat", paths[0].Caveat.GetCaveat().CaveatName)
	})

	t.Run("MultiplePaths_CombineCaveats_AND_Logic", func(t *testing.T) {
		t.Parallel()

		// Left side: document has multiple teams, each with different caveats
		leftPath1 := MustPathFromString("document:doc1#team@team:team1")
		leftPath1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat_1",
				},
			},
		}

		leftPath2 := MustPathFromString("document:doc1#team@team:team2")
		leftPath2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat_2",
				},
			},
		}

		leftPath3 := MustPathFromString("document:doc1#team@team:team3")
		leftPath3.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "left_caveat_3",
				},
			},
		}

		// Right side: alice is a member of all three teams, each with different caveats
		rightPath1 := MustPathFromString("team:team1#member@user:alice")
		rightPath1.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat_1",
				},
			},
		}

		rightPath2 := MustPathFromString("team:team2#member@user:alice")
		rightPath2.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat_2",
				},
			},
		}

		rightPath3 := MustPathFromString("team:team3#member@user:alice")
		rightPath3.Caveat = &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "right_caveat_3",
				},
			},
		}

		leftIter := NewFixedIterator(leftPath1, leftPath2, leftPath3)
		rightIter := NewFixedIterator(rightPath1, rightPath2, rightPath3)

		intersectionArrow := NewIntersectionArrow(leftIter, rightIter)

		resources := []Object{NewObject("document", "doc1")}
		subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

		pathSeq, err := ctx.Check(intersectionArrow, resources, subject)
		require.NoError(err)

		paths, err := CollectAll(pathSeq)
		require.NoError(err)

		require.Len(paths, 1, "IntersectionArrow should return one combined path for the intersection")

		// Verify the result path properties
		require.Equal("document", paths[0].Resource.ObjectType, "Resource type should match input")
		require.Equal("doc1", paths[0].Resource.ObjectID, "Resource ID should match input")
		require.Equal("", paths[0].Relation, "Relation should be empty after traversal")
		require.Equal("user", paths[0].Subject.ObjectType, "Subject type should match input")
		require.Equal("alice", paths[0].Subject.ObjectID, "Subject ID should match input")

		// Verify caveat combination
		require.NotNil(paths[0].Caveat, "Result should have combined caveat")

		// The result should have a complex caveat combining all left and right caveats with AND logic
		// Verify it's an AND operation
		caveatOp := paths[0].Caveat.GetOperation()
		require.NotNil(caveatOp, "Result caveat should be an operation")
		require.Equal(core.CaveatOperation_AND, caveatOp.Op, "Combined caveat should use AND operation")
		require.NotEmpty(caveatOp.Children, "Combined caveat should have children")
	})
}

func TestIntersectionArrowIteratorClone(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	// Create test paths
	leftPath := MustPathFromString("document:doc1#team@team:team1")
	rightPath := MustPathFromString("team:team1#member@user:alice")

	leftIter := NewFixedIterator(leftPath)
	rightIter := NewFixedIterator(rightPath)
	original := NewIntersectionArrow(leftIter, rightIter)

	cloned := original.Clone()
	require.NotSame(original, cloned, "cloned iterator should be a different object")

	// Both should have the same structure when explained
	originalExplain := original.Explain()
	clonedExplain := cloned.Explain()
	require.Equal(originalExplain.Info, clonedExplain.Info)
	require.Equal(len(originalExplain.SubExplain), len(clonedExplain.SubExplain))

	// Create test context
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
	}

	// Test that both iterators produce the same results
	resources := []Object{NewObject("document", "doc1")}
	subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice"}

	originalSeq, err := ctx.Check(original, resources, subject)
	require.NoError(err)
	originalResults, err := CollectAll(originalSeq)
	require.NoError(err)

	// Collect results from cloned iterator
	clonedSeq, err := ctx.Check(cloned, resources, subject)
	require.NoError(err)
	clonedResults, err := CollectAll(clonedSeq)
	require.NoError(err)

	// Both iterators should produce identical results
	require.Equal(originalResults, clonedResults, "original and cloned iterators should produce identical results")
}

func TestIntersectionArrowIteratorExplain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	leftPath := MustPathFromString("document:doc1#team@team:team1")
	rightPath := MustPathFromString("team:team1#member@user:alice")

	leftIter := NewFixedIterator(leftPath)
	rightIter := NewFixedIterator(rightPath)
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

	// Create test context
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return nil
	})
	require.NoError(err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(revision),
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
			_, _ = ctx.IterResources(intersectionArrow, ObjectAndRelation{ObjectType: "user", ObjectID: "alice"})
		})
	})
}

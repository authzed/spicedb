package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestRecursiveSentinel(t *testing.T) {
	sentinel := NewRecursiveSentinel("folder", "view", false)

	require.Equal(t, "folder", sentinel.DefinitionName())
	require.Equal(t, "view", sentinel.RelationName())
	require.Equal(t, false, sentinel.WithSubRelations())
	require.Equal(t, "folder#view:false", sentinel.ID())

	// Test that sentinel returns empty sequences
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(datastore.NoRevision),
	}

	// CheckImpl should return empty
	seq, err := sentinel.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)

	// IterSubjectsImpl should return empty
	seq, err = sentinel.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"})
	require.NoError(t, err)

	paths, err = CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)

	// Test Clone
	cloned := sentinel.Clone()
	require.Equal(t, sentinel.ID(), cloned.(*RecursiveSentinel).ID())
}

func TestRecursiveIteratorEmptyBaseCase(t *testing.T) {
	// Create a simple tree with sentinel that will return empty on depth 0
	sentinel := NewRecursiveSentinel("folder", "view", false)
	emptyIterator := NewEmptyFixedIterator()

	union := NewUnion()
	union.addSubIterator(emptyIterator)
	union.addSubIterator(sentinel)

	recursive := NewRecursiveIterator(union, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(datastore.NoRevision),
	}

	// Execute - should terminate immediately with empty result
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestReplaceSentinels(t *testing.T) {
	sentinel := NewRecursiveSentinel("folder", "view", false)

	// Create a tree with sentinel in various positions
	union := NewUnion()
	union.addSubIterator(NewEmptyFixedIterator())
	union.addSubIterator(sentinel)

	arrow := NewArrow(NewEmptyFixedIterator(), sentinel)
	union.addSubIterator(arrow)

	// Create a replacement tree
	replacementTree := NewEmptyFixedIterator()

	// Create a RecursiveIterator to access the replaceSentinelsInTree method
	recursive := NewRecursiveIterator(union, "folder", "view")

	// Replace sentinels with the tree
	result, err := recursive.replaceSentinelsInTree(union, replacementTree)
	require.NoError(t, err)

	// Verify sentinels were replaced
	resultUnion := result.(*Union)

	// Union's second child should now be FixedIterator
	_, isFixed := resultUnion.subIts[1].(*FixedIterator)
	require.True(t, isFixed, "Sentinel in union should be replaced with tree")

	// Arrow's right side should be FixedIterator
	arrowIter := resultUnion.subIts[2].(*Arrow)
	_, isFixed = arrowIter.right.(*FixedIterator)
	require.True(t, isFixed, "Sentinel in arrow should be replaced with tree")
}

func TestRecursiveIteratorClone(t *testing.T) {
	// Create a recursive iterator with a non-trivial tree
	sentinel := NewRecursiveSentinel("folder", "view", false)
	union := NewUnion()
	union.addSubIterator(NewEmptyFixedIterator())
	union.addSubIterator(sentinel)

	recursive := NewRecursiveIterator(union, "folder", "view")

	// Clone it
	cloned := recursive.Clone()

	// Verify it's a different instance
	require.NotSame(t, recursive, cloned)

	// Verify the structure is the same
	clonedRecursive := cloned.(*RecursiveIterator)
	require.NotNil(t, clonedRecursive.templateTree)

	// Verify the cloned tree is also different instances
	require.NotSame(t, recursive.templateTree, clonedRecursive.templateTree)

	// But the structure should be equivalent
	require.Equal(t, recursive.Explain().Name, clonedRecursive.Explain().Name)
}

func TestRecursiveIteratorSubiteratorsAndReplace(t *testing.T) {
	// Create a recursive iterator
	sentinel := NewRecursiveSentinel("folder", "view", false)
	union := NewUnion()
	union.addSubIterator(NewEmptyFixedIterator())
	union.addSubIterator(sentinel)

	recursive := NewRecursiveIterator(union, "folder", "view")

	// Test Subiterators
	subs := recursive.Subiterators()
	require.Len(t, subs, 1)
	require.Same(t, union, subs[0])

	// Test ReplaceSubiterators
	newTree := NewEmptyFixedIterator()
	replaced, err := recursive.ReplaceSubiterators([]Iterator{newTree})
	require.NoError(t, err)

	// Verify the replacement worked
	replacedRecursive := replaced.(*RecursiveIterator)
	require.Same(t, newTree, replacedRecursive.templateTree)

	// Verify original is unchanged
	require.Same(t, union, recursive.templateTree)
}

func TestRecursiveIteratorExecutionError(t *testing.T) {
	// Test error path: execute() returns an error (line 67-70 in iterativeDeepening)
	// This tests when CheckImpl/IterSubjects/IterResources fails during execution

	faultyIter := NewFaultyIterator(true, false) // Fails on Check
	recursive := NewRecursiveIterator(faultyIter, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(datastore.NoRevision),
	}

	// Test CheckImpl with a faulty iterator
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err, "CheckImpl should return sequence without error")

	// Error should occur during sequence iteration
	paths, err := CollectAll(seq)
	require.Error(t, err, "Should get error from faulty iterator during execution")
	require.Contains(t, err.Error(), "execution failed at depth", "Error should be wrapped with depth info")
	require.Empty(t, paths)
}

func TestRecursiveIteratorCollectionError(t *testing.T) {
	// Test error path: pathSeq yields an error during iteration (line 78-80 in iterativeDeepening)
	// This tests when the returned PathSeq fails during collection

	faultyIter := NewFaultyIterator(false, true) // Fails on collection
	recursive := NewRecursiveIterator(faultyIter, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := &Context{
		Context:  context.Background(),
		Executor: LocalExecutor{},
		Reader:   ds.SnapshotReader(datastore.NoRevision),
	}

	// Test CheckImpl with a faulty iterator that fails on collection
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err, "CheckImpl should return sequence without error")

	// Error should occur during sequence iteration (collection)
	paths, err := CollectAll(seq)
	require.Error(t, err, "Should get error during path collection")
	require.Contains(t, err.Error(), "faulty iterator collection error", "Should get collection error")
	require.Empty(t, paths)
}

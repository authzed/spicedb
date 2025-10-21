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
		Context:   context.Background(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  datastore.NoRevision,
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

	recursive := NewRecursiveIterator(union, []*RecursiveSentinel{sentinel})

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := &Context{
		Context:   context.Background(),
		Executor:  LocalExecutor{},
		Datastore: ds,
		Revision:  datastore.NoRevision,
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

	// Create a RecursiveIterator to use replaceSentinelsWithTree
	recursive := NewRecursiveIterator(union, []*RecursiveSentinel{sentinel})

	// Replace sentinels with the tree
	result := recursive.replaceSentinelsWithTree(union, replacementTree)

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

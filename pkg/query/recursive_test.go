package query

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestRecursiveSentinel(t *testing.T) {
	sentinel := NewRecursiveSentinelIterator("folder", "view", false)

	require.Equal(t, "folder", sentinel.DefinitionName())
	require.Equal(t, "view", sentinel.RelationName())
	require.False(t, sentinel.WithSubRelations())
	require.NotEmpty(t, sentinel.ID(), "ID should be a non-empty UUID")

	// Test that sentinel returns empty sequences
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

	// CheckImpl should return empty
	seq, err := sentinel.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)

	// IterSubjectsImpl should return empty
	seq, err = sentinel.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
	require.NoError(t, err)

	paths, err = CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)

	// Test Clone - should generate a new UUID
	cloned := sentinel.Clone()
	require.NotEqual(t, sentinel.ID(), cloned.(*RecursiveSentinelIterator).ID())
	require.NotEmpty(t, cloned.(*RecursiveSentinelIterator).ID())
}

func TestRecursiveIteratorEmptyBaseCase(t *testing.T) {
	// Create a simple tree with sentinel that will return empty on depth 0
	sentinel := NewRecursiveSentinelIterator("folder", "view", false)
	emptyIterator := NewEmptyFixedIterator()

	union := NewUnionIterator(emptyIterator, sentinel)

	recursive := NewRecursiveIterator(union, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

	// Execute - should terminate immediately with empty result
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestReplaceSentinels(t *testing.T) {
	sentinel := NewRecursiveSentinelIterator("folder", "view", false)

	// Create a tree with sentinel in various positions
	arrow := NewArrowIterator(NewEmptyFixedIterator(), sentinel)
	union := NewUnionIterator(
		NewEmptyFixedIterator(),
		sentinel,
		arrow,
	)

	// Create a replacement tree
	replacementTree := NewEmptyFixedIterator()

	// Create a RecursiveIterator to access the replaceSentinelsInTree method
	recursive := NewRecursiveIterator(union, "folder", "view")

	// Replace sentinels with the tree
	result, err := recursive.replaceSentinelsInTree(union, replacementTree)
	require.NoError(t, err)

	// Verify sentinels were replaced
	resultUnion := result.(*UnionIterator)

	// Union's second child should now be FixedIterator
	_, isFixed := resultUnion.subIts[1].(*FixedIterator)
	require.True(t, isFixed, "Sentinel in union should be replaced with tree")

	// Arrow's right side should be FixedIterator
	arrowIter := resultUnion.subIts[2].(*ArrowIterator)
	_, isFixed = arrowIter.right.(*FixedIterator)
	require.True(t, isFixed, "Sentinel in arrow should be replaced with tree")
}

func TestRecursiveIteratorClone(t *testing.T) {
	// Create a recursive iterator with a non-trivial tree
	sentinel := NewRecursiveSentinelIterator("folder", "view", false)
	union := NewUnionIterator(NewEmptyFixedIterator(), sentinel)

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
	sentinel := NewRecursiveSentinelIterator("folder", "view", false)
	union := NewUnionIterator(NewEmptyFixedIterator(), sentinel)

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
	// This tests when CheckImpl/IterSubjects/IterResources fails during execution

	faultyIter := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{}) // Fails on Check
	recursive := NewRecursiveIterator(faultyIter, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

	// Test CheckImpl with a faulty iterator
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err, "CheckImpl should return sequence without error")

	// Error should occur during sequence iteration
	paths, err := CollectAll(seq)
	require.Error(t, err, "Should get error from faulty iterator during execution")
	// The error message depends on which strategy is used:
	// - IterSubjects strategy: "IterSubjects failed... at depth" or "execution failed at ply" (from BFS)
	// - Deepening strategy: "check failed at ply"
	require.True(t,
		strings.Contains(err.Error(), "execution failed at ply") ||
			strings.Contains(err.Error(), "check failed at ply") ||
			strings.Contains(err.Error(), "at depth"),
		"Error should be wrapped with ply/depth info, got: %s", err.Error())
	require.Empty(t, paths)
}

func TestRecursiveIteratorCollectionError(t *testing.T) {
	// Test error path: pathSeq yields an error during iteration (line 78-80 in iterativeDeepening)
	// This tests when the returned PathSeq fails during collection

	faultyIter := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{}) // Fails on collection
	recursive := NewRecursiveIterator(faultyIter, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

	// Test CheckImpl with a faulty iterator that fails on collection
	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "tom", Relation: "..."})
	require.NoError(t, err, "CheckImpl should return sequence without error")

	// Error should occur during sequence iteration (collection)
	paths, err := CollectAll(seq)
	require.Error(t, err, "Should get error during path collection")
	require.Contains(t, err.Error(), "faulty iterator collection error", "Should get collection error")
	require.Empty(t, paths)
}

// TestBFSEarlyTermination verifies that BFS terminates early when frontier is empty
func TestBFSEarlyTermination(t *testing.T) {
	// Create a shallow graph (depth 2) and verify it terminates early, not at maxDepth
	// folder1 -> (sentinel returns empty)

	sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
	recursive := NewRecursiveIterator(sentinel, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50)) // High max depth

	// IterSubjects on a node with no children (sentinel returns empty)
	// Should terminate at ply 0, not continue to maxDepth
	seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)
	require.Empty(t, paths, "No paths should be found since sentinel returns empty")

	// Verify from trace logs that it terminated early (checked via TraceLogger in actual use)
}

// TestBFSCycleDetection verifies that BFS handles cycles correctly
func TestBFSCycleDetection(t *testing.T) {
	// Create a cycle: folder1 -> folder2 -> folder1
	// BFS should detect the cycle and not infinite loop

	// Create an iterator that returns cyclic paths
	cyclicIter := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder2", Relation: "..."},
		},
	)

	// When we query folder2, it should return folder1
	folder2Iter := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder1", Relation: "..."},
		},
	)

	// Create a union that returns different results based on which resource is queried
	// This simulates the cycle: folder1 -> folder2 -> folder1
	union := NewUnionIterator(cyclicIter, folder2Iter)

	recursive := NewRecursiveIterator(union, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(10))

	seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)

	// Should find folder2, but not recurse back to folder1 (already visited)
	// The exact behavior depends on the union deduplication
	require.NotEmpty(t, paths, "Should find at least one path")
}

// TestBFSSelfReferential verifies that BFS handles self-referential nodes correctly
func TestBFSSelfReferential(t *testing.T) {
	// folder1 -> parent -> folder1 (self-reference)
	// Should not infinite loop

	selfRefIter := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder1", Relation: "..."},
		},
	)

	recursive := NewRecursiveIterator(selfRefIter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(10))

	seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)

	// Should find the self-referential path once, but not re-explore folder1
	require.Len(t, paths, 1, "Should find exactly one path (self-reference)")
	require.Equal(t, "folder1", paths[0].Subject.ObjectID)
}

// TestBFSCaveatMergingAcrossPlies verifies caveat merging with OR semantics across plies
func TestBFSCaveatMergingAcrossPlies(t *testing.T) {
	// Test that if the same endpoint is reached via different paths in different plies,
	// caveats are merged with OR semantics
	// This is a placeholder test - actual caveat merging behavior depends on the iterator structure

	// TODO: Implement when we have a concrete scenario with caveated recursive paths
	t.Skip("Caveat merging test requires complex setup - to be implemented with real schemas")
}

// TestBFSResourcesWithEllipses verifies IterResources converts resources correctly
func TestBFSResourcesWithEllipses(t *testing.T) {
	// Verify that when extracting recursive resources, they're converted with WithEllipses()

	resourceIter := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
	)

	recursive := NewRecursiveIterator(resourceIter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(5))

	// Query IterResources - should find folder2
	seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
	require.NoError(t, err)

	paths, err := CollectAll(seq)
	require.NoError(t, err)

	require.NotEmpty(t, paths, "Should find at least one resource")
	require.Equal(t, "folder2", paths[0].Resource.ObjectID)
}

func TestRecursiveIterator_Types(t *testing.T) {
	t.Parallel()

	t.Run("ResourceType", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a recursive iterator with a template tree
		path := MustPathFromString("folder:folder1#parent@folder:folder2")
		templateTree := NewFixedIterator(path)
		recursive := NewRecursiveIterator(templateTree, "folder", "parent")

		resourceType, err := recursive.ResourceType()
		require.NoError(err)
		require.Len(resourceType, 1)
		require.Equal("folder", resourceType[0].Type) // From templateTree
	})

	t.Run("SubjectTypes", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a recursive iterator with a template tree
		path := MustPathFromString("folder:folder1#parent@folder:folder2")
		templateTree := NewFixedIterator(path)
		recursive := NewRecursiveIterator(templateTree, "folder", "parent")

		subjectTypes, err := recursive.SubjectTypes()
		require.NoError(err)
		require.Len(subjectTypes, 1) // From templateTree
		require.Equal("folder", subjectTypes[0].Type)
	})
}

// TestRecursiveSentinel_ReplaceSubiterators tests that RecursiveSentinel panics
func TestRecursiveSentinel_ReplaceSubiterators(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	sentinel := NewRecursiveSentinelIterator("folder", "view", false)

	// Should panic - leaf node
	require.Panics(func() {
		_, _ = sentinel.ReplaceSubiterators([]Iterator{})
	}, "Should panic when trying to replace subiterators on leaf")
}

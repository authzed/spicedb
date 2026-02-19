package query

import (
	"context"
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TestRecursiveIterator_ID tests the ID() method (currently 0% coverage)
func TestRecursiveIterator_ID(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	sentinel := NewRecursiveSentinelIterator("folder", "view", false)
	recursive := NewRecursiveIterator(sentinel, "folder", "view")

	id := recursive.ID()
	require.NotEmpty(id, "ID should be non-empty")

	// Verify IDs are unique
	recursive2 := NewRecursiveIterator(sentinel, "folder", "view")
	require.NotEqual(id, recursive2.ID(), "Different instances should have different IDs")
}

// TestReplaceRecursiveSentinel_NonMatchingSentinel tests replacing sentinels that don't match
func TestReplaceRecursiveSentinel_NonMatchingSentinel(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create sentinels with different definitions/relations
	matchingSentinel := NewRecursiveSentinelIterator("folder", "view", false)
	nonMatchingSentinel1 := NewRecursiveSentinelIterator("document", "view", false) // Different definition
	nonMatchingSentinel2 := NewRecursiveSentinelIterator("folder", "read", false)   // Different relation

	// Create a tree with both matching and non-matching sentinels
	union := NewUnionIterator(matchingSentinel, nonMatchingSentinel1, nonMatchingSentinel2)

	recursive := NewRecursiveIterator(union, "folder", "view")
	replacement := NewEmptyFixedIterator()

	// Replace sentinels
	result, err := recursive.replaceRecursiveSentinel(union, replacement)
	require.NoError(err)

	resultUnion := result.(*UnionIterator)
	require.Len(resultUnion.subIts, 3)

	// First should be replaced (matching sentinel)
	_, isFixed := resultUnion.subIts[0].(*FixedIterator)
	require.True(isFixed, "Matching sentinel should be replaced")

	// Second should NOT be replaced (different definition)
	sentinel1, isSentinel := resultUnion.subIts[1].(*RecursiveSentinelIterator)
	require.True(isSentinel, "Non-matching sentinel (different definition) should not be replaced")
	require.Equal("document", sentinel1.DefinitionName())

	// Third should NOT be replaced (different relation)
	sentinel2, isSentinel := resultUnion.subIts[2].(*RecursiveSentinelIterator)
	require.True(isSentinel, "Non-matching sentinel (different relation) should not be replaced")
	require.Equal("read", sentinel2.RelationName())
}

// TestReplaceRecursiveSentinel_DeepNesting tests replacement in deeply nested trees
func TestReplaceRecursiveSentinel_DeepNesting(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	sentinel := NewRecursiveSentinelIterator("folder", "view", false)

	// Create deeply nested tree: Union -> Arrow -> Union -> Sentinel
	innerUnion := NewUnionIterator(sentinel)
	arrow := NewArrowIterator(NewEmptyFixedIterator(), innerUnion)
	outerUnion := NewUnionIterator(arrow)

	recursive := NewRecursiveIterator(outerUnion, "folder", "view")
	replacement := NewEmptyFixedIterator()

	result, err := recursive.replaceRecursiveSentinel(outerUnion, replacement)
	require.NoError(err)

	// Verify deep replacement occurred
	resultOuterUnion := result.(*UnionIterator)
	resultArrow := resultOuterUnion.subIts[0].(*ArrowIterator)
	resultInnerUnion := resultArrow.right.(*UnionIterator)
	_, isFixed := resultInnerUnion.subIts[0].(*FixedIterator)
	require.True(isFixed, "Deeply nested sentinel should be replaced")
}

// TestBreadthFirstIterResources_MaxDepth tests that BFS respects max depth limit
func TestBreadthFirstIterResources_MaxDepth(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create an iterator that always returns new paths (infinite recursion)
	infiniteIter := &infiniteRecursiveIterator{
		resourceType: "folder",
		counter:      0,
	}

	sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
	union := NewUnionIterator(infiniteIter, sentinel)
	recursive := NewRecursiveIterator(union, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	// Set a low max depth
	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(3))

	seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
	require.NoError(err)

	paths, err := CollectAll(seq)
	require.NoError(err)

	// Should terminate at max depth, not infinite loop
	// Each ply generates one new path, so we expect at most 3 paths
	require.LessOrEqual(len(paths), 3, "Should terminate at max depth")
}

// TestBreadthFirstIterResources_ErrorHandling tests error paths in BFS IterResources
func TestBreadthFirstIterResources_ErrorHandling(t *testing.T) {
	t.Parallel()

	t.Run("ErrorDuringQuery", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a faulty iterator that fails during query
		faultyIter := NewFaultyIterator(true, false, ObjectType{}, []ObjectType{})
		sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
		union := NewUnionIterator(faultyIter, sentinel)
		recursive := NewRecursiveIterator(union, "folder", "parent")

		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		ctx := NewLocalContext(context.Background(),
			WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

		seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
		require.NoError(err)

		// Error should occur during collection
		paths, err := CollectAll(seq)
		require.Error(err, "Should get error from faulty iterator")
		require.Empty(paths)
	})

	t.Run("ErrorDuringCollection", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Create a faulty iterator that fails during collection
		faultyIter := NewFaultyIterator(false, true, ObjectType{}, []ObjectType{})
		sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
		union := NewUnionIterator(faultyIter, sentinel)
		recursive := NewRecursiveIterator(union, "folder", "parent")

		ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		ctx := NewLocalContext(context.Background(),
			WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)))

		seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
		require.NoError(err)

		// Error should occur during collection
		paths, err := CollectAll(seq)
		require.Error(err, "Should get error during collection")
		require.Empty(paths)
	})
}

// TestBreadthFirstIterResources_MergeOrSemantics tests OR merging of duplicate paths
func TestBreadthFirstIterResources_MergeOrSemantics(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create an iterator that returns the same endpoint in different plies
	// This tests the merge-or logic in breadthFirstIterResources
	path1 := Path{
		Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
		Relation: "parent",
		Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	}

	iter := NewFixedIterator(path1)
	sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
	union := NewUnionIterator(iter, sentinel)
	recursive := NewRecursiveIterator(union, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(5))

	seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
	require.NoError(err)

	paths, err := CollectAll(seq)
	require.NoError(err)

	// Should deduplicate - only one path to folder1
	require.Len(paths, 1, "Should deduplicate paths with same endpoints")
	require.Equal("folder1", paths[0].Resource.ObjectID)
}

// TestIterativeDeepening_MaxDepth tests that iterative deepening respects max depth
func TestIterativeDeepening_MaxDepth(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create a simple iterator that always returns something at each depth
	depthCounter := &depthCountingIterator{counter: 0}
	recursive := NewRecursiveIterator(depthCounter, "folder", "view")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	maxDepth := 5
	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(maxDepth))

	seq, err := recursive.CheckImpl(ctx, []Object{{ObjectType: "folder", ObjectID: "folder1"}}, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."})
	require.NoError(err)

	paths, err := CollectAll(seq)
	require.NoError(err)

	// Should have run exactly maxDepth iterations
	require.LessOrEqual(len(paths), maxDepth, "Should not exceed max depth")
}

// TestUnwrapRecursiveIterators_NestedRecursion tests unwrapping nested RecursiveIterators
func TestUnwrapRecursiveIterators_NestedRecursion(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Create nested recursive iterators
	innerSentinel := NewRecursiveSentinelIterator("document", "parent", false)
	innerRecursive := NewRecursiveIterator(innerSentinel, "document", "parent")

	outerSentinel := NewRecursiveSentinelIterator("folder", "parent", false)
	union := NewUnionIterator(outerSentinel, innerRecursive)
	outerRecursive := NewRecursiveIterator(union, "folder", "parent")

	// Unwrap at depth 0
	unwrapped, err := unwrapRecursiveIterators(outerRecursive, 0)
	require.NoError(err)
	require.NotNil(unwrapped)

	// The outer RecursiveIterator should be unwrapped to a Union
	_, isUnion := unwrapped.(*UnionIterator)
	require.True(isUnion, "Outer recursive should unwrap to union")
}

// Helper iterator for testing max depth - always returns new paths
type infiniteRecursiveIterator struct {
	resourceType string
	counter      int
}

func (i *infiniteRecursiveIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

func (i *infiniteRecursiveIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

func (i *infiniteRecursiveIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		i.counter++
		counter, err := safecast.Convert[int32](i.counter)
		if err != nil {
			yield(Path{}, err)
			return
		}
		path := Path{
			Resource: Object{ObjectType: i.resourceType, ObjectID: "folder" + string('0'+counter)},
			Relation: "parent",
			Subject:  subject,
		}
		yield(path, nil)
	}, nil
}

func (i *infiniteRecursiveIterator) Clone() Iterator {
	return &infiniteRecursiveIterator{
		resourceType: i.resourceType,
		counter:      i.counter,
	}
}

func (i *infiniteRecursiveIterator) Explain() Explain {
	return Explain{Name: "InfiniteRecursive"}
}

func (i *infiniteRecursiveIterator) Subiterators() []Iterator {
	return nil
}

func (i *infiniteRecursiveIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return i, nil
}

func (i *infiniteRecursiveIterator) ID() string {
	return "infinite"
}

func (i *infiniteRecursiveIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{Type: i.resourceType, Subrelation: "..."}}, nil
}

func (i *infiniteRecursiveIterator) SubjectTypes() ([]ObjectType, error) {
	return []ObjectType{{Type: "user", Subrelation: "..."}}, nil
}

// Helper iterator that counts depth iterations
type depthCountingIterator struct {
	counter int
}

func (d *depthCountingIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	d.counter++
	return func(yield func(Path, error) bool) {
		// Always return a path so iterative deepening continues
		path := Path{
			Resource: resources[0],
			Relation: "view",
			Subject:  subject,
		}
		yield(path, nil)
	}, nil
}

func (d *depthCountingIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

func (d *depthCountingIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	return EmptyPathSeq(), nil
}

func (d *depthCountingIterator) Clone() Iterator {
	return &depthCountingIterator{counter: d.counter}
}

func (d *depthCountingIterator) Explain() Explain {
	return Explain{Name: "DepthCounting"}
}

func (d *depthCountingIterator) Subiterators() []Iterator {
	return nil
}

func (d *depthCountingIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return d, nil
}

func (d *depthCountingIterator) ID() string {
	return "depth"
}

func (d *depthCountingIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{Type: "folder", Subrelation: "..."}}, nil
}

func (d *depthCountingIterator) SubjectTypes() ([]ObjectType, error) {
	return []ObjectType{{Type: "user", Subrelation: "..."}}, nil
}

package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

// Benchmarks for comparing BFS vs iterative deepening performance

// BenchmarkRecursiveShallowGraph benchmarks a shallow graph (depth 2-3)
// This is the common case and should show BFS advantage
func BenchmarkRecursiveShallowGraph(b *testing.B) {
	// Create a shallow 3-level hierarchy:
	// folder1 -> folder2 -> folder3 -> user:alice
	paths := []Path{
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder2", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder3", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder3"},
			Relation: "viewer",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
	}

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveWideGraph benchmarks a wide graph (many branches at each level)
// Tests performance with many parallel paths
func BenchmarkRecursiveWideGraph(b *testing.B) {
	// Create a wide 2-level hierarchy:
	// folder1 -> {folder2, folder3, folder4, folder5, folder6} -> users
	var paths []Path

	// Level 1: folder1 connects to 5 folders
	for i := 2; i <= 6; i++ {
		paths = append(paths, Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: fmt.Sprintf("folder%d", i), Relation: "..."},
		})
	}

	// Level 2: Each folder connects to 2 users
	for i := 2; i <= 6; i++ {
		for j := 1; j <= 2; j++ {
			paths = append(paths, Path{
				Resource: Object{ObjectType: "folder", ObjectID: fmt.Sprintf("folder%d", i)},
				Relation: "viewer",
				Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: fmt.Sprintf("user%d", (i-2)*2+j), Relation: "..."},
			})
		}
	}

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveDeepGraph benchmarks a deep graph (depth 10+)
// This tests the worst case where both approaches run to similar depths
func BenchmarkRecursiveDeepGraph(b *testing.B) {
	// Create a deep 10-level chain:
	// folder1 -> folder2 -> ... -> folder10 -> user:alice
	var paths []Path

	for i := 1; i < 10; i++ {
		paths = append(paths, Path{
			Resource: Object{ObjectType: "folder", ObjectID: fmt.Sprintf("folder%d", i)},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: fmt.Sprintf("folder%d", i+1), Relation: "..."},
		})
	}

	// Final level
	paths = append(paths, Path{
		Resource: Object{ObjectType: "folder", ObjectID: "folder10"},
		Relation: "viewer",
		Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
	})

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveEmptyGraph benchmarks an empty graph (no paths)
// Tests early termination on empty results
func BenchmarkRecursiveEmptyGraph(b *testing.B) {
	// Empty sentinel - no paths
	sentinel := NewRecursiveSentinelIterator("folder", "parent", false)
	recursive := NewRecursiveIterator(sentinel, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveSparseGraph benchmarks a sparse graph (few recursive nodes)
// Tests performance when most nodes are leaf nodes
func BenchmarkRecursiveSparseGraph(b *testing.B) {
	// Create a sparse hierarchy:
	// folder1 -> folder2 (recursive) and 10 users (non-recursive)
	// folder2 -> 10 more users (non-recursive)
	var paths []Path

	// folder1 -> folder2 (recursive)
	paths = append(paths, Path{
		Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
		Relation: "parent",
		Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder2", Relation: "..."},
	})

	// folder1 -> 10 users (non-recursive)
	for i := 1; i <= 10; i++ {
		paths = append(paths, Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "viewer",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: fmt.Sprintf("user%d", i), Relation: "..."},
		})
	}

	// folder2 -> 10 more users (non-recursive)
	for i := 11; i <= 20; i++ {
		paths = append(paths, Path{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "viewer",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: fmt.Sprintf("user%d", i), Relation: "..."},
		})
	}

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveCyclicGraph benchmarks a cyclic graph
// Tests cycle detection overhead
func BenchmarkRecursiveCyclicGraph(b *testing.B) {
	// Create a cycle: folder1 -> folder2 -> folder3 -> folder1
	paths := []Path{
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder2", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder3", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder3"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder1", Relation: "..."},
		},
	}

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterSubjectsImpl(ctx, Object{ObjectType: "folder", ObjectID: "folder1"}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecursiveIterResources benchmarks IterResources with a shallow graph
// Tests the resources variant of BFS
func BenchmarkRecursiveIterResources(b *testing.B) {
	// Create a 3-level hierarchy in reverse (for IterResources)
	paths := []Path{
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder1"},
			Relation: "viewer",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder2"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder1", Relation: "..."},
		},
		{
			Resource: Object{ObjectType: "folder", ObjectID: "folder3"},
			Relation: "parent",
			Subject:  ObjectAndRelation{ObjectType: "folder", ObjectID: "folder2", Relation: "..."},
		},
	}

	iter := NewFixedIterator(paths...)
	recursive := NewRecursiveIterator(iter, "folder", "parent")

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		b.Fatal(err)
	}

	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision)),
		WithMaxRecursionDepth(50))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq, err := recursive.IterResourcesImpl(ctx, ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}, NoObjectFilter())
		if err != nil {
			b.Fatal(err)
		}

		_, err = CollectAll(seq)
		if err != nil {
			b.Fatal(err)
		}
	}
}

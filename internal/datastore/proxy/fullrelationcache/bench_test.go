package fullrelationcache_test

import (
	"fmt"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	benchViewersPerObject = 400
	benchSeededObjects    = 64
)

// newBenchProxy builds a proxy over documents doc-0..doc-63, each with viewers user-0..user-399.
func newBenchProxy(b *testing.B, threshold, maxSize uint64) (*countingDatastore, datastore.Datastore, datastore.Revision) {
	return newProxyWithRels(b, threshold, maxSize, func(raw datastore.Datastore) datastore.Revision {
		rels := make([]string, 0, benchSeededObjects*benchViewersPerObject)
		for obj := range benchSeededObjects {
			for i := range benchViewersPerObject {
				rels = append(rels, fmt.Sprintf("document:doc-%d#viewer@user:user-%d", obj, i))
			}
		}
		return writeRels(b, raw, rels...)
	})
}

// BenchmarkFullRelationCacheWorkloads reports delegate queries per API query.
// Query i targets object i%objects and subject i%subjects.
func BenchmarkFullRelationCacheWorkloads(b *testing.B) {
	for _, tc := range []struct {
		name string

		// objects is the number of distinct objects. A value of 0 gives a new object for each query.
		objects  int
		subjects int
	}{
		// With one hot object, both cases converge to about 0 after the set materializes.
		{"hot-object/subject-diverse", 1, 500},
		{"hot-object/subject-repetitive", 1, 1},
		// Each object costs T_materialize delegate queries before the cache serves it.
		{"warm-objects-64", benchSeededObjects, 500},
		// No object reaches the threshold, so every query goes to the delegate.
		{"cold-objects", 0, 500},
	} {
		b.Run(tc.name, func(b *testing.B) {
			counting, ds, rev := newBenchProxy(b, 3 /* T_materialize */, 1024)
			r := ds.SnapshotReader(rev)
			ops := 0
			for i := 0; b.Loop(); i++ {
				obj := i
				if tc.objects > 0 {
					obj = i % tc.objects
				}
				queryViewer(b, r, fmt.Sprintf("doc-%d", obj), fmt.Sprintf("user-%d", i%tc.subjects))
				ops++
			}
			b.ReportMetric(float64(counting.queries.Load())/float64(ops), "delegate-queries/op")
		})
	}
}

const (
	benchWideObjects    = 100
	benchWideSetRows    = 500
	benchWideSeededRels = benchWideObjects * benchWideSetRows
)

// BenchmarkServeWideQuery serves one 100-ID query from 100 warm 500-row sets.
// No query reaches the delegate.
func BenchmarkServeWideQuery(b *testing.B) {
	counting, ds, rev := newProxyWithRels(b, 1, 1024, func(raw datastore.Datastore) datastore.Revision {
		rels := make([]string, 0, benchWideSeededRels)
		for obj := range benchWideObjects {
			for i := range benchWideSetRows {
				rels = append(rels, fmt.Sprintf("document:wide-%d#viewer@user:user-%d", obj, i))
			}
		}
		return writeRels(b, raw, rels...)
	})
	r := ds.SnapshotReader(rev)
	ids := make([]string, benchWideObjects)
	for i := range ids {
		ids[i] = fmt.Sprintf("wide-%d", i)
	}
	query := func() int {
		it, err := r.QueryRelationships(b.Context(), datastore.RelationshipsFilter{
			OptionalResourceType: "document", OptionalResourceIds: ids,
			OptionalResourceRelation: "viewer",
			OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
				OptionalSubjectType: "user", OptionalSubjectIds: []string{"user-7"},
				RelationFilter: datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			}},
		})
		if err != nil {
			b.Fatal(err)
		}
		rels, err := datastore.IteratorToSlice(it)
		if err != nil {
			b.Fatal(err)
		}
		return len(rels)
	}
	// This first query materializes all sets.
	if got := query(); got != benchWideObjects {
		b.Fatalf("expected %d rels, got %d", benchWideObjects, got)
	}
	counting.reset()
	b.ReportAllocs()
	for b.Loop() {
		query()
	}
	if counting.queries.Load() != 0 {
		b.Fatalf("expected every query served from sets, got %d delegate queries", counting.queries.Load())
	}
}

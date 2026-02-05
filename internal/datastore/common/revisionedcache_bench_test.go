package common

import (
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// BenchmarkRevisionedCache_ParallelGet benchmarks parallel cache gets with the current implementation
func BenchmarkRevisionedCache_ParallelGet(b *testing.B) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-populate cache with a single schema range
	rev := revisions.NewHLCForTime(baseTime)
	schema := createTestSchema("hash1")
	_ = cache.Set(rev, schema, "hash1")
	cache.Flush()

	// Update checkpoint to cover a wider range (same hash, so just updates checkpoint)
	futureRev := revisions.NewHLCForTime(baseTime.Add(1000 * time.Second))
	_ = cache.Set(futureRev, schema, "hash1")
	cache.Flush()

	// Create revision to benchmark - same as initial revision to ensure it's in range
	benchRev := rev

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := cache.Get(benchRev)
			if result == nil {
				b.Fatal("expected non-nil result")
			}
		}
	})
}

// BenchmarkRevisionedCache_ParallelGetMiss benchmarks parallel cache misses
func BenchmarkRevisionedCache_ParallelGetMiss(b *testing.B) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-populate cache with one schema
	rev := revisions.NewHLCForTime(baseTime)
	schema := createTestSchema("hash1")
	_ = cache.Set(rev, schema, "hash1")
	cache.Flush()

	// Create revision that will miss - way in the future
	missRev := revisions.NewHLCForTime(baseTime.Add(1000 * time.Hour))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := cache.Get(missRev)
			if result != nil {
				b.Fatal("expected nil result for cache miss")
			}
		}
	})
}

// BenchmarkRevisionedCache_ParallelGetWithHistorical benchmarks with historical ranges
func BenchmarkRevisionedCache_ParallelGetWithHistorical(b *testing.B) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create multiple historical ranges by changing schema hash
	for i := 0; i < 10; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		hashNum := (i / 2) + 1 // Change hash every 2 revisions
		schema := createTestSchema("hash" + string(rune('0'+hashNum)))
		_ = cache.Set(rev, schema, schema.GetV1().SchemaHash)
		cache.Flush() // Ensure each schema change creates a new range
	}

	// Benchmark accessing a revision from the middle historical range
	benchRev := revisions.NewHLCForTime(baseTime.Add(5 * time.Minute))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := cache.Get(benchRev)
			if result == nil {
				b.Fatal("expected non-nil result")
			}
		}
	})
}

// BenchmarkRevisionedCache_MixedReadWrite benchmarks mixed read/write workload
func BenchmarkRevisionedCache_MixedReadWrite(b *testing.B) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-populate with initial schema
	rev := revisions.NewHLCForTime(baseTime)
	schema := createTestSchema("hash1")
	_ = cache.Set(rev, schema, "hash1")
	cache.Flush()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			// 90% reads, 10% writes
			if i%10 == 0 {
				newRev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Millisecond))
				_ = cache.Set(newRev, schema, "hash1")
			} else {
				result := cache.Get(rev)
				if result == nil {
					b.Fatal("expected non-nil result")
				}
			}
		}
	})
}

package common

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Helper function to create a test schema
func createTestSchema(hash string) *core.StoredSchema {
	return &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaHash: hash,
				SchemaText: "test schema",
			},
		},
	}
}

func TestRevisionedCache_EmptyCache(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Get from empty cache should return nil
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rev := revisions.NewHLCForTime(baseTime)
	result := cache.Get(rev)
	require.Nil(t, result)
}

func TestRevisionedCache_BasicSetAndGet(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Use deterministic time
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rev := revisions.NewHLCForTime(baseTime)
	schema := createTestSchema("hash1")
	err := cache.Set(rev, schema, "hash1")
	require.NoError(t, err)

	// Wait for async update to process
	cache.Flush()

	// Get at the same revision should return the schema
	result := cache.Get(rev)
	require.NotNil(t, result)
	require.Equal(t, "hash1", result.GetV1().SchemaHash)
}

func TestRevisionedCache_CheckpointValidity(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Use deterministic times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := revisions.NewHLCForTime(baseTime)
	t2 := revisions.NewHLCForTime(baseTime.Add(1 * time.Hour))

	// Set a schema at T1
	schema := createTestSchema("hash1")
	err := cache.Set(t1, schema, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Get at T1 should work (checkpoint is set to T1)
	result := cache.Get(t1)
	require.NotNil(t, result)

	// Get at T2 (after checkpoint) should return nil
	result = cache.Get(t2)
	require.Nil(t, result, "revision beyond checkpoint should return nil")
}

func TestRevisionedCache_CheckpointUpdate(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Use deterministic times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := revisions.NewHLCForTime(baseTime)
	t2 := revisions.NewHLCForTime(baseTime.Add(1 * time.Minute))

	// Set a schema at T1
	schema := createTestSchema("hash1")
	err := cache.Set(t1, schema, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Set same hash at T2 (should update checkpoint)
	err = cache.Set(t2, schema, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Now T2 should be accessible
	result := cache.Get(t2)
	require.NotNil(t, result, "checkpoint should have been updated to T2")
}

func TestRevisionedCache_OldRevisionNoOp(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Use deterministic times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := revisions.NewHLCForTime(baseTime)
	t2 := revisions.NewHLCForTime(baseTime.Add(1 * time.Minute))

	// Set a schema at T2
	schema2 := createTestSchema("hash2")
	err := cache.Set(t2, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	// Try to set at T1 (older than current start) - should no-op
	schema1 := createTestSchema("hash1")
	err = cache.Set(t1, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Current range should still be hash2
	result := cache.Get(t2)
	require.NotNil(t, result)
	require.Equal(t, "hash2", result.GetV1().SchemaHash)
}

func TestRevisionedCache_HashChangeCreatesNewRange(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	// Use deterministic times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := revisions.NewHLCForTime(baseTime)
	t2 := revisions.NewHLCForTime(baseTime.Add(1 * time.Minute))

	// Set a schema at T1
	schema1 := createTestSchema("hash1")
	err := cache.Set(t1, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Set different hash at T2
	schema2 := createTestSchema("hash2")
	err = cache.Set(t2, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	// T1 should still return hash1 (now in historical range)
	result := cache.Get(t1)
	require.NotNil(t, result)
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	// T2 should return hash2 (current range)
	result = cache.Get(t2)
	require.NotNil(t, result)
	require.Equal(t, "hash2", result.GetV1().SchemaHash)
}

func TestRevisionedCache_HistoricalRangeCheckpointBounded(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := revisions.NewHLCForTime(baseTime)
	t2 := revisions.NewHLCForTime(baseTime.Add(1 * time.Minute))

	// Set hash1 at T1 (checkpoint is also T1)
	schema1 := createTestSchema("hash1")
	err := cache.Set(t1, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Set hash2 at T2 (closes hash1 range with checkpoint at T1)
	schema2 := createTestSchema("hash2")
	err = cache.Set(t2, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	// T1 should return hash1 (within checkpoint)
	result := cache.Get(t1)
	require.NotNil(t, result)
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	// T2 should return hash2 (new range)
	result = cache.Get(t2)
	require.NotNil(t, result)
	require.Equal(t, "hash2", result.GetV1().SchemaHash)
}

func TestRevisionedCache_NilSchemaError(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rev := revisions.NewHLCForTime(baseTime)
	err := cache.Set(rev, nil, "hash1")
	require.NoError(t, err) // Set doesn't block, error will be in background

	// Wait for background processing
	cache.Flush()

	// Should not have cached anything
	result := cache.Get(rev)
	require.Nil(t, result)
}

func TestRevisionedCache_TimeBasedCleanup(t *testing.T) {
	quantWindow := 50 * time.Millisecond
	cache := NewRevisionedSchemaCache[revisions.TimestampRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
		QuantizationWindow:      quantWindow,
	})
	defer cache.Close()

	// Use deterministic times
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// Create ranges well separated in time
	// t1's range will END at t2, so the end needs to be > 2x window before t4
	t1 := revisions.NewForTime(baseTime.Add(-300 * time.Millisecond))
	t2 := revisions.NewForTime(baseTime.Add(-250 * time.Millisecond)) // t1 range ends here - well outside 2x window
	t3 := revisions.NewForTime(baseTime.Add(-80 * time.Millisecond))  // Within 2x window
	t4 := revisions.NewForTime(baseTime)                              // Current time

	// Set schemas
	schema1 := createTestSchema("hash1")
	err := cache.Set(t1, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	schema2 := createTestSchema("hash2")
	err = cache.Set(t2, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	schema3 := createTestSchema("hash3")
	err = cache.Set(t3, schema3, "hash3")
	require.NoError(t, err)
	cache.Flush()

	// Set one more schema to trigger cleanup
	// Cutoff will be: baseTime - (50ms * 2) = baseTime - 100ms
	// t1 range ends at t2 (baseTime - 250ms), which is < cutoff, so should be deleted
	schema4 := createTestSchema("hash4")
	err = cache.Set(t4, schema4, "hash4")
	require.NoError(t, err)
	cache.Flush()

	// T1 range (ending at t2 = baseTime-250ms) should be cleaned up (< cutoff of baseTime-100ms)
	snap := cache.snapshot.Load()
	foundT1 := false
	for _, r := range snap.historicalRanges {
		if r.startRevision.Equal(t1) {
			foundT1 = true
			break
		}
	}
	require.False(t, foundT1, "range ending > 2x window ago should be cleaned up")

	// T3 should still exist (within 2x window)
	result := cache.Get(t3)
	require.NotNil(t, result, "range from within window should still exist")
}

func TestRevisionedCache_MemoryEviction(t *testing.T) {
	// Test that eviction respects memory limits
	// First, figure out how big one schema is
	testSchema := createTestSchema("test")
	schemaSize := testSchema.SizeVT()

	// Set cache to hold approximately 3 schemas worth
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: uint64(schemaSize) * 3,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 6 schemas with different hashes, flushing after each to ensure processing
	for i := 0; i < 6; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		schema := createTestSchema("hash" + string(rune('1'+i)))
		err := cache.Set(rev, schema, schema.GetV1().SchemaHash)
		require.NoError(t, err)
		cache.Flush() // Ensure this update is processed before the next
	}

	// Check eviction happened - should have evicted oldest ranges
	// Should keep at least 2 ranges (minimum) but no more than ~4 (due to size limit)
	snap := cache.snapshot.Load()
	totalRanges := len(snap.historicalRanges)
	if snap.currentRange != nil {
		totalRanges++
	}

	require.GreaterOrEqual(t, totalRanges, 2, "should keep minimum 2 ranges")
	require.LessOrEqual(t, totalRanges, 5, "should have evicted some ranges due to memory pressure")

	// Most recent should still exist
	rev := revisions.NewHLCForTime(baseTime.Add(5 * time.Minute))
	result := cache.Get(rev)
	require.NotNil(t, result, "most recent schema should exist")
}

func TestRevisionedCache_KeepsMinimumTwoRanges(t *testing.T) {
	// Very small cache
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 10, // Extremely small
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 3 schemas, flushing after each to ensure processing
	for i := 0; i < 3; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		schema := createTestSchema("hash" + string(rune('1'+i)))
		err := cache.Set(rev, schema, schema.GetV1().SchemaHash)
		require.NoError(t, err)
		cache.Flush() // Ensure this update is processed before the next
	}

	// Should keep at least 2 ranges (1 current + 1 historical minimum)
	snap := cache.snapshot.Load()
	totalRanges := len(snap.historicalRanges)
	if snap.currentRange != nil {
		totalRanges++
	}

	require.GreaterOrEqual(t, totalRanges, 2, "should keep minimum 2 ranges")
}

func TestRevisionedCache_AsyncUpdate(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rev := revisions.NewHLCForTime(baseTime)
	schema := createTestSchema("hash1")

	// Set should return immediately (non-blocking)
	err := cache.Set(rev, schema, "hash1")
	require.NoError(t, err)

	// Wait for async processing
	cache.Flush()

	// Schema should be available
	result := cache.Get(rev)
	require.NotNil(t, result)
}

func TestRevisionedCache_ConcurrentAccess(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// First, set an initial schema to ensure the cache has data
	initialRev := revisions.NewHLCForTime(baseTime)
	initialSchema := createTestSchema("hash1")
	_ = cache.Set(initialRev, initialSchema, "hash1")
	cache.Flush()

	var wg sync.WaitGroup

	// Concurrent writes (all with same hash so they update checkpoint)
	// Due to buffer size 1, most updates will be dropped (intentional design)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(idx) * time.Millisecond))
			schema := createTestSchema("hash1")
			_ = cache.Set(rev, schema, "hash1")
		}(i)
	}

	wg.Wait()

	// Wait for any updates that made it through to complete
	cache.Flush()

	// Concurrent reads after writes are done
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(idx) * time.Millisecond))
			_ = cache.Get(rev)
		}(i)
	}

	wg.Wait()

	// Should not panic and cache should be in valid state
	// Check the initial revision which we know is in the cache
	result := cache.Get(initialRev)
	require.NotNil(t, result, "should be able to read after concurrent access")
}

func TestRevisionedCache_WarmCache(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	rev := revisions.NewHLCForTime(time.Now())
	schema := createTestSchema("hash1")

	// Set up schema loader
	schemaLoader := func(ctx context.Context) (*core.StoredSchema, revisions.HLCRevision, error) {
		return schema, rev, nil
	}

	cache.schemaLoader = schemaLoader

	// Warm the cache
	err := cache.WarmCache(context.Background())
	require.NoError(t, err)

	// Flush to ensure async update is processed
	cache.Flush()

	// Schema should be immediately available
	result := cache.Get(rev)
	require.NotNil(t, result)
	require.Equal(t, "hash1", result.GetV1().SchemaHash)
}

// TestRevisionedCache_NeedsUpdate removed - needsUpdate is now an internal implementation detail
// The behavior is tested indirectly through other tests that verify caching behavior

func TestRevisionedCache_MultipleHistoricalRanges(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 4 different schema versions, flushing after each to ensure processing
	for i := 0; i < 4; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		schema := createTestSchema("hash" + string(rune('1'+i)))
		err := cache.Set(rev, schema, schema.GetV1().SchemaHash)
		require.NoError(t, err)
		cache.Flush() // Ensure this update is processed before the next
	}

	// Should have 3 historical ranges + 1 current
	snap := cache.snapshot.Load()
	histCount := len(snap.historicalRanges)
	hasCurrent := snap.currentRange != nil

	require.Equal(t, 3, histCount, "should have 3 historical ranges")
	require.True(t, hasCurrent, "should have current range")

	// All revisions should be accessible
	for i := 0; i < 4; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		result := cache.Get(rev)
		require.NotNil(t, result, "revision %d should be accessible", i)
		expectedHash := "hash" + string(rune('1'+i))
		require.Equal(t, expectedHash, result.GetV1().SchemaHash)
	}
}

func TestRevisionedCache_Close(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})

	rev := revisions.NewHLCForTime(time.Now())
	schema := createTestSchema("hash1")
	err := cache.Set(rev, schema, "hash1")
	require.NoError(t, err)

	// Close should wait for pending updates
	cache.Close()

	// Update channel should be closed
	_, ok := <-cache.updateChan
	require.False(t, ok, "update channel should be closed")
}

func TestRevisionedCache_BackwardSearch(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 5 historical ranges, flushing after each to ensure processing
	for i := 0; i < 5; i++ {
		rev := revisions.NewHLCForTime(baseTime.Add(time.Duration(i) * time.Minute))
		schema := createTestSchema("hash" + string(rune('1'+i)))
		err := cache.Set(rev, schema, schema.GetV1().SchemaHash)
		require.NoError(t, err)
		cache.Flush() // Ensure this update is processed before the next
	}

	// Search for revision in the second-to-last range (more recent)
	// Should be found quickly with backward search
	rev := revisions.NewHLCForTime(baseTime.Add(3 * time.Minute))
	result := cache.Get(rev)
	require.NotNil(t, result)
	require.Equal(t, "hash4", result.GetV1().SchemaHash)
}

func TestRevisionedCache_WatcherSchemaTransition(t *testing.T) {
	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// 1. Warm the cache with initial schema at t0
	t0 := revisions.NewHLCForTime(baseTime)
	schema1 := createTestSchema("hash1")

	// Set up schema loader
	cache.schemaLoader = func(ctx context.Context) (*core.StoredSchema, revisions.HLCRevision, error) {
		return schema1, t0, nil
	}

	err := cache.WarmCache(context.Background())
	require.NoError(t, err)
	cache.Flush() // Flush to ensure async update is processed

	// Verify initial schema is accessible at t0
	result := cache.Get(t0)
	require.NotNil(t, result)
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	// 2. Simulate watcher moving checkpoint forward (same hash, no schema change)
	t1 := revisions.NewHLCForTime(baseTime.Add(1 * time.Minute))
	err = cache.Set(t1, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Verify schema is still accessible at both t0 and t1
	result = cache.Get(t0)
	require.NotNil(t, result, "should get schema at t0")
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	result = cache.Get(t1)
	require.NotNil(t, result, "should get schema at t1")
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	// 3. Move checkpoint forward again
	t2 := revisions.NewHLCForTime(baseTime.Add(2 * time.Minute))
	err = cache.Set(t2, schema1, "hash1")
	require.NoError(t, err)
	cache.Flush()

	// Verify schema accessible at all previous revisions
	result = cache.Get(t0)
	require.NotNil(t, result, "should get schema at t0")
	result = cache.Get(t1)
	require.NotNil(t, result, "should get schema at t1")
	result = cache.Get(t2)
	require.NotNil(t, result, "should get schema at t2")

	// 4. Now simulate watcher detecting a schema change at t3
	t3 := revisions.NewHLCForTime(baseTime.Add(3 * time.Minute))
	schema2 := createTestSchema("hash2")
	err = cache.Set(t3, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	// 5. Verify old schema is still accessible for revisions before t3
	result = cache.Get(t0)
	require.NotNil(t, result, "should get old schema at t0")
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	result = cache.Get(t1)
	require.NotNil(t, result, "should get old schema at t1")
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	result = cache.Get(t2)
	require.NotNil(t, result, "should get old schema at t2")
	require.Equal(t, "hash1", result.GetV1().SchemaHash)

	// 6. Verify new schema is accessible at t3
	result = cache.Get(t3)
	require.NotNil(t, result, "should get new schema at t3")
	require.Equal(t, "hash2", result.GetV1().SchemaHash)

	// 7. Verify Get for revision AFTER the schema change but before watcher confirms returns nil
	// This is because the new range's checkpoint is at t3, so anything beyond t3 is not yet confirmed
	t4 := revisions.NewHLCForTime(baseTime.Add(4 * time.Minute))
	result = cache.Get(t4)
	require.Nil(t, result, "should return nil for revision beyond checkpoint")

	// 8. Now simulate watcher moving checkpoint forward for the new schema
	t5 := revisions.NewHLCForTime(baseTime.Add(5 * time.Minute))
	err = cache.Set(t5, schema2, "hash2")
	require.NoError(t, err)
	cache.Flush()

	// Now t4 should be accessible (within the checkpoint)
	result = cache.Get(t4)
	require.NotNil(t, result, "should get new schema at t4 after checkpoint moved")
	require.Equal(t, "hash2", result.GetV1().SchemaHash)

	// And t5 should also be accessible
	result = cache.Get(t5)
	require.NotNil(t, result, "should get new schema at t5")
	require.Equal(t, "hash2", result.GetV1().SchemaHash)
}

func TestRevisionedCache_SafetyWithMissedSchemaChange(t *testing.T) {
	// This test demonstrates why we can't trust endRevision for historical ranges.
	// Consider this scenario:
	// 1. Schema A starts at revision 100, watcher checkpoints at 105
	// 2. Schema actually changes to B at revision 112 (but watcher hasn't detected it)
	// 3. Watcher detects change at revision 115 and sets endRevision=115 for schema A
	// 4. Query at revision 114 would incorrectly return schema A (because 114 < 115)
	//    but the actual schema at 114 was B
	//
	// The safe behavior is to only trust up to the checkpoint (105 in this case),
	// so revision 114 should return nil (cache miss) rather than wrong data.

	cache := NewRevisionedSchemaCache[revisions.HLCRevision](options.SchemaCacheOptions{
		MaximumCacheMemoryBytes: 1024 * 1024,
	})
	defer cache.Close()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// 1. Set schema A at t100 with checkpoint at t105
	t100 := revisions.NewHLCForTime(baseTime.Add(100 * time.Millisecond))
	t105 := revisions.NewHLCForTime(baseTime.Add(105 * time.Millisecond))
	schemaA := createTestSchema("hashA")

	// Simulate watcher setting initial schema
	err := cache.setWithCheckpoint(t100, schemaA, "hashA", t105)
	require.NoError(t, err)
	cache.Flush()

	// 2. Schema actually changes to B at t112, but watcher doesn't detect until t115
	t112 := revisions.NewHLCForTime(baseTime.Add(112 * time.Millisecond))
	t115 := revisions.NewHLCForTime(baseTime.Add(115 * time.Millisecond))
	schemaB := createTestSchema("hashB")

	// Simulate watcher detecting the change at t115
	err = cache.Set(t115, schemaB, "hashB")
	require.NoError(t, err)
	cache.Flush()

	// 3. Query at t105 (the checkpoint) - should return schema A
	result := cache.Get(t105)
	require.NotNil(t, result, "should get schema A at checkpoint revision t105")
	require.Equal(t, "hashA", result.GetV1().SchemaHash)

	// 4. Query at t112 or t114 (after checkpoint but before watcher detected change)
	// SAFETY REQUIREMENT: These MUST return nil (cache miss) not schema A
	// because we don't actually know what the schema was between t105 and t115
	result = cache.Get(t112)
	require.Nil(t, result, "SAFETY: revision after checkpoint but before new range should return nil (cache miss)")

	t114 := revisions.NewHLCForTime(baseTime.Add(114 * time.Millisecond))
	result = cache.Get(t114)
	require.Nil(t, result, "SAFETY: revision after checkpoint but before new range should return nil (cache miss)")

	// 5. Query at t115 (the new schema) - should return schema B
	result = cache.Get(t115)
	require.NotNil(t, result, "should get schema B at t115")
	require.Equal(t, "hashB", result.GetV1().SchemaHash)
}

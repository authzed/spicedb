package common

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func makeTestSchema(text string) *core.StoredSchema {
	return &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: text,
			},
		},
	}
}

func TestSchemaHashCache_NilCache(t *testing.T) {
	// Cache should use default settings when MaximumCacheEntries is 0
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Operations on nil cache should be safe no-ops
	var nilCache *SchemaHashCache
	schema, err := nilCache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, schema)

	err = nilCache.Set(datastore.NoRevision, datastore.SchemaHash("hash1"), makeTestSchema("definition user {}"))
	require.NoError(t, err)
	schema, err = nilCache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, schema)

	schema, err = nilCache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash("hash1"), func(ctx context.Context) (*core.StoredSchema, error) {
		return makeTestSchema("definition user {}"), nil
	})
	require.NoError(t, err)
	require.NotNil(t, schema)
}

func TestSchemaHashCache_BasicGetSet(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Cache miss
	retrieved, err := cache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, retrieved)

	// Set and get
	schema := makeTestSchema("definition user {}")
	err = cache.Set(datastore.NoRevision, datastore.SchemaHash("hash1"), schema)
	require.NoError(t, err)

	retrieved, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, schema.GetV1().SchemaText, retrieved.GetV1().SchemaText)
}

func TestSchemaHashCache_EmptyHash(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Empty hash should panic (via MustBugf) in tests
	require.Panics(t, func() {
		cache.Set(datastore.NoRevision, datastore.SchemaHash(""), makeTestSchema("definition user {}"))
	}, "empty hash should panic")

	require.Panics(t, func() {
		cache.Get(datastore.NoRevision, datastore.SchemaHash(""))
	}, "empty hash should panic")
}

func TestSchemaHashCache_NilRevision(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Nil revision should panic (via MustBugf) in tests
	require.Panics(t, func() {
		cache.Set(nil, datastore.SchemaHash("hash1"), makeTestSchema("definition user {}"))
	}, "nil revision should panic")
}

func TestSchemaHashCache_LRUEviction(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 3, // Small cache for testing eviction
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Fill cache with 3 entries
	require.NoError(t, cache.Set(datastore.NoRevision, datastore.SchemaHash("hash0"), makeTestSchema("definition 0")))
	require.NoError(t, cache.Set(datastore.NoRevision, datastore.SchemaHash("hash1"), makeTestSchema("definition 1")))
	require.NoError(t, cache.Set(datastore.NoRevision, datastore.SchemaHash("hash2"), makeTestSchema("definition 2")))

	// All should be present initially
	schema, err := cache.Get(datastore.NoRevision, datastore.SchemaHash("hash0"))
	require.NoError(t, err)
	require.NotNil(t, schema)
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, schema)
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash2"))
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Add one more - this will evict the least recently used
	// Since we just accessed hash2 (via the "latest" fast path), and hash0 and hash1
	// via LRU.Get(), the LRU order is: hash1, hash0, hash2
	// Adding hash3 will evict hash2 from the LRU
	require.NoError(t, cache.Set(datastore.NoRevision, datastore.SchemaHash("hash3"), makeTestSchema("definition 3")))

	// hash3 is now the newest
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash3"))
	require.NoError(t, err)
	require.NotNil(t, schema)

	// hash0 and hash1 should still be in the LRU
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash0"))
	require.NoError(t, err)
	require.NotNil(t, schema)
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.NotNil(t, schema)

	// hash2 was evicted from LRU and is no longer in "latest" (hash3 is), so it should return nil
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash2"))
	require.NoError(t, err)
	require.Nil(t, schema)
}

func TestSchemaHashCache_GetOrLoad(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	loadCalls := 0
	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		loadCalls++
		return makeTestSchema("loaded definition"), nil
	}

	// First call should load
	schema, err := cache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, "loaded definition", schema.GetV1().SchemaText)
	require.Equal(t, 1, loadCalls)

	// Second call should hit cache
	schema, err = cache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash("hash1"), loader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, "loaded definition", schema.GetV1().SchemaText)
	require.Equal(t, 1, loadCalls) // Should not call loader again
}

func TestSchemaHashCache_GetOrLoadEmptyHash(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		return makeTestSchema("loaded definition"), nil
	}

	// Empty hash should panic (via MustBugf) in tests
	require.Panics(t, func() {
		cache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash(""), loader)
	}, "empty hash should panic")
}

func TestSchemaHashCache_Singleflight(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	loadCalls := 0
	loadStarted := make(chan struct{})
	loadContinue := make(chan struct{})

	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		loadCalls++
		close(loadStarted)
		<-loadContinue
		return makeTestSchema("loaded definition"), nil
	}

	// Start multiple concurrent loads
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			schema, err := cache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash("hash1"), loader)
			require.NoError(t, err)
			require.NotNil(t, schema)
			require.Equal(t, "loaded definition", schema.GetV1().SchemaText)
		}()
	}

	// Wait for first load to start
	<-loadStarted

	// Let the load complete
	close(loadContinue)

	// Wait for all goroutines to finish
	wg.Wait()

	// Should only have called loader once due to singleflight
	require.Equal(t, 1, loadCalls)
}

func TestSchemaHashCache_LoadError(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	expectedErr := fmt.Errorf("load failed")
	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		return nil, expectedErr
	}

	// Error should be propagated
	schema, err := cache.GetOrLoad(context.Background(), datastore.NoRevision, datastore.SchemaHash("hash1"), loader)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, schema)

	// Failed load should not be cached
	cached, err := cache.Get(datastore.NoRevision, datastore.SchemaHash("hash1"))
	require.NoError(t, err)
	require.Nil(t, cached)
}

func TestSchemaHashCache_DefaultMaxEntries(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 0, // Should default to 100
	})
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Add more than 100 entries to test default
	for i := 0; i < 101; i++ {
		require.NoError(t, cache.Set(datastore.NoRevision, datastore.SchemaHash(fmt.Sprintf("hash%d", i)), makeTestSchema(fmt.Sprintf("definition %d", i))))
	}

	// First entry should be evicted
	schema, err := cache.Get(datastore.NoRevision, datastore.SchemaHash("hash0"))
	require.NoError(t, err)
	require.Nil(t, schema)

	// Last entry should be present
	schema, err = cache.Get(datastore.NoRevision, datastore.SchemaHash("hash100"))
	require.NoError(t, err)
	require.NotNil(t, schema)
}

func TestBypassSentinels_AllIncluded(t *testing.T) {
	// This test ensures that all sentinel values defined in the datastore package
	// are included in the bypassSentinels slice. If a new sentinel is added to the
	// datastore package, it must also be added to the bypassSentinels slice in hashcache.go.

	allSentinels := []datastore.SchemaHash{
		datastore.NoSchemaHashInTransaction,
		datastore.NoSchemaHashForTesting,
		datastore.NoSchemaHashForWatch,
		datastore.NoSchemaHashForLegacyCursor,
	}

	// Verify each sentinel is in the bypassSentinels slice
	for _, sentinel := range allSentinels {
		require.True(t, isBypassSentinel(sentinel),
			"sentinel %q is not in bypassSentinels slice", sentinel)
	}

	// Verify bypassSentinels doesn't contain any extra values
	require.Len(t, bypassSentinels, len(allSentinels),
		"bypassSentinels should contain exactly %d entries", len(allSentinels))
}

func TestBypassSentinels_CacheBehavior(t *testing.T) {
	cache, err := NewSchemaHashCache(options.SchemaCacheOptions{
		MaximumCacheEntries: 10,
	})
	require.NoError(t, err)

	schema := makeTestSchema("definition user {}")

	// Test that each sentinel bypasses the cache in Get()
	for sentinel := range bypassSentinels {
		result, err := cache.Get(datastore.NoRevision, sentinel)
		require.NoError(t, err)
		require.Nil(t, result, "Get with sentinel %q should return nil", sentinel)
	}

	// Test that each sentinel bypasses the cache in Set()
	for sentinel := range bypassSentinels {
		err := cache.Set(datastore.NoRevision, sentinel, schema)
		require.NoError(t, err, "Set with sentinel %q should not error", sentinel)

		// Verify it wasn't actually cached
		result, err := cache.Get(datastore.NoRevision, sentinel)
		require.NoError(t, err)
		require.Nil(t, result, "sentinel %q should not be cached", sentinel)
	}

	// Test that each sentinel bypasses the cache in GetOrLoad()
	for sentinel := range bypassSentinels {
		loadCalled := false
		result, err := cache.GetOrLoad(context.Background(), datastore.NoRevision, sentinel,
			func(ctx context.Context) (*core.StoredSchema, error) {
				loadCalled = true
				return schema, nil
			})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, loadCalled, "loader should be called for sentinel %q", sentinel)

		// Verify it wasn't cached - loader should be called again
		loadCalled = false
		result, err = cache.GetOrLoad(context.Background(), datastore.NoRevision, sentinel,
			func(ctx context.Context) (*core.StoredSchema, error) {
				loadCalled = true
				return schema, nil
			})
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, loadCalled, "loader should be called again for sentinel %q (not cached)", sentinel)
	}
}

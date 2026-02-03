//go:build !wasm

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheWithMetrics(t *testing.T) {
	t.Parallel()

	config := &Config{
		NumCounters: 10000,
		MaxCost:     1000,
		DefaultTTL:  10 * time.Hour,
	}

	t.Run("otter", func(t *testing.T) {
		t.Parallel()
		testCacheImplementation(t, func() (Cache[StringKey, string], error) {
			return NewOtterCacheWithMetrics[StringKey, string]("test-otter", config)
		})
	})

	t.Run("ristretto", func(t *testing.T) {
		t.Parallel()
		testCacheImplementation(t, func() (Cache[StringKey, string], error) {
			// Use the metrics version for proper metrics tracking
			return NewRistrettoCacheWithMetrics[StringKey, string]("test-ristretto", config)
		})
	})

	t.Run("theine", func(t *testing.T) {
		t.Parallel()
		testCacheImplementation(t, func() (Cache[StringKey, string], error) {
			return NewTheineCacheWithMetrics[StringKey, string]("test-theine", config)
		})
	})
}

func testCacheImplementation(t *testing.T, factory func() (Cache[StringKey, string], error)) {
	t.Run("Set and Get", func(t *testing.T) {
		cache, err := factory()
		require.NoError(t, err)
		defer cache.Close()

		// Set multiple entries
		entries := []struct {
			key   StringKey
			value string
		}{
			{"key1", "value1"},
			{"key2", "value2"},
			{"key3", "value3"},
		}

		for _, entry := range entries {
			ok := cache.Set(entry.key, entry.value, 10)
			require.True(t, ok)
		}

		// Wait for all sets to be processed
		cache.Wait()

		// Verify all entries
		for _, entry := range entries {
			retrieved, found := cache.Get(entry.key)
			require.True(t, found, "expected key %s to be found", entry.key)
			require.Equal(t, entry.value, retrieved, "expected value for key %s to match", entry.key)
		}
	})

	t.Run("Set same key with diff values", func(t *testing.T) {
		cache, err := factory()
		require.NoError(t, err)
		defer cache.Close()

		ok := cache.Set(StringKey("metric-key-1"), "value1", 10)
		require.True(t, ok)
		cache.Wait()
		val, found := cache.Get("metric-key-1")
		require.True(t, found)
		require.Equal(t, "value1", val)

		// same key set, diff value
		ok = cache.Set(StringKey("metric-key-1"), "value2", 10)
		require.True(t, ok)
		cache.Wait()
		val, found = cache.Get("metric-key-1")
		require.True(t, found)
		require.Equal(t, "value2", val)
	})

	t.Run("Close multiple times", func(t *testing.T) {
		cache, err := factory()
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			cache.Close()
		}
	})

	t.Run("GetTTL", func(t *testing.T) {
		cache, err := factory()
		require.NoError(t, err)
		defer cache.Close()

		require.Equal(t, 10*time.Hour, cache.GetTTL())
	})

	t.Run("GetMetrics", func(t *testing.T) {
		cache, err := factory()
		require.NoError(t, err)
		defer cache.Close()

		// Set some values
		ok := cache.Set(StringKey("metric-key-1"), "value1", 10)
		require.True(t, ok)
		ok = cache.Set(StringKey("metric-key-2"), "value2", 20)
		require.True(t, ok)

		cache.Wait()

		// Perform some gets (hits and misses)
		_, ok = cache.Get(StringKey("metric-key-1")) // hit
		require.True(t, ok)
		_, ok = cache.Get(StringKey("metric-key-2")) // hit
		require.True(t, ok)
		_, ok = cache.Get(StringKey("non-existent")) // miss
		require.False(t, ok)

		metrics := cache.GetMetrics()
		require.NotNil(t, metrics, "expected metrics to be available")

		// Verify hits and misses are tracked
		hits := metrics.Hits()
		misses := metrics.Misses()
		require.GreaterOrEqual(t, hits, uint64(1), "expected at least one hit")
		require.GreaterOrEqual(t, misses, uint64(1), "expected at least one miss")

		// Verify cost tracking
		costAdded := metrics.CostAdded()
		require.GreaterOrEqual(t, costAdded, uint64(10), "expected cost to be tracked")
	})
}

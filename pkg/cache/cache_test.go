package cache

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestEntryWeight(t *testing.T) {
	// Empty key, zero payload.
	require.Equal(t, uint32(0), entryWeight("", 0))

	// Payload + key bytes.
	require.Equal(t, uint32(10+3), entryWeight("abc", 10))

	// Saturates rather than overflowing uint32.
	require.Equal(t, uint32(math.MaxUint32), entryWeight("x", math.MaxUint32))
	require.Equal(t, uint32(math.MaxUint32), entryWeight("x", math.MaxUint32-1))
}

func TestCostAddedIncludesKey(t *testing.T) {
	cache, err := NewOtterCacheWithMetrics[StringKey, string](
		prometheus.NewRegistry(), "test-otter",
		&Config{MaxCost: 100000, DefaultTTL: 10 * time.Hour},
	)
	require.NoError(t, err)
	defer cache.Close()

	const key = "some-key"
	const payloadCost = 10
	require.True(t, cache.Set(StringKey(key), "value", payloadCost))
	cache.Wait()

	// costAdded must reflect the full entry weight (payload + key bytes), not
	// just the caller-supplied payload cost.
	require.Equal(t,
		uint64(payloadCost+len(key)),
		cache.GetMetrics().CostAdded(),
	)
}

func TestCacheWithMetrics(t *testing.T) {
	config := &Config{
		MaxCost:    1000,
		DefaultTTL: 10 * time.Hour,
	}

	t.Run("Set and Get", func(t *testing.T) {
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
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
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
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
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
		require.NoError(t, err)

		for range 10 {
			cache.Close()
		}
	})

	t.Run("GetTTL", func(t *testing.T) {
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
		require.NoError(t, err)
		defer cache.Close()

		require.Equal(t, 10*time.Hour, cache.GetTTL())
	})

	t.Run("GetMetrics", func(t *testing.T) {
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
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

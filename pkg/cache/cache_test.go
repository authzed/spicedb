package cache

import (
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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

	// costAdded must reflect the full entry weight (payload + key bytes), not
	// just the caller-supplied payload cost.
	require.Equal(
		t,
		uint64(payloadCost+len(key)),
		cache.GetMetrics().CostAdded(),
	)
}

// TestSetDoesNotRecordAccesses asserts that Set leaves the hit/miss counters
// alone. Set must not probe the cache first: the underlying cache records every
// lookup it is asked to perform, so a probe inside Set would be counted as a
// hit or a miss that no caller ever made, understating the reported hit rate.
func TestSetDoesNotRecordAccesses(t *testing.T) {
	cache, err := NewOtterCacheWithMetrics[StringKey, string](
		prometheus.NewRegistry(), "test-otter",
		&Config{MaxCost: 100000, DefaultTTL: 10 * time.Hour},
	)
	require.NoError(t, err)
	defer cache.Close()

	// A write to a new key, then a write overwriting it.
	require.True(t, cache.Set(StringKey("key"), "first", 10))
	require.True(t, cache.Set(StringKey("key"), "second", 10))

	metrics := cache.GetMetrics()
	require.Zero(t, metrics.Hits(), "Set must not record a hit")
	require.Zero(t, metrics.Misses(), "Set must not record a miss")

	// One real hit and one real miss, and nothing else.
	value, found := cache.Get(StringKey("key"))
	require.True(t, found)
	require.Equal(t, "second", value, "the overwriting Set should have taken effect")

	_, found = cache.Get(StringKey("absent"))
	require.False(t, found)

	require.Equal(t, uint64(1), metrics.Hits())
	require.Equal(t, uint64(1), metrics.Misses())
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
		val, found := cache.Get("metric-key-1")
		require.True(t, found)
		require.Equal(t, "value1", val)

		// same key set, diff value
		ok = cache.Set(StringKey("metric-key-1"), "value2", 10)
		require.True(t, ok)
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

	t.Run("GetMetrics", func(t *testing.T) {
		cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", config)
		require.NoError(t, err)
		t.Cleanup(func() {
			cache.Close()
		})

		// Set some values
		ok := cache.Set(StringKey("metric-key-1"), "value1", 10)
		require.True(t, ok)
		ok = cache.Set(StringKey("metric-key-2"), "value2", 20)
		require.True(t, ok)

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

	t.Run("TTL Behavior", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cache, err := NewOtterCacheWithMetrics[StringKey, string](prometheus.NewRegistry(), "test-otter", &Config{
				MaxCost: 1000,
				// set a lower TTL
				DefaultTTL: 1 * time.Minute,
			})
			//nolint:testifylint  // we're in a goroutine
			if !assert.NoError(t, err) {
				return
			}

			// Set and get a key
			ok := cache.Set(StringKey("a key"), "a value", 10)
			assert.True(t, ok)

			// retrieve the key
			retrieved, found := cache.Get(StringKey("a key"))
			assert.True(t, found, "expected key %s to be found", "a key")
			assert.Equal(t, "a value", retrieved, "expected value for key %s to match", "a key")

			// wait for a bit
			time.Sleep(3 * time.Minute)

			// Retrieve the original key again; we're expecting not to find it.
			_, found = cache.Get(StringKey("a key"))
			assert.False(t, found, "expected key %s to be found", "a key")

			cache.Close()
		})
	})
}

func TestEvictionsByCause(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		cache, err := newOtterCache[StringKey, string]("test-otter", &Config{MaxCost: 100})
		require.NoError(t, err)
		t.Cleanup(cache.Close)

		for _, key := range []StringKey{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"} {
			cache.Set(key, "value", 18) // weight 20 with the key
		}
		cache.cache.CleanUp()

		// 200 of weight into a budget of 100 must evict at least five entries.
		require.GreaterOrEqual(t, cache.metrics.overflow.entries.Load(), uint64(5))
		require.Equal(t, 20*cache.metrics.overflow.entries.Load(), cache.metrics.overflow.cost.Load())
		require.Zero(t, cache.metrics.expiration.entries.Load())
		require.Equal(t, cache.metrics.CostEvicted(), cache.metrics.overflow.cost.Load())
	})

	t.Run("expiration", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cache, err := newOtterCache[StringKey, string]("test-otter", &Config{MaxCost: 1000, DefaultTTL: time.Minute})
			//nolint:testifylint // we're in a goroutine
			if !assert.NoError(t, err) {
				return
			}
			defer cache.Close()

			cache.Set("key", "value", 7) // weight 10 with the key
			time.Sleep(3 * time.Minute)
			cache.cache.CleanUp()

			assert.Equal(t, uint64(1), cache.metrics.expiration.entries.Load())
			assert.Equal(t, uint64(10), cache.metrics.expiration.cost.Load())
			assert.Zero(t, cache.metrics.overflow.entries.Load())
		})
	})

	t.Run("replacement is not an eviction", func(t *testing.T) {
		cache, err := newOtterCache[StringKey, string]("test-otter", &Config{MaxCost: 1000})
		require.NoError(t, err)
		t.Cleanup(cache.Close)

		cache.Set("key", "value1", 10)
		cache.Set("key", "value2", 10)
		cache.cache.CleanUp()

		require.Zero(t, cache.metrics.overflow.entries.Load())
		require.Zero(t, cache.metrics.expiration.entries.Load())
	})

	t.Run("exported by cause", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		c, err := NewOtterCacheWithMetrics[StringKey, string](registry, "test-otter", &Config{MaxCost: 100})
		require.NoError(t, err)
		t.Cleanup(c.Close)

		for _, key := range []StringKey{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"} {
			c.Set(key, "value", 18)
		}
		c.(*otterCache[StringKey, string]).cache.CleanUp()

		families, err := registry.Gather()
		require.NoError(t, err)
		got := map[string]map[string]float64{}
		for _, family := range families {
			name := family.GetName()
			if name != "spicedb_cache_evictions_total" && name != "spicedb_cache_evicted_bytes_total" {
				continue
			}
			got[name] = map[string]float64{}
			for _, metric := range family.GetMetric() {
				labels := map[string]string{}
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				require.Equal(t, "test-otter", labels["cache"])
				got[name][labels["cause"]] = metric.GetCounter().GetValue()
			}
		}

		require.GreaterOrEqual(t, got["spicedb_cache_evictions_total"]["overflow"], float64(5))
		require.Zero(t, got["spicedb_cache_evictions_total"]["expiration"])
		require.InDelta(t, 20*got["spicedb_cache_evictions_total"]["overflow"], got["spicedb_cache_evicted_bytes_total"]["overflow"], 0)
		require.Contains(t, got["spicedb_cache_evicted_bytes_total"], "expiration")
	})
}

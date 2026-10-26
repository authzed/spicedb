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

func TestTTLExpiration(t *testing.T) {
	const ttl = 1 * time.Minute

	newCache := func(t *testing.T, refreshOnAccess bool) Cache[StringKey, string] {
		c, err := NewOtterCache[StringKey, string]("test-otter", &Config{
			MaxCost:            1000,
			DefaultTTL:         ttl,
			RefreshTTLOnAccess: refreshOnAccess,
		})
		//nolint:testifylint  // we're in a goroutine
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		t.Cleanup(c.Close)
		return c
	}

	t.Run("expires after the TTL", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := newCache(t, false)
			assert.True(t, c.Set(StringKey("key"), "value", 10))

			time.Sleep(ttl - time.Second)
			_, found := c.Get(StringKey("key"))
			assert.True(t, found, "expected key to be present before the TTL elapsed")

			time.Sleep(2 * time.Second)
			_, found = c.Get(StringKey("key"))
			assert.False(t, found, "expected key to have expired after the TTL")
		})
	})

	t.Run("reads do not extend the TTL", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := newCache(t, false)
			assert.True(t, c.Set(StringKey("key"), "value", 10))

			// Read the key every 10 seconds, so a sliding TTL would never lapse.
			// A key read steadily must still expire one TTL after it was set.
			for elapsed := 10 * time.Second; elapsed < ttl; elapsed += 10 * time.Second {
				time.Sleep(10 * time.Second)
				_, found := c.Get(StringKey("key"))
				assert.True(t, found, "expected key to be present %s after it was set", elapsed)
			}

			time.Sleep(10 * time.Second)
			_, found := c.Get(StringKey("key"))
			assert.False(t, found, "expected key to have expired despite being read")
		})
	})

	t.Run("setting a key restarts its TTL", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := newCache(t, false)
			assert.True(t, c.Set(StringKey("key"), "first", 10))

			time.Sleep(ttl / 2)
			assert.True(t, c.Set(StringKey("key"), "second", 10))

			time.Sleep(ttl/2 + time.Second)
			value, found := c.Get(StringKey("key"))
			assert.True(t, found, "expected key to be present one TTL after its first set")
			assert.Equal(t, "second", value)

			time.Sleep(ttl / 2)
			_, found = c.Get(StringKey("key"))
			assert.False(t, found, "expected key to have expired one TTL after its last set")
		})
	})

	t.Run("RefreshTTLOnAccess extends the TTL on reads", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := newCache(t, true)
			assert.True(t, c.Set(StringKey("key"), "value", 10))

			for range 5 {
				time.Sleep(ttl / 2)
				_, found := c.Get(StringKey("key"))
				assert.True(t, found, "expected a read to keep the key alive")
			}

			// Once the reads stop, the key expires one TTL after the last one.
			time.Sleep(ttl + time.Second)
			_, found := c.Get(StringKey("key"))
			assert.False(t, found, "expected key to have expired once reads stopped")
		})
	})
}

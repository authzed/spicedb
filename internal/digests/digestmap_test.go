package digests

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDigestMap(t *testing.T) {
	dm := NewDigestMap()
	require.NotNil(t, dm)
	require.NotNil(t, dm.m)
}

func TestDigestMap_Add(t *testing.T) {
	dm := NewDigestMap()

	// Add values to a new key
	dm.Add("test-key", 10.0)
	dm.Add("test-key", 20.0)
	dm.Add("test-key", 30.0)

	// Verify the digest was created and has values
	cdf, ok := dm.CDF("test-key", 22.0)
	require.True(t, ok)
	require.True(t, cdf > 0 && cdf < 1)
}

func TestDigestMap_CDF(t *testing.T) {
	dm := NewDigestMap()

	// Test CDF on non-existent key
	_, ok := dm.CDF("non-existent", 10.0)
	require.False(t, ok)

	// Add some values
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, v := range values {
		dm.Add("test-key", v)
	}

	// Test CDF at various points
	cdf0, ok := dm.CDF("test-key", 0)
	require.True(t, ok)
	require.Equal(t, float64(0), cdf0)

	cdf5, ok := dm.CDF("test-key", 5)
	require.True(t, ok)
	require.True(t, cdf5 > 0 && cdf5 < 1)

	cdf100, ok := dm.CDF("test-key", 100)
	require.True(t, ok)
	require.Equal(t, float64(1), cdf100)
}

func TestDigestMap_MultipleKeys(t *testing.T) {
	dm := NewDigestMap()

	// Add values to different keys
	dm.Add("key1", 10.0)
	dm.Add("key1", 20.0)
	dm.Add("key2", 100.0)
	dm.Add("key2", 200.0)

	// Test CDF for both keys
	cdf1, ok1 := dm.CDF("key1", 12.0)
	require.True(t, ok1)
	require.True(t, cdf1 > 0 && cdf1 < 1)

	cdf2, ok2 := dm.CDF("key2", 120.0)
	require.True(t, ok2)
	require.True(t, cdf2 > 0 && cdf2 < 1)

	// Test non-existent key
	_, ok3 := dm.CDF("key3", 50.0)
	require.False(t, ok3)
}

func TestDigestMap_ConcurrentAccess(t *testing.T) {
	dm := NewDigestMap()
	numGoroutines := 10
	numOperations := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // writers and readers

	// Concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "key-" + string(rune('0'+id))
			for j := 0; j < numOperations; j++ {
				dm.Add(key, float64(j))
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "key-" + string(rune('0'+id))
			for j := 0; j < numOperations; j++ {
				dm.CDF(key, 50.0)
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys have data
	for i := 0; i < numGoroutines; i++ {
		key := "key-" + string(rune('0'+i))
		cdf, ok := dm.CDF(key, 50.0)
		require.True(t, ok)
		require.True(t, cdf >= 0 && cdf <= 1)
	}
}

func TestDigestMap_EmptyKey(t *testing.T) {
	dm := NewDigestMap()

	// CDF on empty map should return false
	_, ok := dm.CDF("", 10.0)
	require.False(t, ok)

	// Add value to empty string key
	dm.Add("", 42.0)

	// Should now work
	cdf, ok := dm.CDF("", 42.0)
	require.True(t, ok)
	require.Equal(t, float64(1), cdf)
}

func TestDigestMap_SingleValue(t *testing.T) {
	dm := NewDigestMap()

	dm.Add("single", 42.0)

	cdf41, ok := dm.CDF("single", 41)
	require.True(t, ok)
	require.Equal(t, float64(0), cdf41)

	cdf42, ok := dm.CDF("single", 42)
	require.True(t, ok)
	require.Equal(t, float64(1), cdf42)

	cdf43, ok := dm.CDF("single", 43)
	require.True(t, ok)
	require.Equal(t, float64(1), cdf43)
}

func TestDigestMap_LargeDataset(t *testing.T) {
	dm := NewDigestMap()

	// Add a large number of values
	for i := 1; i <= 1000; i++ {
		dm.Add("large", float64(i))
	}

	// Test various percentile points
	cdf10, ok := dm.CDF("large", 100)
	require.True(t, ok)
	require.True(t, cdf10 >= 0.09 && cdf10 <= 0.11) // Should be around 10%

	cdf50, ok := dm.CDF("large", 500)
	require.True(t, ok)
	require.True(t, cdf50 >= 0.49 && cdf50 <= 0.51) // Should be around 50%

	cdf90, ok := dm.CDF("large", 900)
	require.True(t, ok)
	require.True(t, cdf90 >= 0.89 && cdf90 <= 0.91) // Should be around 90%
}

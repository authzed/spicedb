package query

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewQueryPlanMetadata(t *testing.T) {
	metadata := NewQueryPlanMetadata()
	require.NotNil(t, metadata)
	require.NotNil(t, metadata.stats)
	require.Empty(t, metadata.GetStats())
}

func TestQueryPlanMetadataMergeCountStats(t *testing.T) {
	metadata := NewQueryPlanMetadata()
	key1 := CanonicalKey("test-key-1")
	key2 := CanonicalKey("test-key-2")

	// First merge
	counts1 := map[CanonicalKey]CountStats{
		key1: {
			CheckCalls:           5,
			IterSubjectsCalls:    3,
			IterResourcesCalls:   2,
			CheckResults:         10,
			IterSubjectsResults:  6,
			IterResourcesResults: 4,
		},
		key2: {
			CheckCalls:           2,
			IterSubjectsCalls:    1,
			IterResourcesCalls:   1,
			CheckResults:         4,
			IterSubjectsResults:  2,
			IterResourcesResults: 2,
		},
	}

	metadata.MergeCountStats(counts1)

	stats := metadata.GetStats()
	require.Equal(t, counts1[key1], stats[key1])
	require.Equal(t, counts1[key2], stats[key2])

	// Second merge - should accumulate
	counts2 := map[CanonicalKey]CountStats{
		key1: {
			CheckCalls:           3,
			IterSubjectsCalls:    2,
			IterResourcesCalls:   1,
			CheckResults:         6,
			IterSubjectsResults:  4,
			IterResourcesResults: 2,
		},
	}

	metadata.MergeCountStats(counts2)

	stats = metadata.GetStats()
	expected := CountStats{
		CheckCalls:           8,  // 5 + 3
		IterSubjectsCalls:    5,  // 3 + 2
		IterResourcesCalls:   3,  // 2 + 1
		CheckResults:         16, // 10 + 6
		IterSubjectsResults:  10, // 6 + 4
		IterResourcesResults: 6,  // 4 + 2
	}
	require.Equal(t, expected, stats[key1])
	require.Equal(t, counts1[key2], stats[key2]) // key2 unchanged
}

func TestQueryPlanMetadataGetStatsReturnsACopy(t *testing.T) {
	metadata := NewQueryPlanMetadata()
	key := CanonicalKey("test-key")

	counts := map[CanonicalKey]CountStats{
		key: {
			CheckCalls:   5,
			CheckResults: 10,
		},
	}
	metadata.MergeCountStats(counts)

	stats1 := metadata.GetStats()
	stats2 := metadata.GetStats()

	// Should be equal
	require.Equal(t, stats1, stats2)

	// Modifying the returned map should not affect the metadata
	stats1[key] = CountStats{CheckCalls: 999}
	stats3 := metadata.GetStats()

	require.NotEqual(t, 999, stats3[key].CheckCalls)
	require.Equal(t, 5, stats3[key].CheckCalls)
}

func TestQueryPlanMetadataConcurrency(t *testing.T) {
	metadata := NewQueryPlanMetadata()
	key1 := CanonicalKey("test-key-1")
	key2 := CanonicalKey("test-key-2")

	var wg sync.WaitGroup
	iterations := 100

	// Goroutine 1 - merges stats for key1
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			metadata.MergeCountStats(map[CanonicalKey]CountStats{
				key1: {CheckCalls: 1, CheckResults: 2},
			})
		}
	}()

	// Goroutine 2 - merges stats for key2
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			metadata.MergeCountStats(map[CanonicalKey]CountStats{
				key2: {IterSubjectsCalls: 1, IterSubjectsResults: 3},
			})
		}
	}()

	// Goroutine 3 - reads stats
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = metadata.GetStats()
		}
	}()

	wg.Wait()

	// Verify final counts
	stats := metadata.GetStats()
	require.Equal(t, iterations, stats[key1].CheckCalls)
	require.Equal(t, iterations*2, stats[key1].CheckResults)
	require.Equal(t, iterations, stats[key2].IterSubjectsCalls)
	require.Equal(t, iterations*3, stats[key2].IterSubjectsResults)
}

func TestQueryPlanMetadataEmptyMerge(t *testing.T) {
	metadata := NewQueryPlanMetadata()

	// Merging empty map should not cause issues
	metadata.MergeCountStats(map[CanonicalKey]CountStats{})

	stats := metadata.GetStats()
	require.Empty(t, stats)
}

func TestQueryPlanMetadataMergeMultipleKeys(t *testing.T) {
	metadata := NewQueryPlanMetadata()

	// Merge multiple keys at once
	counts := map[CanonicalKey]CountStats{
		CanonicalKey("key1"): {CheckCalls: 1, CheckResults: 2},
		CanonicalKey("key2"): {IterSubjectsCalls: 3, IterSubjectsResults: 4},
		CanonicalKey("key3"): {IterResourcesCalls: 5, IterResourcesResults: 6},
	}

	metadata.MergeCountStats(counts)

	stats := metadata.GetStats()
	require.Len(t, stats, 3)
	require.Equal(t, counts[CanonicalKey("key1")], stats[CanonicalKey("key1")])
	require.Equal(t, counts[CanonicalKey("key2")], stats[CanonicalKey("key2")])
	require.Equal(t, counts[CanonicalKey("key3")], stats[CanonicalKey("key3")])
}

func TestQueryPlanMetadataInitialization(t *testing.T) {
	// Verify that NewQueryPlanMetadata returns a ready-to-use instance with empty stats.
	m := NewQueryPlanMetadata()
	require.NotNil(t, m)
	require.Empty(t, m.GetStats())
}

func TestQueryPlanMetadataAccumulatesAcrossQueries(t *testing.T) {
	metadata := NewQueryPlanMetadata()
	key := CanonicalKey("test-iterator")

	// Simulate first query execution
	firstQueryStats := map[CanonicalKey]CountStats{
		key: {
			CheckCalls:   2,
			CheckResults: 3,
		},
	}
	metadata.MergeCountStats(firstQueryStats)

	stats := metadata.GetStats()
	require.Equal(t, 2, stats[key].CheckCalls)
	require.Equal(t, 3, stats[key].CheckResults)

	// Simulate second query execution - should accumulate
	secondQueryStats := map[CanonicalKey]CountStats{
		key: {
			CheckCalls:   1,
			CheckResults: 2,
		},
	}
	metadata.MergeCountStats(secondQueryStats)

	stats = metadata.GetStats()
	require.Equal(t, 3, stats[key].CheckCalls, "Should accumulate: 2 + 1 = 3")
	require.Equal(t, 5, stats[key].CheckResults, "Should accumulate: 3 + 2 = 5")
}

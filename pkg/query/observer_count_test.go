package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestNewCountObserver(t *testing.T) {
	obs := NewCountObserver()
	require.NotNil(t, obs)
	require.Empty(t, obs.GetStats())
}

func TestCountObserverEnterIterator(t *testing.T) {
	obs := NewCountObserver()
	key := CanonicalKey("test-key")

	obs.ObserveEnterIterator(CheckOperation, key)
	obs.ObserveEnterIterator(IterSubjectsOperation, key)
	obs.ObserveEnterIterator(IterResourcesOperation, key)

	stats := obs.GetStats()[key]
	require.Equal(t, 1, stats.CheckCalls)
	require.Equal(t, 1, stats.IterSubjectsCalls)
	require.Equal(t, 1, stats.IterResourcesCalls)
}

func TestCountObserverPath(t *testing.T) {
	obs := NewCountObserver()
	key := CanonicalKey("test-key")
	path := Path{}

	obs.ObservePath(CheckOperation, key, path)
	obs.ObservePath(CheckOperation, key, path)
	obs.ObservePath(IterSubjectsOperation, key, path)
	obs.ObservePath(IterResourcesOperation, key, path)

	stats := obs.GetStats()[key]
	require.Equal(t, 2, stats.CheckResults)
	require.Equal(t, 1, stats.IterSubjectsResults)
	require.Equal(t, 1, stats.IterResourcesResults)
}

func TestCountObserverReturnIterator(t *testing.T) {
	obs := NewCountObserver()
	key := CanonicalKey("test-key")

	obs.ObserveEnterIterator(CheckOperation, key)
	obs.ObserveReturnIterator(CheckOperation, key)

	// Should still have the call count
	stats := obs.GetStats()[key]
	require.Equal(t, 1, stats.CheckCalls)
}

func TestCountObserverGetStatsReturnsACopy(t *testing.T) {
	obs := NewCountObserver()
	key := CanonicalKey("test-key")
	obs.ObserveEnterIterator(CheckOperation, key)

	stats1 := obs.GetStats()
	stats2 := obs.GetStats()
	require.Equal(t, stats1, stats2)

	// Modifying the returned map should not affect the observer
	stats1[key] = CountStats{CheckCalls: 999}
	stats3 := obs.GetStats()
	require.NotEqual(t, 999, stats3[key].CheckCalls)
	require.Equal(t, 1, stats3[key].CheckCalls)
}

func TestAggregateCountStatsEmpty(t *testing.T) {
	result := AggregateCountStats(map[CanonicalKey]CountStats{})
	require.Equal(t, CountStats{}, result)
}

func TestAggregateCountStatsSingleEntry(t *testing.T) {
	stats := map[CanonicalKey]CountStats{
		"k1": {
			CheckCalls:           5,
			IterSubjectsCalls:    3,
			IterResourcesCalls:   2,
			CheckResults:         10,
			IterSubjectsResults:  6,
			IterResourcesResults: 4,
		},
	}
	result := AggregateCountStats(stats)
	require.Equal(t, stats["k1"], result)
}

func TestAggregateCountStatsMultipleEntries(t *testing.T) {
	stats := map[CanonicalKey]CountStats{
		"k1": {
			CheckCalls:           5,
			IterSubjectsCalls:    3,
			IterResourcesCalls:   2,
			CheckResults:         10,
			IterSubjectsResults:  6,
			IterResourcesResults: 4,
		},
		"k2": {
			CheckCalls:           3,
			IterSubjectsCalls:    2,
			IterResourcesCalls:   1,
			CheckResults:         6,
			IterSubjectsResults:  4,
			IterResourcesResults: 2,
		},
		"k3": {
			CheckCalls:           2,
			IterSubjectsCalls:    1,
			IterResourcesCalls:   1,
			CheckResults:         4,
			IterSubjectsResults:  2,
			IterResourcesResults: 2,
		},
	}
	result := AggregateCountStats(stats)
	expected := CountStats{
		CheckCalls:           10,
		IterSubjectsCalls:    6,
		IterResourcesCalls:   4,
		CheckResults:         20,
		IterSubjectsResults:  12,
		IterResourcesResults: 8,
	}
	require.Equal(t, expected, result)
}

func TestCountObserverIntegration(t *testing.T) {
	// Create a datastore
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	// Create a simple iterator
	fixed := NewFixedIterator(
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc1"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."},
		},
		Path{
			Resource: Object{ObjectType: "document", ObjectID: "doc2"},
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "bob", Relation: "..."},
		},
	)

	// Create a context with count observer enabled
	countObs := NewCountObserver()
	ctx := NewLocalContext(context.Background(),
		WithReader(datalayer.NewDataLayer(ds).SnapshotReader(datastore.NoRevision, datalayer.NoSchemaHashForTesting)),
		WithObserver(countObs))

	// Execute a Check operation
	resources := []Object{{ObjectType: "document", ObjectID: "doc1"}}
	subject := ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: "..."}
	pathSeq, err := ctx.Check(fixed, resources, subject)
	require.NoError(t, err)

	// Consume the results
	paths, err := CollectAll(pathSeq)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	// Verify stats were recorded
	countStats := countObs.GetStats()
	stats, exists := countStats[fixed.CanonicalKey()]
	require.True(t, exists, "Stats should exist for iterator")
	require.Equal(t, 1, stats.CheckCalls, "Should have 1 Check call")
	require.Equal(t, 1, stats.CheckResults, "Should have 1 Check result")

	// Verify no timing is tracked (all should be zero)
	// CountStats has no timing fields, so this just verifies the type
	require.IsType(t, CountStats{}, stats)
}

func TestCountObserverMultipleCalls(t *testing.T) {
	obs := NewCountObserver()
	key := CanonicalKey("test-key")

	// Simulate multiple calls to the same iterator
	for i := 0; i < 3; i++ {
		obs.ObserveEnterIterator(CheckOperation, key)
		for j := 0; j < 2; j++ {
			obs.ObservePath(CheckOperation, key, Path{})
		}
		obs.ObserveReturnIterator(CheckOperation, key)
	}

	stats := obs.GetStats()[key]
	require.Equal(t, 3, stats.CheckCalls, "Should have 3 calls")
	require.Equal(t, 6, stats.CheckResults, "Should have 6 results (2 per call)")
}

func TestCountObserverConcurrency(t *testing.T) {
	obs := NewCountObserver()
	key1 := CanonicalKey("test-key-1")
	key2 := CanonicalKey("test-key-2")

	done := make(chan bool, 2)

	// Goroutine 1 - updates key1
	go func() {
		for i := 0; i < 100; i++ {
			obs.ObserveEnterIterator(CheckOperation, key1)
			obs.ObservePath(CheckOperation, key1, Path{})
			obs.ObserveReturnIterator(CheckOperation, key1)
		}
		done <- true
	}()

	// Goroutine 2 - updates key2
	go func() {
		for i := 0; i < 100; i++ {
			obs.ObserveEnterIterator(IterSubjectsOperation, key2)
			obs.ObservePath(IterSubjectsOperation, key2, Path{})
			obs.ObserveReturnIterator(IterSubjectsOperation, key2)
		}
		done <- true
	}()

	// Wait for both to complete
	<-done
	<-done

	// Verify counts
	stats := obs.GetStats()
	require.Equal(t, 100, stats[key1].CheckCalls)
	require.Equal(t, 100, stats[key1].CheckResults)
	require.Equal(t, 100, stats[key2].IterSubjectsCalls)
	require.Equal(t, 100, stats[key2].IterSubjectsResults)
}

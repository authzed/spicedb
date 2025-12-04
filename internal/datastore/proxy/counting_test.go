package proxy

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestCountingProxyBasicCounting(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)
	reader.On("ReverseQueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)
	reader.On("ReadNamespaceByName", mock.Anything, mock.Anything).Return(&core.NamespaceDefinition{}, datastore.NoRevision, nil)
	reader.On("ListAllNamespaces", mock.Anything).Return([]datastore.RevisionedNamespace{}, nil)
	reader.On("LookupNamespacesWithNames", mock.Anything, mock.Anything).Return([]datastore.RevisionedNamespace{}, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()

	// Verify all counters start at 0
	require.Equal(uint64(0), counts.QueryRelationships())
	require.Equal(uint64(0), counts.ReverseQueryRelationships())
	require.Equal(uint64(0), counts.ReadNamespaceByName())
	require.Equal(uint64(0), counts.ListAllNamespaces())
	require.Equal(uint64(0), counts.LookupNamespacesWithNames())

	r := ds.SnapshotReader(datastore.NoRevision)

	// Call each method once
	_, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(1), counts.QueryRelationships())

	_, err = r.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{SubjectType: "user"})
	require.NoError(err)
	require.Equal(uint64(1), counts.ReverseQueryRelationships())

	_, _, err = r.ReadNamespaceByName(ctx, "test")
	require.NoError(err)
	require.Equal(uint64(1), counts.ReadNamespaceByName())

	_, err = r.ListAllNamespaces(ctx)
	require.NoError(err)
	require.Equal(uint64(1), counts.ListAllNamespaces())

	_, err = r.LookupNamespacesWithNames(ctx, []string{"test"})
	require.NoError(err)
	require.Equal(uint64(1), counts.LookupNamespacesWithNames())
}

func TestCountingProxyMultipleCalls(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()
	r := ds.SnapshotReader(datastore.NoRevision)

	require.Equal(uint64(0), counts.QueryRelationships())

	// Call multiple times
	_, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(1), counts.QueryRelationships())

	_, err = r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(2), counts.QueryRelationships())

	_, err = r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(3), counts.QueryRelationships())
}

func TestCountingProxyCaveatMethodsNotCounted(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("ReadCaveatByName", "test").Return(&core.CaveatDefinition{}, datastore.NoRevision, nil)
	reader.On("ListAllCaveats").Return([]datastore.RevisionedCaveat{}, nil)
	reader.On("LookupCaveatsWithNames", mock.Anything).Return([]datastore.RevisionedCaveat{}, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()
	r := ds.SnapshotReader(datastore.NoRevision)

	// Call caveat methods
	_, _, err := r.ReadCaveatByName(ctx, "test")
	require.NoError(err)
	_, err = r.ListAllCaveats(ctx)
	require.NoError(err)
	_, err = r.LookupCaveatsWithNames(ctx, []string{"test"})
	require.NoError(err)

	// Verify no counters changed
	require.Equal(uint64(0), counts.QueryRelationships())
	require.Equal(uint64(0), counts.ReverseQueryRelationships())
	require.Equal(uint64(0), counts.ReadNamespaceByName())
	require.Equal(uint64(0), counts.ListAllNamespaces())
	require.Equal(uint64(0), counts.LookupNamespacesWithNames())

	delegate.AssertExpectations(t)
}

func TestCountingProxyCounterMethodsNotCounted(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("CountRelationships", "counter1").Return(42, nil)
	reader.On("LookupCounters").Return([]datastore.RelationshipCounter{}, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()
	r := ds.SnapshotReader(datastore.NoRevision)

	// Call counter methods
	_, err := r.CountRelationships(ctx, "counter1")
	require.NoError(err)
	_, err = r.LookupCounters(ctx)
	require.NoError(err)

	// Verify no counters changed
	require.Equal(uint64(0), counts.QueryRelationships())
	require.Equal(uint64(0), counts.ReverseQueryRelationships())
	require.Equal(uint64(0), counts.ReadNamespaceByName())
	require.Equal(uint64(0), counts.ListAllNamespaces())
	require.Equal(uint64(0), counts.LookupNamespacesWithNames())

	delegate.AssertExpectations(t)
}

func TestCountingProxyThreadSafety(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()

	numGoroutines := uint64(100)
	callsPerGoroutine := uint64(100)

	var wg sync.WaitGroup
	for i := uint64(0); i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := ds.SnapshotReader(datastore.NoRevision)
			for range callsPerGoroutine {
				_, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
				require.NoError(err)
			}
		}()
	}

	wg.Wait()

	// Should have exactly numGoroutines * callsPerGoroutine calls
	expected := numGoroutines * callsPerGoroutine
	require.Equal(expected, counts.QueryRelationships())
}

func TestCountingProxyUnwrap(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}

	ds, _ := NewCountingDatastoreProxy(delegate)

	// Access unwrap directly since we wrap ReadOnlyDatastore, not Datastore
	unwrapped := ds.(*countingProxy).Unwrap()
	require.Equal(delegate, unwrapped)
}

func TestCountingProxyPassthrough(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	ds, _ := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()

	// Test MetricsID (mock returns hardcoded "mock")
	metricsID, err := ds.MetricsID()
	require.NoError(err)
	require.Equal("mock", metricsID)

	// Test UniqueID (mock returns hardcoded "mockds" if CurrentUniqueID is empty)
	uniqueID, err := ds.UniqueID(ctx)
	require.NoError(err)
	require.Equal("mockds", uniqueID)

	// Test HeadRevision
	delegate.On("HeadRevision", mock.Anything).Return(datastore.NoRevision, nil).Once()
	_, err = ds.HeadRevision(ctx)
	require.NoError(err)

	// Test OptimizedRevision
	delegate.On("OptimizedRevision", mock.Anything).Return(datastore.NoRevision, nil).Once()
	_, err = ds.OptimizedRevision(ctx)
	require.NoError(err)

	// Test CheckRevision
	delegate.On("CheckRevision", datastore.NoRevision).Return(nil).Once()
	err = ds.CheckRevision(ctx, datastore.NoRevision)
	require.NoError(err)

	// Test RevisionFromString
	delegate.On("RevisionFromString", "test").Return(datastore.NoRevision, nil).Once()
	_, err = ds.RevisionFromString("test")
	require.NoError(err)

	// Test ReadyState
	delegate.On("ReadyState", mock.Anything).Return(datastore.ReadyState{IsReady: true}, nil).Once()
	state, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(state.IsReady)

	// Test Features
	features := &datastore.Features{}
	delegate.On("Features", mock.Anything).Return(features, nil).Once()
	returnedFeatures, err := ds.Features(ctx)
	require.NoError(err)
	require.Equal(features, returnedFeatures)

	// Test OfflineFeatures
	delegate.On("OfflineFeatures").Return(features, nil).Once()
	returnedFeatures, err = ds.OfflineFeatures()
	require.NoError(err)
	require.Equal(features, returnedFeatures)

	// Test Statistics
	stats := datastore.Stats{}
	delegate.On("Statistics", mock.Anything).Return(stats, nil).Once()
	returnedStats, err := ds.Statistics(ctx)
	require.NoError(err)
	require.Equal(stats, returnedStats)

	// Test Close
	delegate.On("Close").Return(nil).Once()
	err = ds.Close()
	require.NoError(err)

	delegate.AssertExpectations(t)
}

func TestCountingProxyMultipleReaders(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader1 := &proxy_test.MockReader{}
	reader2 := &proxy_test.MockReader{}

	// First snapshot reader
	delegate.On("SnapshotReader", datastore.NoRevision).Return(reader1).Once()
	reader1.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	// Second snapshot reader
	delegate.On("SnapshotReader", mock.Anything).Return(reader2)
	reader2.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()

	// Create two readers and make calls on each
	r1 := ds.SnapshotReader(datastore.NoRevision)
	r2 := ds.SnapshotReader(datastore.NoRevision)

	_, err := r1.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(1), counts.QueryRelationships())

	_, err = r2.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(2), counts.QueryRelationships())

	_, err = r1.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	require.Equal(uint64(3), counts.QueryRelationships())
}

func TestWriteMethodCounts(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	reader := &proxy_test.MockReader{}

	delegate.On("SnapshotReader", mock.Anything).Return(reader)
	reader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)
	reader.On("ReadNamespaceByName", "test").Return(&core.NamespaceDefinition{}, datastore.NoRevision, nil)

	ds, counts := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()
	r := ds.SnapshotReader(datastore.NoRevision)

	// Make some calls
	_, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	_, err = r.QueryRelationships(ctx, datastore.RelationshipsFilter{})
	require.NoError(err)
	_, _, err = r.ReadNamespaceByName(ctx, "test")
	require.NoError(err)

	require.Equal(uint64(2), counts.QueryRelationships())
	require.Equal(uint64(1), counts.ReadNamespaceByName())

	// WriteMethodCounts should not panic and should be callable
	// Note: We can't easily test the actual Prometheus counters without
	// setting up a full Prometheus test environment, but we can verify
	// the function executes without error
	require.NotPanics(func() {
		WriteMethodCounts(counts)
	})
}

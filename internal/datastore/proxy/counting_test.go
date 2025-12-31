package proxy

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestCountingProxyBasicCounting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	reader.EXPECT().ReverseQueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	reader.EXPECT().ReadNamespaceByName(gomock.Any(), gomock.Any()).Return(&core.NamespaceDefinition{}, datastore.NoRevision, nil).AnyTimes()
	reader.EXPECT().ListAllNamespaces(gomock.Any()).Return([]datastore.RevisionedNamespace{}, nil).AnyTimes()
	reader.EXPECT().LookupNamespacesWithNames(gomock.Any(), gomock.Any()).Return([]datastore.RevisionedNamespace{}, nil).AnyTimes()

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().ReadCaveatByName(gomock.Any(), "test").Return(&core.CaveatDefinition{}, datastore.NoRevision, nil).AnyTimes()
	reader.EXPECT().ListAllCaveats(gomock.Any()).Return([]datastore.RevisionedCaveat{}, nil).AnyTimes()
	reader.EXPECT().LookupCaveatsWithNames(gomock.Any(), gomock.Any()).Return([]datastore.RevisionedCaveat{}, nil).AnyTimes()

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
}

func TestCountingProxyCounterMethodsNotCounted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().CountRelationships(gomock.Any(), "counter1").Return(42, nil).AnyTimes()
	reader.EXPECT().LookupCounters(gomock.Any()).Return([]datastore.RelationshipCounter{}, nil).AnyTimes()

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
}

func TestCountingProxyThreadSafety(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

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
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	// Should have exactly numGoroutines * callsPerGoroutine calls
	expected := numGoroutines * callsPerGoroutine
	require.Equal(expected, counts.QueryRelationships())
}

func TestCountingProxyUnwrap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)

	ds, _ := NewCountingDatastoreProxy(delegate)

	// Access unwrap directly since we wrap ReadOnlyDatastore, not Datastore
	unwrapped := ds.(*countingProxy).Unwrap()
	require.Equal(delegate, unwrapped)
}

func TestCountingProxyPassthrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	ds, _ := NewCountingDatastoreProxy(delegate)
	ctx := context.Background()

	// Test MetricsID (mock returns hardcoded "mock")
	delegate.EXPECT().MetricsID().Return("mock", nil).Times(1)
	metricsID, err := ds.MetricsID()
	require.NoError(err)
	require.Equal("mock", metricsID)

	// Test UniqueID (mock returns hardcoded "mockds" if CurrentUniqueID is empty)
	delegate.EXPECT().UniqueID(gomock.Any()).Return("mockds", nil).Times(1)
	uniqueID, err := ds.UniqueID(ctx)
	require.NoError(err)
	require.Equal("mockds", uniqueID)

	// Test HeadRevision
	delegate.EXPECT().HeadRevision(gomock.Any()).Return(datastore.NoRevision, nil).Times(1)
	_, err = ds.HeadRevision(ctx)
	require.NoError(err)

	// Test OptimizedRevision
	delegate.EXPECT().OptimizedRevision(gomock.Any()).Return(datastore.NoRevision, nil).Times(1)
	_, err = ds.OptimizedRevision(ctx)
	require.NoError(err)

	// Test CheckRevision
	delegate.EXPECT().CheckRevision(gomock.Any(), datastore.NoRevision).Return(nil).Times(1)
	err = ds.CheckRevision(ctx, datastore.NoRevision)
	require.NoError(err)

	// Test RevisionFromString
	delegate.EXPECT().RevisionFromString("test").Return(datastore.NoRevision, nil).Times(1)
	_, err = ds.RevisionFromString("test")
	require.NoError(err)

	// Test ReadyState
	delegate.EXPECT().ReadyState(gomock.Any()).Return(datastore.ReadyState{IsReady: true}, nil).Times(1)
	state, err := ds.ReadyState(ctx)
	require.NoError(err)
	require.True(state.IsReady)

	// Test Features
	features := &datastore.Features{}
	delegate.EXPECT().Features(gomock.Any()).Return(features, nil).Times(1)
	returnedFeatures, err := ds.Features(ctx)
	require.NoError(err)
	require.Equal(features, returnedFeatures)

	// Test OfflineFeatures
	delegate.EXPECT().OfflineFeatures().Return(features, nil).Times(1)
	returnedFeatures, err = ds.OfflineFeatures()
	require.NoError(err)
	require.Equal(features, returnedFeatures)

	// Test Statistics
	stats := datastore.Stats{}
	delegate.EXPECT().Statistics(gomock.Any()).Return(stats, nil).Times(1)
	returnedStats, err := ds.Statistics(ctx)
	require.NoError(err)
	require.Equal(stats, returnedStats)

	// Test Close
	delegate.EXPECT().Close().Return(nil).Times(1)
	err = ds.Close()
	require.NoError(err)
}

func TestCountingProxyMultipleReaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader1 := mocks.NewMockReader(ctrl)
	reader2 := mocks.NewMockReader(ctrl)

	// First snapshot reader
	delegate.EXPECT().SnapshotReader(datastore.NoRevision).Return(reader1).Times(1)
	reader1.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	// Second snapshot reader
	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader2).AnyTimes()
	reader2.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	delegate := mocks.NewMockDatastore(ctrl)
	reader := mocks.NewMockReader(ctrl)

	delegate.EXPECT().SnapshotReader(gomock.Any()).Return(reader).AnyTimes()
	reader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	reader.EXPECT().ReadNamespaceByName(gomock.Any(), "test").Return(&core.NamespaceDefinition{}, datastore.NoRevision, nil).AnyTimes()

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

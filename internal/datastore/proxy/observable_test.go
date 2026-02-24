package proxy

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var testRev = revisions.NewForTransactionID(42)

func floatPtr(f float64) *float64 { return &f }

func histogramSampleCount(m prometheus.Metric) uint64 {
	pb := &dto.Metric{}
	_ = m.Write(pb)
	return pb.GetHistogram().GetSampleCount()
}

func histogramSampleSum(m prometheus.Metric) float64 {
	pb := &dto.Metric{}
	_ = m.Write(pb)
	return pb.GetHistogram().GetSampleSum()
}

func requireObservedLatency(t *testing.T, operation string, f func()) {
	t.Helper()
	obs := queryLatency.WithLabelValues(operation, "(none)")
	before := histogramSampleCount(obs.(prometheus.Metric))
	f()
	after := histogramSampleCount(obs.(prometheus.Metric))
	require.Greater(t, after, before, "expected latency metric for %s", operation)
}

func requireObservedRelationshipCount(t *testing.T, expectedCount float64, f func()) {
	t.Helper()
	beforeCount := histogramSampleCount(loadedRelationshipCount)
	beforeSum := histogramSampleSum(loadedRelationshipCount)
	f()
	afterCount := histogramSampleCount(loadedRelationshipCount)
	afterSum := histogramSampleSum(loadedRelationshipCount)
	require.Greater(t, afterCount, beforeCount, "expected loadedRelationshipCount metric to be recorded")
	require.InDelta(t, expectedCount, afterSum-beforeSum, 0.01, "expected relationship count to match")
}

func newMocks() (*proxy_test.MockDatastore, *proxy_test.MockReader, *proxy_test.MockReadWriteTransaction) {
	dsMock := &proxy_test.MockDatastore{}
	readerMock := &proxy_test.MockReader{}
	dsMock.On("SnapshotReader", mock.Anything).Return(readerMock).Maybe()
	rwtMock := &proxy_test.MockReadWriteTransaction{}
	dsMock.On("ReadWriteTx", mock.Anything).Return(rwtMock, testRev, nil).Maybe()
	return dsMock, readerMock, rwtMock
}

func TestObservableProxy_DatastoreMethodsWithMetrics(t *testing.T) {
	tests := []struct {
		name      string
		metricOp  string
		setupMock func(ds *proxy_test.MockDatastore)
		call      func(t *testing.T, ds datastore.Datastore)
	}{
		{
			name:     "OptimizedRevision",
			metricOp: "OptimizedRevision",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHash{Revision: testRev}, nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				_, err := ds.OptimizedRevision(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "HeadRevision",
			metricOp: "HeadRevision",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("HeadRevision").Return(datastore.RevisionWithSchemaHash{Revision: testRev}, nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				_, err := ds.HeadRevision(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "CheckRevision",
			metricOp: "CheckRevision",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("CheckRevision", testRev).Return(nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				require.NoError(t, ds.CheckRevision(t.Context(), testRev))
			},
		},
		{
			name:     "ReadyState",
			metricOp: "ReadyState",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("ReadyState").Return(datastore.ReadyState{IsReady: true}, nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				_, err := ds.ReadyState(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "Features",
			metricOp: "Features",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("Features").Return(&datastore.Features{}, nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				_, err := ds.Features(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "Statistics",
			metricOp: "Statistics",
			setupMock: func(ds *proxy_test.MockDatastore) {
				ds.On("Statistics").Return(datastore.Stats{}, nil).Once()
			},
			call: func(t *testing.T, ds datastore.Datastore) {
				_, err := ds.Statistics(t.Context())
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsMock, _, _ := newMocks()
			tt.setupMock(dsMock)
			sut := NewObservableDatastoreProxy(dsMock)

			requireObservedLatency(t, tt.metricOp, func() { tt.call(t, sut) })
			dsMock.AssertExpectations(t)
		})
	}
}

func TestObservableProxy_DatastorePassthroughMethods(t *testing.T) {
	t.Run("RevisionFromString", func(t *testing.T) {
		dsMock, _, _ := newMocks()
		dsMock.On("RevisionFromString", "42").Return(testRev, nil).Once()
		sut := NewObservableDatastoreProxy(dsMock)

		rev, err := sut.RevisionFromString("42")
		require.NoError(t, err)
		require.Equal(t, testRev, rev)
		dsMock.AssertExpectations(t)
	})

	t.Run("Watch", func(t *testing.T) {
		dsMock, _, _ := newMocks()
		changesCh := make(<-chan datastore.RevisionChanges)
		errCh := make(<-chan error)
		dsMock.On("Watch", testRev).Return(changesCh, errCh).Once()
		sut := NewObservableDatastoreProxy(dsMock)

		gotChanges, gotErr := sut.Watch(t.Context(), testRev, datastore.WatchJustRelationships())
		require.Equal(t, changesCh, gotChanges)
		require.Equal(t, errCh, gotErr)
		dsMock.AssertExpectations(t)
	})

	t.Run("Close", func(t *testing.T) {
		dsMock, _, _ := newMocks()
		dsMock.On("Close").Return(nil).Once()

		sut := NewObservableDatastoreProxy(dsMock)
		require.NoError(t, sut.Close())
		dsMock.AssertExpectations(t)
	})

	t.Run("OfflineFeatures", func(t *testing.T) {
		dsMock, _, _ := newMocks()
		dsMock.On("OfflineFeatures").Return(&datastore.Features{}, nil).Once()
		sut := NewObservableDatastoreProxy(dsMock)
		_, err := sut.OfflineFeatures()
		require.NoError(t, err)
		dsMock.AssertExpectations(t)
	})

	t.Run("Unwrap", func(t *testing.T) {
		dsMock, _, _ := newMocks()
		sut := NewObservableDatastoreProxy(dsMock)
		require.Equal(t, dsMock, sut.(datastore.UnwrappableDatastore).Unwrap())
	})
}

func TestObservableProxy_ReaderMethodsWithMetrics(t *testing.T) {
	tests := []struct {
		name             string
		metricOp         string
		expectedRelCount *float64
		setupMock        func(r *proxy_test.MockReader)
		call             func(t *testing.T, reader datastore.Reader)
	}{
		{
			name:      "CountRelationships",
			metricOp:  "CountRelationships",
			setupMock: func(r *proxy_test.MockReader) { r.On("CountRelationships", "c1").Return(42, nil).Once() },
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.CountRelationships(t.Context(), "c1")
				require.NoError(t, err)
			},
		},
		{
			name:     "LookupCounters",
			metricOp: "LookupCounters",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LookupCounters").Return([]datastore.RelationshipCounter(nil), nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.LookupCounters(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyReadCaveatByName",
			metricOp: "ReadCaveatByName",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyReadCaveatByName", "caveat1").Return((*core.CaveatDefinition)(nil), testRev, nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, _, err := reader.LegacyReadCaveatByName(t.Context(), "caveat1")
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyLookupCaveatsWithNames",
			metricOp: "LookupCaveatsWithNames",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyLookupCaveatsWithNames", []string{"c1"}).Return([]datastore.RevisionedCaveat(nil), nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.LegacyLookupCaveatsWithNames(t.Context(), []string{"c1"})
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyListAllCaveats",
			metricOp: "ListAllCaveats",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyListAllCaveats").Return([]datastore.RevisionedCaveat(nil), nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.LegacyListAllCaveats(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyListAllNamespaces",
			metricOp: "ListAllNamespaces",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyListAllNamespaces").Return([]datastore.RevisionedNamespace(nil), nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.LegacyListAllNamespaces(t.Context())
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyLookupNamespacesWithNames",
			metricOp: "LookupNamespacesWithNames",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyLookupNamespacesWithNames", []string{"ns1"}).Return([]datastore.RevisionedNamespace(nil), nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, err := reader.LegacyLookupNamespacesWithNames(t.Context(), []string{"ns1"})
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyReadNamespaceByName",
			metricOp: "ReadNamespaceByName",
			setupMock: func(r *proxy_test.MockReader) {
				r.On("LegacyReadNamespaceByName", "ns1").Return((*core.NamespaceDefinition)(nil), testRev, nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				_, _, err := reader.LegacyReadNamespaceByName(t.Context(), "ns1")
				require.NoError(t, err)
			},
		},
		{
			name:             "QueryRelationships",
			metricOp:         "QueryRelationships",
			expectedRelCount: floatPtr(2),
			setupMock: func(r *proxy_test.MockReader) {
				twoRelIter := datastore.RelationshipIterator(func(yield func(tuple.Relationship, error) bool) {
					rel := tuple.MustParse("document:1#viewer@user:1")
					if !yield(rel, nil) {
						return
					}
					rel = tuple.MustParse("document:1#viewer@user:2")
					yield(rel, nil)
				})
				r.On("QueryRelationships", datastore.RelationshipsFilter{OptionalResourceType: "document"}).Return(twoRelIter, nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"})
				require.NoError(t, err)
				var count int
				for range iter {
					count++
				}
				require.Equal(t, 2, count)
			},
		},
		{
			name:             "ReverseQueryRelationships",
			metricOp:         "ReverseQueryRelationships",
			expectedRelCount: floatPtr(1),
			setupMock: func(r *proxy_test.MockReader) {
				oneRelIter := datastore.RelationshipIterator(func(yield func(tuple.Relationship, error) bool) {
					yield(tuple.MustParse("document:1#viewer@user:1"), nil)
				})
				r.On("ReverseQueryRelationships", datastore.SubjectsFilter{SubjectType: "user"}).Return(oneRelIter, nil).Once()
			},
			call: func(t *testing.T, reader datastore.Reader) {
				iter, err := reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{SubjectType: "user"})
				require.NoError(t, err)
				var count int
				for range iter {
					count++
				}
				require.Equal(t, 1, count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsMock, readerMock, _ := newMocks()
			tt.setupMock(readerMock)

			sut := NewObservableDatastoreProxy(dsMock)
			invoke := func() { tt.call(t, sut.SnapshotReader(testRev)) }
			requireObservedLatency(t, tt.metricOp, func() {
				if tt.expectedRelCount != nil {
					requireObservedRelationshipCount(t, *tt.expectedRelCount, invoke)
				} else {
					invoke()
				}
			})
			dsMock.AssertExpectations(t)
			readerMock.AssertExpectations(t)
		})
	}
}

func TestObservableProxy_RWTMethodsWithMetrics(t *testing.T) {
	tests := []struct {
		name      string
		metricOp  string
		setupMock func(rwt *proxy_test.MockReadWriteTransaction)
		call      func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction)
	}{
		{
			name:     "RegisterCounter",
			metricOp: "RegisterCounter",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("RegisterCounter", "counter1", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.RegisterCounter(ctx, "counter1", &core.RelationshipFilter{}))
			},
		},
		{
			name:     "UnregisterCounter",
			metricOp: "UnregisterCounter",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("UnregisterCounter", "counter1").Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.UnregisterCounter(ctx, "counter1"))
			},
		},
		{
			name:     "StoreCounterValue",
			metricOp: "StoreCounterValue",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("StoreCounterValue", "counter1", 10, testRev).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.StoreCounterValue(ctx, "counter1", 10, testRev))
			},
		},
		{
			name:     "WriteRelationships",
			metricOp: "WriteRelationships",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("WriteRelationships", []tuple.RelationshipUpdate(nil)).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.WriteRelationships(ctx, nil))
			},
		},
		{
			name:     "DeleteRelationships",
			metricOp: "DeleteRelationships",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("DeleteRelationships", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{ResourceType: "document"})
				require.NoError(t, err)
			},
		},
		{
			name:     "BulkLoad",
			metricOp: "BulkLoad",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("BulkLoad", mock.Anything).Return(0, nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				_, err := rwt.BulkLoad(ctx, nil)
				require.NoError(t, err)
			},
		},
		{
			name:     "LegacyWriteNamespaces",
			metricOp: "WriteNamespaces",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("LegacyWriteNamespaces", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "ns1"}))
			},
		},
		{
			name:     "LegacyDeleteNamespaces",
			metricOp: "DeleteNamespaces",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("LegacyDeleteNamespaces", "ns1", datastore.DeleteNamespacesAndRelationships).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.LegacyDeleteNamespaces(ctx, []string{"ns1"}, datastore.DeleteNamespacesAndRelationships))
			},
		},
		{
			name:     "LegacyWriteCaveats",
			metricOp: "WriteCaveats",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("LegacyWriteCaveats", mock.Anything).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{{Name: "c1"}}))
			},
		},
		{
			name:     "LegacyDeleteCaveats",
			metricOp: "DeleteCaveats",
			setupMock: func(rwt *proxy_test.MockReadWriteTransaction) {
				rwt.On("LegacyDeleteCaveats", []string{"c1"}).Return(nil).Once()
			},
			call: func(t *testing.T, ctx context.Context, rwt datastore.ReadWriteTransaction) {
				require.NoError(t, rwt.LegacyDeleteCaveats(ctx, []string{"c1"}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsMock, _, rwtMock := newMocks()
			tt.setupMock(rwtMock)
			sut := NewObservableDatastoreProxy(dsMock)

			requireObservedLatency(t, tt.metricOp, func() {
				_, err := sut.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					tt.call(t, ctx, rwt)
					return nil
				})
				require.NoError(t, err)
			})
			dsMock.AssertExpectations(t)
			rwtMock.AssertExpectations(t)
		})
	}
}

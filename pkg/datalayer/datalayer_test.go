package datalayer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newTestDataLayer(t testing.TB) (DataLayer, datastore.Datastore) {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ds.Close()
	})
	return NewDataLayer(ds), ds
}

// TestDefaultDataLayer_PassThroughs exercises the trivial pass-through methods on
// defaultDataLayer in impl.go.
func TestDefaultDataLayer_PassThroughs(t *testing.T) {
	dl, underlying := newTestDataLayer(t)
	ctx := t.Context()

	readyState, err := dl.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	features, err := dl.Features(ctx)
	require.NoError(t, err)
	require.NotNil(t, features)

	offline, err := dl.OfflineFeatures()
	require.NoError(t, err)
	require.NotNil(t, offline)

	_, err = dl.Statistics(ctx)
	require.NoError(t, err)

	metricsID, err := dl.MetricsID()
	require.NoError(t, err)
	require.NotEmpty(t, metricsID)

	uniqueID, err := dl.UniqueID(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, uniqueID)

	rev, err := dl.HeadRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, dl.CheckRevision(ctx, rev))

	optRev, err := dl.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	roundTripped, err := dl.RevisionFromString(rev.String())
	require.NoError(t, err)
	require.True(t, roundTripped.Equal(rev))

	watchCh, errCh := dl.Watch(ctx, rev, datastore.WatchOptions{Content: datastore.WatchRelationships})
	require.NotNil(t, watchCh)
	require.NotNil(t, errCh)

	require.Equal(t, underlying, UnwrapDatastore(dl))
	require.NoError(t, dl.Close())
}

// TestCountingDataLayer_CountsQueryAndReverseQuery ensures QueryRelationships
// and ReverseQueryRelationships increment their counters on both the snapshot
// reader and the RW transaction.
func TestCountingDataLayer_CountsQueryAndReverseQuery(t *testing.T) {
	baseDL, _ := newTestDataLayer(t)
	counting, counts := NewCountingDataLayer(baseDL)
	ctx := t.Context()

	_, err := counting.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
		})
	})
	require.NoError(t, err)

	rev, err := counting.HeadRevision(ctx)
	require.NoError(t, err)

	reader := counting.SnapshotReader(rev)
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)
	_, err = datastore.IteratorToSlice(it)
	require.NoError(t, err)
	require.Equal(t, uint64(1), counts.QueryRelationships())

	rit, err := reader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{SubjectType: "user"})
	require.NoError(t, err)
	_, err = datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Equal(t, uint64(1), counts.ReverseQueryRelationships())

	// RWT also counts queries.
	_, err = counting.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		it, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType: "resource",
		})
		require.NoError(t, err)
		_, err = datastore.IteratorToSlice(it)
		require.NoError(t, err)

		rit, err := rwt.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{SubjectType: "user"})
		require.NoError(t, err)
		_, err = datastore.IteratorToSlice(rit)
		require.NoError(t, err)

		// CountRelationships and LookupCounters delegate without counting.
		_, _ = rwt.CountRelationships(ctx, "nonexistent")
		_, _ = rwt.LookupCounters(ctx)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, uint64(2), counts.QueryRelationships())
	require.Equal(t, uint64(2), counts.ReverseQueryRelationships())
}

// TestCountingDataLayer_PassThroughs exercises the trivial pass-through methods
// on countingDataLayer.
func TestCountingDataLayer_PassThroughs(t *testing.T) {
	base, _ := newTestDataLayer(t)
	counting, _ := NewCountingDataLayer(base)
	ctx := t.Context()

	readyState, err := counting.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	features, err := counting.Features(ctx)
	require.NoError(t, err)
	require.NotNil(t, features)

	offline, err := counting.OfflineFeatures()
	require.NoError(t, err)
	require.NotNil(t, offline)

	_, err = counting.Statistics(ctx)
	require.NoError(t, err)

	metricsID, err := counting.MetricsID()
	require.NoError(t, err)
	require.NotEmpty(t, metricsID)

	uniqueID, err := counting.UniqueID(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, uniqueID)

	rev, err := counting.HeadRevision(ctx)
	require.NoError(t, err)

	require.NoError(t, counting.CheckRevision(ctx, rev))

	optRev, err := counting.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	roundTripped, err := counting.RevisionFromString(rev.String())
	require.NoError(t, err)
	require.True(t, roundTripped.Equal(rev))

	watchCh, errCh := counting.Watch(ctx, rev, datastore.WatchOptions{Content: datastore.WatchRelationships})
	require.NotNil(t, watchCh)
	require.NotNil(t, errCh)

	// UnwrapDatastore transitively returns the underlying datastore.
	require.NotNil(t, UnwrapDatastore(counting))

	reader := counting.SnapshotReader(rev)
	_, err = reader.ReadSchema(ctx)
	require.NoError(t, err)

	_, _ = reader.CountRelationships(ctx, "nonexistent")
	_, err = reader.LookupCounters(ctx)
	require.NoError(t, err)

	// Exercise DeleteRelationships on countingReadWriteTransaction.
	_, err = counting.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		require.NoError(t, rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
		}))
		_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{ResourceType: "resource"})
		return err
	})
	require.NoError(t, err)

	require.NoError(t, counting.Close())
}

// TestUnaryCountingInterceptor verifies the interceptor swaps in a counting
// datalayer, invokes the handler, and calls the exporter exactly once.
func TestUnaryCountingInterceptor(t *testing.T) {
	base, _ := newTestDataLayer(t)
	parentCtx := ContextWithDataLayer(t.Context(), base)

	var exportedCounts *MethodCounts
	interceptor := UnaryCountingInterceptor(func(counts *MethodCounts) {
		exportedCounts = counts
	})

	resp, err := interceptor(parentCtx, "req", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"},
		func(ctx context.Context, _ any) (any, error) {
			dl := MustFromContext(ctx)
			_, err := dl.HeadRevision(ctx)
			require.NoError(t, err)
			return "ok", nil
		})
	require.NoError(t, err)
	require.Equal(t, "ok", resp)

	require.NotNil(t, exportedCounts, "exporter should have been called")
	require.Equal(t, uint64(0), exportedCounts.QueryRelationships())
}

func TestUnaryCountingInterceptor_NilExporter(t *testing.T) {
	base, _ := newTestDataLayer(t)
	parentCtx := ContextWithDataLayer(t.Context(), base)

	interceptor := UnaryCountingInterceptor(nil)

	_, err := interceptor(parentCtx, "req", &grpc.UnaryServerInfo{FullMethod: "/x/y"},
		func(ctx context.Context, _ any) (any, error) {
			return "ok", nil
		})
	require.NoError(t, err)
}

// streamStub implements the minimum of grpc.ServerStream needed by the
// interceptor — just Context().
type streamStub struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *streamStub) Context() context.Context { return s.ctx }

func TestStreamCountingInterceptor(t *testing.T) {
	base, _ := newTestDataLayer(t)
	streamCtx := ContextWithDataLayer(t.Context(), base)

	var exportedCounts *MethodCounts
	interceptor := StreamCountingInterceptor(func(counts *MethodCounts) {
		exportedCounts = counts
	})

	stub := &streamStub{ctx: streamCtx}
	err := interceptor(nil, stub, &grpc.StreamServerInfo{FullMethod: "/test/Stream"},
		func(_ any, ss grpc.ServerStream) error {
			dl := MustFromContext(ss.Context())
			_, err := dl.HeadRevision(ss.Context())
			return err
		})
	require.NoError(t, err)
	require.NotNil(t, exportedCounts)
}

func TestStreamCountingInterceptor_NilExporter(t *testing.T) {
	base, _ := newTestDataLayer(t)
	streamCtx := ContextWithDataLayer(t.Context(), base)

	interceptor := StreamCountingInterceptor(nil)
	err := interceptor(nil, &streamStub{ctx: streamCtx}, &grpc.StreamServerInfo{FullMethod: "/x"},
		func(_ any, _ grpc.ServerStream) error { return nil })
	require.NoError(t, err)
}

// TestNewReadOnlyDataLayer_RejectsWrite ensures the read-only adapter returns a
// readonly error from ReadWriteTx and pass-throughs everything else.
func TestNewReadOnlyDataLayer_RejectsWrite(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ro := NewReadOnlyDataLayer(ds)
	ctx := t.Context()

	_, err = ro.ReadWriteTx(ctx, func(context.Context, ReadWriteTransaction) error {
		return nil
	})
	require.Error(t, err)

	rev, err := ro.HeadRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, ro.CheckRevision(ctx, rev))

	readyState, err := ro.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	_, err = ro.Features(ctx)
	require.NoError(t, err)

	_, err = ro.OfflineFeatures()
	require.NoError(t, err)

	_, err = ro.Statistics(ctx)
	require.NoError(t, err)

	_, err = ro.UniqueID(ctx)
	require.NoError(t, err)

	_, err = ro.MetricsID()
	require.NoError(t, err)

	optRev, err := ro.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	rtr, err := ro.RevisionFromString(rev.String())
	require.NoError(t, err)
	require.True(t, rtr.Equal(rev))

	watchCh, errCh := ro.Watch(ctx, rev, datastore.WatchOptions{Content: datastore.WatchRelationships})
	require.NotNil(t, watchCh)
	require.NotNil(t, errCh)

	// UnwrapDatastore returns nil for the readonly adapter.
	require.Nil(t, UnwrapDatastore(ro))

	require.NoError(t, ro.Close())
}

// TestReadOnlyDataLayer_ReaderPassThroughs exercises the snapshot reader on the
// readonly adapter.
func TestReadOnlyDataLayer_ReaderPassThroughs(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ro := NewReadOnlyDataLayer(ds)
	ctx := t.Context()

	rev, err := ro.HeadRevision(ctx)
	require.NoError(t, err)

	reader := ro.SnapshotReader(rev)

	// ReadSchema on revisionedReader (shared between impl.go and readonly adapter).
	sr, err := reader.ReadSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, sr)

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "resource"})
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Empty(t, rels)

	rit, err := reader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{SubjectType: "user"})
	require.NoError(t, err)
	rrels, err := datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Empty(t, rrels)

	_, err = reader.CountRelationships(ctx, "missing")
	require.Error(t, err)

	_, err = reader.LookupCounters(ctx)
	require.NoError(t, err)
}

// TestReadWriteTransaction_AllMethods exercises the RW transaction wrappers in
// impl.go: WriteRelationships, DeleteRelationships, ReadSchema,
// CountRelationships, LookupCounters, BulkLoad, counter registration, and the
// legacy schema writer.
func TestReadWriteTransaction_AllMethods(t *testing.T) {
	dl, _ := newTestDataLayer(t)
	ctx := t.Context()

	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		require.NoError(t, rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:bar#viewer@user:fred")),
		}))

		_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType:       "resource",
			OptionalResourceId: "bar",
		})
		require.NoError(t, err)

		sr, err := rwt.ReadSchema(ctx)
		require.NoError(t, err)
		require.NotNil(t, sr)

		_, err = rwt.CountRelationships(ctx, "missing")
		require.Error(t, err)

		_, err = rwt.LookupCounters(ctx)
		require.NoError(t, err)

		// BulkLoad via the passthrough iterator.
		src := &stubBulkSourceDL{rels: []tuple.Relationship{
			tuple.MustParse("resource:bulk1#viewer@user:alice"),
			tuple.MustParse("resource:bulk2#viewer@user:bob"),
		}}
		loaded, err := rwt.BulkLoad(ctx, src)
		require.NoError(t, err)
		require.Equal(t, uint64(2), loaded)

		// Counter registration + store + unregister.
		counterName := "my_counter"
		require.NoError(t, rwt.RegisterCounter(ctx, counterName, &core.RelationshipFilter{
			ResourceType: "resource",
		}))

		// StoreCounterValue requires a revision; use a zero/no revision.
		require.NoError(t, rwt.StoreCounterValue(ctx, counterName, 42, datastore.NoRevision))

		require.NoError(t, rwt.UnregisterCounter(ctx, counterName))

		// LegacySchemaWriter surface: exercise each method. memdb's legacy writer
		// accepts empty inputs.
		lw := rwt.LegacySchemaWriter()
		require.NotNil(t, lw)
		require.NoError(t, lw.LegacyWriteCaveats(ctx, nil))
		require.NoError(t, lw.LegacyWriteNamespaces(ctx))
		require.NoError(t, lw.LegacyDeleteCaveats(ctx, nil))
		require.NoError(t, lw.LegacyDeleteNamespaces(ctx, nil, datastore.DeleteNamespacesOnly))

		// WriteSchema with no definitions should succeed via the legacy path.
		require.NoError(t, rwt.WriteSchema(ctx, nil, "", nil))

		return nil
	})
	require.NoError(t, err)
}

// stubBulkSourceDL is a minimal BulkWriteRelationshipSource for tests.
type stubBulkSourceDL struct {
	rels []tuple.Relationship
	idx  int
}

func (s *stubBulkSourceDL) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.idx >= len(s.rels) {
		return nil, nil
	}
	rel := s.rels[s.idx]
	s.idx++
	return &rel, nil
}

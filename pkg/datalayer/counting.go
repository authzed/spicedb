package datalayer

import (
	"context"
	"sync/atomic"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// MethodCounts holds per-request counters for tracked methods.
type MethodCounts struct {
	queryRelationships        atomic.Uint64
	reverseQueryRelationships atomic.Uint64
}

// QueryRelationships returns the count of QueryRelationships calls.
func (m *MethodCounts) QueryRelationships() uint64 {
	return m.queryRelationships.Load()
}

// ReverseQueryRelationships returns the count of ReverseQueryRelationships calls.
func (m *MethodCounts) ReverseQueryRelationships() uint64 {
	return m.reverseQueryRelationships.Load()
}

// MethodCountsExporter is a function that exports method counts (e.g. to Prometheus).
// This is provided by callers to avoid duplicate metric registration.
type MethodCountsExporter func(counts *MethodCounts)

// NewCountingDataLayer wraps a DataLayer with per-request counting.
func NewCountingDataLayer(dl DataLayer) (DataLayer, *MethodCounts) {
	counts := &MethodCounts{}
	return &countingDataLayer{
		delegate: dl,
		counts:   counts,
	}, counts
}

type countingDataLayer struct {
	delegate DataLayer
	counts   *MethodCounts
}

func (c *countingDataLayer) SnapshotReader(rev datastore.Revision) RevisionedReader {
	return &countingRevisionedReader{
		delegate: c.delegate.SnapshotReader(rev),
		counts:   c.counts,
	}
}

func (c *countingDataLayer) ReadWriteTx(ctx context.Context, fn TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	return c.delegate.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return fn(ctx, &countingReadWriteTransaction{
			ReadWriteTransaction: rwt,
			counts:               c.counts,
		})
	}, opts...)
}

func (c *countingDataLayer) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return c.delegate.OptimizedRevision(ctx)
}

func (c *countingDataLayer) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return c.delegate.HeadRevision(ctx)
}

func (c *countingDataLayer) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return c.delegate.CheckRevision(ctx, revision)
}

func (c *countingDataLayer) RevisionFromString(serialized string) (datastore.Revision, error) {
	return c.delegate.RevisionFromString(serialized)
}

func (c *countingDataLayer) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return c.delegate.Watch(ctx, afterRevision, opts)
}

func (c *countingDataLayer) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return c.delegate.ReadyState(ctx)
}

func (c *countingDataLayer) Features(ctx context.Context) (*datastore.Features, error) {
	return c.delegate.Features(ctx)
}

func (c *countingDataLayer) OfflineFeatures() (*datastore.Features, error) {
	return c.delegate.OfflineFeatures()
}

func (c *countingDataLayer) Statistics(ctx context.Context) (datastore.Stats, error) {
	return c.delegate.Statistics(ctx)
}

func (c *countingDataLayer) UniqueID(ctx context.Context) (string, error) {
	return c.delegate.UniqueID(ctx)
}

func (c *countingDataLayer) MetricsID() (string, error) {
	return c.delegate.MetricsID()
}

func (c *countingDataLayer) Close() error {
	return c.delegate.Close()
}

func (c *countingDataLayer) unwrapDatastore() datastore.Datastore {
	return UnwrapDatastore(c.delegate)
}

type countingRevisionedReader struct {
	delegate RevisionedReader
	counts   *MethodCounts
}

func (r *countingRevisionedReader) ReadSchema() (SchemaReader, error) {
	return r.delegate.ReadSchema()
}

func (r *countingRevisionedReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counts.queryRelationships.Add(1)
	return r.delegate.QueryRelationships(ctx, filter, opts...)
}

func (r *countingRevisionedReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counts.reverseQueryRelationships.Add(1)
	return r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (r *countingRevisionedReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.delegate.CountRelationships(ctx, name)
}

func (r *countingRevisionedReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.delegate.LookupCounters(ctx)
}

type countingReadWriteTransaction struct {
	ReadWriteTransaction
	counts *MethodCounts
}

func (t *countingReadWriteTransaction) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	t.counts.queryRelationships.Add(1)
	return t.ReadWriteTransaction.QueryRelationships(ctx, filter, opts...)
}

func (t *countingReadWriteTransaction) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	t.counts.reverseQueryRelationships.Add(1)
	return t.ReadWriteTransaction.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (t *countingReadWriteTransaction) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	return t.ReadWriteTransaction.WriteRelationships(ctx, mutations)
}

func (t *countingReadWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	return t.ReadWriteTransaction.DeleteRelationships(ctx, filter, opts...)
}

// UnaryCountingInterceptor wraps the datalayer with counting for each unary request.
// The exporter function is called after each request to export counts (e.g. to Prometheus).
func UnaryCountingInterceptor(exporter MethodCountsExporter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		dl := MustFromContext(ctx)
		countingDL, counts := NewCountingDataLayer(dl)
		if err := SetInContext(ctx, countingDL); err != nil {
			return nil, err
		}
		resp, err := handler(ctx, req)
		if exporter != nil {
			exporter(counts)
		}
		return resp, err
	}
}

// StreamCountingInterceptor wraps the datalayer with counting for each stream request.
// The exporter function is called after each stream to export counts (e.g. to Prometheus).
func StreamCountingInterceptor(exporter MethodCountsExporter) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		dl := MustFromContext(ctx)
		countingDL, counts := NewCountingDataLayer(dl)
		if err := SetInContext(ctx, countingDL); err != nil {
			return err
		}
		wrapped := middleware.WrapServerStream(ss)
		wrapped.WrappedContext = ctx
		err := handler(srv, wrapped)
		if exporter != nil {
			exporter(counts)
		}
		return err
	}
}

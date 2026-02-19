package proxy

import (
	"context"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	countingQueryRelationshipsTotal        prometheus.Counter
	countingReverseQueryRelationshipsTotal prometheus.Counter
	countingReadNamespaceByNameTotal       prometheus.Counter
	countingListAllNamespacesTotal         prometheus.Counter
	countingLookupNamespacesWithNamesTotal prometheus.Counter
)

func init() {
	countingQueryRelationshipsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "query_relationships_total",
		Help:      "total number of QueryRelationships calls",
	})

	countingReverseQueryRelationshipsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "reverse_query_relationships_total",
		Help:      "total number of ReverseQueryRelationships calls",
	})

	countingReadNamespaceByNameTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "read_namespace_by_name_total",
		Help:      "total number of ReadNamespaceByName calls",
	})

	countingListAllNamespacesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "list_all_namespaces_total",
		Help:      "total number of ListAllNamespaces calls",
	})

	countingLookupNamespacesWithNamesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "lookup_namespaces_with_names_total",
		Help:      "total number of LookupNamespacesWithNames calls",
	})
}

// DatastoreReaderMethodCounts holds thread-safe counters for tracked Reader methods.
// This struct is typically ephemeral (per-request), and represents the delta of
// calls made during its lifetime. Use WriteMethodCounts() to add these counts to
// global Prometheus counters.
type DatastoreReaderMethodCounts struct {
	queryRelationships        atomic.Uint64
	reverseQueryRelationships atomic.Uint64
	readNamespaceByName       atomic.Uint64
	listAllNamespaces         atomic.Uint64
	lookupNamespacesWithNames atomic.Uint64
}

// QueryRelationships returns the count of QueryRelationships calls.
func (m *DatastoreReaderMethodCounts) QueryRelationships() uint64 {
	return m.queryRelationships.Load()
}

// ReverseQueryRelationships returns the count of ReverseQueryRelationships calls.
func (m *DatastoreReaderMethodCounts) ReverseQueryRelationships() uint64 {
	return m.reverseQueryRelationships.Load()
}

// ReadNamespaceByName returns the count of ReadNamespaceByName calls.
func (m *DatastoreReaderMethodCounts) LegacyReadNamespaceByName() uint64 {
	return m.readNamespaceByName.Load()
}

// ListAllNamespaces returns the count of ListAllNamespaces calls.
func (m *DatastoreReaderMethodCounts) LegacyListAllNamespaces() uint64 {
	return m.listAllNamespaces.Load()
}

// LookupNamespacesWithNames returns the count of LookupNamespacesWithNames calls.
func (m *DatastoreReaderMethodCounts) LegacyLookupNamespacesWithNames() uint64 {
	return m.lookupNamespacesWithNames.Load()
}

// WriteMethodCounts writes the counts to Prometheus metrics as additive counters.
// The DatastoreReaderMethodCounts struct is ephemeral (typically per-request),
// and this function adds its counts to the global Prometheus counters.
// Multiple counts can be combined by calling this function on each.
// Tests can skip calling this and just access the DatastoreReaderMethodCounts directly.
func WriteMethodCounts(counts *DatastoreReaderMethodCounts) {
	countingQueryRelationshipsTotal.Add(float64(counts.QueryRelationships()))
	countingReverseQueryRelationshipsTotal.Add(float64(counts.ReverseQueryRelationships()))
	countingReadNamespaceByNameTotal.Add(float64(counts.LegacyReadNamespaceByName()))
	countingListAllNamespacesTotal.Add(float64(counts.LegacyListAllNamespaces()))
	countingLookupNamespacesWithNamesTotal.Add(float64(counts.LegacyLookupNamespacesWithNames()))
}

// NewCountingDatastoreProxy creates a new datastore proxy that counts Reader method calls.
// Returns both the wrapped datastore and a reference to the counts object.
// The counts object can be used in tests to verify datastore usage patterns,
// or passed to WriteMethodCounts() to export counts to Prometheus.
func NewCountingDatastoreProxy(d datastore.ReadOnlyDatastore) (datastore.ReadOnlyDatastore, *DatastoreReaderMethodCounts) {
	counts := &DatastoreReaderMethodCounts{}
	return &countingProxy{
		delegate: d,
		counts:   counts,
	}, counts
}

// NewCountingDatastoreProxyForDatastore wraps a full Datastore with counting proxy.
// Like NewCountingDatastoreProxy, but returns a full Datastore interface for use with middleware.
// The ReadWriteTx method is passed through without counting (only Reader methods are counted).
func NewCountingDatastoreProxyForDatastore(d datastore.Datastore) (datastore.Datastore, *DatastoreReaderMethodCounts) {
	counts := &DatastoreReaderMethodCounts{}
	return &countingDatastoreProxy{
		countingProxy: countingProxy{
			delegate: d,
			counts:   counts,
		},
		fullDatastore: d,
	}, counts
}

type countingDatastoreProxy struct {
	countingProxy
	fullDatastore datastore.Datastore
}

// ReadWriteTx delegates to the underlying full datastore
func (p *countingDatastoreProxy) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	return p.fullDatastore.ReadWriteTx(ctx, fn, opts...)
}

type countingProxy struct {
	delegate datastore.ReadOnlyDatastore
	counts   *DatastoreReaderMethodCounts
}

func (p *countingProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *countingProxy) UniqueID(ctx context.Context) (string, error) {
	return p.delegate.UniqueID(ctx)
}

func (p *countingProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &countingReader{
		delegate: delegateReader,
		counts:   p.counts,
	}
}

func (p *countingProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return p.delegate.OptimizedRevision(ctx)
}

func (p *countingProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return p.delegate.HeadRevision(ctx)
}

func (p *countingProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return p.delegate.CheckRevision(ctx, revision)
}

func (p *countingProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *countingProxy) Watch(ctx context.Context, afterRevision datastore.Revision, watchOptions datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, watchOptions)
}

func (p *countingProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return p.delegate.ReadyState(ctx)
}

func (p *countingProxy) Features(ctx context.Context) (*datastore.Features, error) {
	return p.delegate.Features(ctx)
}

func (p *countingProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *countingProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	return p.delegate.Statistics(ctx)
}

func (p *countingProxy) Close() error {
	return p.delegate.Close()
}

func (p *countingProxy) Unwrap() datastore.ReadOnlyDatastore {
	return p.delegate
}

type countingReader struct {
	delegate datastore.Reader
	counts   *DatastoreReaderMethodCounts
}

// Counted methods - increment counter then delegate

func (r *countingReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counts.queryRelationships.Add(1)
	return r.delegate.QueryRelationships(ctx, filter, opts...)
}

func (r *countingReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	r.counts.reverseQueryRelationships.Add(1)
	return r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
}

func (r *countingReader) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	r.counts.readNamespaceByName.Add(1)
	return r.delegate.LegacyReadNamespaceByName(ctx, nsName)
}

func (r *countingReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	r.counts.listAllNamespaces.Add(1)
	return r.delegate.LegacyListAllNamespaces(ctx)
}

func (r *countingReader) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	r.counts.lookupNamespacesWithNames.Add(1)
	return r.delegate.LegacyLookupNamespacesWithNames(ctx, nsNames)
}

// Passthrough methods - not counted

func (r *countingReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return r.delegate.LegacyReadCaveatByName(ctx, name)
}

func (r *countingReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.LegacyListAllCaveats(ctx)
}

func (r *countingReader) LegacyLookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.LegacyLookupCaveatsWithNames(ctx, caveatNames)
}

func (r *countingReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.delegate.CountRelationships(ctx, name)
}

func (r *countingReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.delegate.LookupCounters(ctx)
}

// Type assertions
var (
	_ datastore.ReadOnlyDatastore = (*countingProxy)(nil)
	_ datastore.Datastore         = (*countingDatastoreProxy)(nil)
	_ datastore.Reader            = (*countingReader)(nil)
)

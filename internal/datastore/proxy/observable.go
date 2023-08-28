package proxy

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	tracer = otel.Tracer("spicedb/datastore/proxy/observable")

	loadedRelationshipCount = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "loaded_relationships_count",
		Buckets:   []float64{0, 1, 3, 10, 32, 100, 316, 1000, 3162, 10000},
		Help:      "total number of relationships loaded for a query",
	})

	queryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "query_latency",
		Buckets:   []float64{.0005, .001, .002, .005, .01, .02, .05, .1, .2, .5},
		Help:      "response latency for a database query",
	}, []string{
		"operation",
	})
)

func filterToAttributes(filter *v1.RelationshipFilter) []attribute.KeyValue {
	attrs := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
	if filter.OptionalResourceId != "" {
		attrs = append(attrs, common.ObjIDKey.String(filter.OptionalResourceId))
	}
	if filter.OptionalRelation != "" {
		attrs = append(attrs, common.ObjRelationNameKey.String(filter.OptionalRelation))
	}

	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		attrs = append(attrs, common.SubNamespaceNameKey.String(subjectFilter.SubjectType))
		if subjectFilter.OptionalSubjectId != "" {
			attrs = append(attrs, common.SubObjectIDKey.String(subjectFilter.OptionalSubjectId))
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			attrs = append(attrs, common.SubRelationNameKey.String(relationFilter.Relation))
		}
	}
	return attrs
}

// NewObservableDatastoreProxy creates a new datastore proxy which adds tracing
// and metrics to the datastore.
func NewObservableDatastoreProxy(d datastore.Datastore) datastore.Datastore {
	return &observableProxy{delegate: d}
}

type observableProxy struct{ delegate datastore.Datastore }

func (p *observableProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &observableReader{delegateReader}
}

func (p *observableProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, func(delegateRWT datastore.ReadWriteTransaction) error {
		return f(&observableRWT{&observableReader{delegateRWT}, delegateRWT})
	}, opts...)
}

func (p *observableProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, closer := observe(ctx, "OptimizedRevision")
	defer closer()

	return p.delegate.OptimizedRevision(ctx)
}

func (p *observableProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, closer := observe(ctx, "CheckRevision", trace.WithAttributes(
		attribute.String("revision", revision.String()),
	))
	defer closer()

	return p.delegate.CheckRevision(ctx, revision)
}

func (p *observableProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, closer := observe(ctx, "HeadRevision")
	defer closer()

	return p.delegate.HeadRevision(ctx)
}

func (p *observableProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *observableProxy) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision)
}

func (p *observableProxy) Features(ctx context.Context) (*datastore.Features, error) {
	ctx, closer := observe(ctx, "Features")
	defer closer()

	return p.delegate.Features(ctx)
}

func (p *observableProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	ctx, closer := observe(ctx, "Statistics")
	defer closer()

	return p.delegate.Statistics(ctx)
}

func (p *observableProxy) Unwrap() datastore.Datastore {
	return p.delegate
}

func (p *observableProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	ctx, closer := observe(ctx, "ReadyState")
	defer closer()

	return p.delegate.ReadyState(ctx)
}

func (p *observableProxy) Close() error { return p.delegate.Close() }

type observableReader struct{ delegate datastore.Reader }

func (r *observableReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	ctx, closer := observe(ctx, "ReadCaveatByName", trace.WithAttributes(
		attribute.String("name", name),
	))
	defer closer()

	return r.delegate.ReadCaveatByName(ctx, name)
}

func (r *observableReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	ctx, closer := observe(ctx, "LookupCaveatsWithNames", trace.WithAttributes(
		attribute.StringSlice("names", caveatNames),
	))
	defer closer()

	return r.delegate.LookupCaveatsWithNames(ctx, caveatNames)
}

func (r *observableReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	ctx, closer := observe(ctx, "ListAllCaveats")
	defer closer()

	return r.delegate.ListAllCaveats(ctx)
}

func (r *observableReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	ctx, closer := observe(ctx, "ListAllNamespaces")
	defer closer()

	return r.delegate.ListAllNamespaces(ctx)
}

func (r *observableReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	ctx, closer := observe(ctx, "LookupNamespacesWithNames", trace.WithAttributes(
		attribute.StringSlice("names", nsNames),
	))
	defer closer()

	return r.delegate.LookupNamespacesWithNames(ctx, nsNames)
}

func (r *observableReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, closer := observe(ctx, "ReadNamespaceByName", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer closer()

	return r.delegate.ReadNamespaceByName(ctx, nsName)
}

func (r *observableReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	ctx, closer := observe(ctx, "QueryRelationships")

	iterator, err := r.delegate.QueryRelationships(ctx, filter, options...)
	if err != nil {
		return iterator, err
	}
	return &observableRelationshipIterator{closer, iterator, 0}, nil
}

type observableRelationshipIterator struct {
	closer   func()
	delegate datastore.RelationshipIterator
	count    uint32
}

func (i *observableRelationshipIterator) Next() *core.RelationTuple {
	if next := i.delegate.Next(); next != nil {
		i.count++
		return next
	}
	return nil
}

func (i *observableRelationshipIterator) Err() error { return i.delegate.Err() }

func (i *observableRelationshipIterator) Cursor() (options.Cursor, error) { return i.delegate.Cursor() }

func (i *observableRelationshipIterator) Close() {
	loadedRelationshipCount.Observe(float64(i.count))
	i.closer()
	i.delegate.Close()
}

func (r *observableReader) ReverseQueryRelationships(ctx context.Context, subjectFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	ctx, closer := observe(ctx, "ReverseQueryRelationships")
	iterator, err := r.delegate.ReverseQueryRelationships(ctx, subjectFilter, options...)
	if err != nil {
		return iterator, err
	}
	return &observableRelationshipIterator{closer, iterator, 0}, nil
}

type observableRWT struct {
	*observableReader
	delegate datastore.ReadWriteTransaction
}

func (rwt *observableRWT) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	caveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		caveatNames = append(caveatNames, caveat.Name)
	}

	ctx, closer := observe(ctx, "WriteCaveats", trace.WithAttributes(
		attribute.StringSlice("names", caveatNames),
	))
	defer closer()

	return rwt.delegate.WriteCaveats(ctx, caveats)
}

func (rwt *observableRWT) DeleteCaveats(ctx context.Context, names []string) error {
	ctx, closer := observe(ctx, "DeleteCaveats", trace.WithAttributes(
		attribute.StringSlice("names", names),
	))
	defer closer()

	return rwt.delegate.DeleteCaveats(ctx, names)
}

func (rwt *observableRWT) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	ctx, closer := observe(ctx, "WriteRelationships", trace.WithAttributes(
		attribute.Int("mutations", len(mutations)),
	))
	defer closer()

	return rwt.delegate.WriteRelationships(ctx, mutations)
}

func (rwt *observableRWT) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	nsNames := make([]string, 0, len(newConfigs))
	for _, ns := range newConfigs {
		nsNames = append(nsNames, ns.Name)
	}

	ctx, closer := observe(ctx, "WriteNamespaces", trace.WithAttributes(
		attribute.StringSlice("names", nsNames),
	))
	defer closer()

	return rwt.delegate.WriteNamespaces(ctx, newConfigs...)
}

func (rwt *observableRWT) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	ctx, closer := observe(ctx, "DeleteNamespaces", trace.WithAttributes(
		attribute.StringSlice("names", nsNames),
	))
	defer closer()

	return rwt.delegate.DeleteNamespaces(ctx, nsNames...)
}

func (rwt *observableRWT) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	ctx, closer := observe(ctx, "DeleteRelationships", trace.WithAttributes(
		filterToAttributes(filter)...,
	))
	defer closer()

	return rwt.delegate.DeleteRelationships(ctx, filter)
}

func (rwt *observableRWT) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	ctx, closer := observe(ctx, "BulkLoad")
	defer closer()

	return rwt.delegate.BulkLoad(ctx, iter)
}

func observe(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, func()) {
	ctx, span := tracer.Start(ctx, name, opts...)
	timer := prometheus.NewTimer(queryLatency.WithLabelValues(name))

	return ctx, func() {
		timer.ObserveDuration()
		span.End()
	}
}

var (
	_ datastore.Datastore            = (*observableProxy)(nil)
	_ datastore.Reader               = (*observableReader)(nil)
	_ datastore.ReadWriteTransaction = (*observableRWT)(nil)
	_ datastore.RelationshipIterator = (*observableRelationshipIterator)(nil)
)

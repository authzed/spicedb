package proxy

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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
		"operation", "query_shape",
	})
)

func filterToAttributes(filter *v1.RelationshipFilter) []attribute.KeyValue {
	attrs := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.GetResourceType())}
	if filter.GetOptionalResourceId() != "" {
		attrs = append(attrs, common.ObjIDKey.String(filter.GetOptionalResourceId()))
	}
	if filter.GetOptionalRelation() != "" {
		attrs = append(attrs, common.ObjRelationNameKey.String(filter.GetOptionalRelation()))
	}

	if subjectFilter := filter.GetOptionalSubjectFilter(); subjectFilter != nil {
		attrs = append(attrs, common.SubNamespaceNameKey.String(subjectFilter.GetSubjectType()))
		if subjectFilter.GetOptionalSubjectId() != "" {
			attrs = append(attrs, common.SubObjectIDKey.String(subjectFilter.GetOptionalSubjectId()))
		}
		if relationFilter := subjectFilter.GetOptionalRelation(); relationFilter != nil {
			attrs = append(attrs, common.SubRelationNameKey.String(relationFilter.GetRelation()))
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

func (p *observableProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *observableProxy) UniqueID(ctx context.Context) (string, error) {
	return p.delegate.UniqueID(ctx)
}

func (p *observableProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &observableReader{delegateReader}
}

func (p *observableProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, func(ctx context.Context, delegateRWT datastore.ReadWriteTransaction) error {
		return f(ctx, &observableRWT{&observableReader{delegateRWT}, delegateRWT})
	}, opts...)
}

func (p *observableProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, closer := observe(ctx, "OptimizedRevision", "")
	defer closer()

	return p.delegate.OptimizedRevision(ctx)
}

func (p *observableProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, closer := observe(ctx, "CheckRevision", "", trace.WithAttributes(
		attribute.String(otelconv.AttrDatastoreRevision, revision.String()),
	))
	defer closer()

	return p.delegate.CheckRevision(ctx, revision)
}

func (p *observableProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, closer := observe(ctx, "HeadRevision", "")
	defer closer()

	return p.delegate.HeadRevision(ctx)
}

func (p *observableProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *observableProxy) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *observableProxy) Features(ctx context.Context) (*datastore.Features, error) {
	ctx, closer := observe(ctx, "Features", "")
	defer closer()

	return p.delegate.Features(ctx)
}

func (p *observableProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *observableProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	ctx, closer := observe(ctx, "Statistics", "")
	defer closer()

	return p.delegate.Statistics(ctx)
}

func (p *observableProxy) Unwrap() datastore.Datastore {
	return p.delegate
}

func (p *observableProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	ctx, closer := observe(ctx, "ReadyState", "")
	defer closer()

	return p.delegate.ReadyState(ctx)
}

func (p *observableProxy) Close() error { return p.delegate.Close() }

type observableReader struct{ delegate datastore.Reader }

func (r *observableReader) CountRelationships(ctx context.Context, name string) (int, error) {
	ctx, closer := observe(ctx, "CountRelationships", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{name}),
	))
	defer closer()

	return r.delegate.CountRelationships(ctx, name)
}

func (r *observableReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	ctx, closer := observe(ctx, "LookupCounters", "")
	defer closer()

	return r.delegate.LookupCounters(ctx)
}

func (r *observableReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	ctx, closer := observe(ctx, "ReadCaveatByName", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{name}),
	))
	defer closer()

	return r.delegate.ReadCaveatByName(ctx, name)
}

func (r *observableReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	ctx, closer := observe(ctx, "LookupCaveatsWithNames", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, caveatNames),
	))
	defer closer()

	return r.delegate.LookupCaveatsWithNames(ctx, caveatNames)
}

func (r *observableReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	ctx, closer := observe(ctx, "ListAllCaveats", "")
	defer closer()

	return r.delegate.ListAllCaveats(ctx)
}

func (r *observableReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	ctx, closer := observe(ctx, "ListAllNamespaces", "")
	defer closer()

	return r.delegate.ListAllNamespaces(ctx)
}

func (r *observableReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	ctx, closer := observe(ctx, "LookupNamespacesWithNames", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, nsNames),
	))
	defer closer()

	return r.delegate.LookupNamespacesWithNames(ctx, nsNames)
}

func (r *observableReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, closer := observe(ctx, "ReadNamespaceByName", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{nsName}),
	))
	defer closer()

	return r.delegate.ReadNamespaceByName(ctx, nsName)
}

func (r *observableReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewQueryOptionsWithOptions(opts...)
	ctx, closer := observe(ctx, "QueryRelationships", string(queryOpts.QueryShape), trace.WithAttributes(
		attribute.String(otelconv.AttrDatastoreResourceType, filter.OptionalResourceType),
		attribute.String(otelconv.AttrDatastoreResourceRelation, filter.OptionalResourceRelation),
		attribute.String(otelconv.AttrDatastoreQueryShape, string(queryOpts.QueryShape)),
	))

	iterator, err := r.delegate.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		closer()
		return iterator, err
	}

	return func(yield func(tuple.Relationship, error) bool) {
		defer closer()

		var count uint64
		for rel, err := range iterator {
			count++
			if !yield(rel, err) {
				break
			}
		}
		loadedRelationshipCount.Observe(float64(count))
	}, nil
}

func (r *observableReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)
	ctx, closer := observe(ctx, "ReverseQueryRelationships", string(queryOpts.QueryShapeForReverse), trace.WithAttributes(
		attribute.String(otelconv.AttrDatastoreSubjectType, subjectsFilter.SubjectType),
		attribute.String(otelconv.AttrDatastoreQueryShape, string(queryOpts.QueryShapeForReverse))))

	iterator, err := r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
	if err != nil {
		closer()
		return iterator, err
	}

	return func(yield func(tuple.Relationship, error) bool) {
		defer closer()

		var count uint64
		for rel, err := range iterator {
			count++
			if !yield(rel, err) {
				break
			}
		}
		loadedRelationshipCount.Observe(float64(count))
	}, nil
}

type observableRWT struct {
	*observableReader
	delegate datastore.ReadWriteTransaction
}

func (rwt *observableRWT) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	ctx, closer := observe(ctx, "RegisterCounter", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{name}),
	))
	defer closer()

	return rwt.delegate.RegisterCounter(ctx, name, filter)
}

func (rwt *observableRWT) UnregisterCounter(ctx context.Context, name string) error {
	ctx, closer := observe(ctx, "UnregisterCounter", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{name}),
	))
	defer closer()

	return rwt.delegate.UnregisterCounter(ctx, name)
}

func (rwt *observableRWT) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	ctx, closer := observe(ctx, "StoreCounterValue", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, []string{name}),
		attribute.Int(otelconv.AttrDatastoreValue, value),
		attribute.String(otelconv.AttrDatastoreRevision, computedAtRevision.String()),
	))
	defer closer()

	return rwt.delegate.StoreCounterValue(ctx, name, value, computedAtRevision)
}

func (rwt *observableRWT) WriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	caveatNames := make([]string, 0, len(caveats))
	for _, caveat := range caveats {
		caveatNames = append(caveatNames, caveat.GetName())
	}

	ctx, closer := observe(ctx, "WriteCaveats", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, caveatNames),
	))
	defer closer()

	return rwt.delegate.WriteCaveats(ctx, caveats)
}

func (rwt *observableRWT) DeleteCaveats(ctx context.Context, names []string) error {
	ctx, closer := observe(ctx, "DeleteCaveats", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, names),
	))
	defer closer()

	return rwt.delegate.DeleteCaveats(ctx, names)
}

func (rwt *observableRWT) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	ctx, closer := observe(ctx, "WriteRelationships", "", trace.WithAttributes(
		attribute.Int(otelconv.AttrDatastoreMutations, len(mutations)),
	))
	defer closer()

	return rwt.delegate.WriteRelationships(ctx, mutations)
}

func (rwt *observableRWT) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	nsNames := make([]string, 0, len(newConfigs))
	for _, ns := range newConfigs {
		nsNames = append(nsNames, ns.GetName())
	}

	ctx, closer := observe(ctx, "WriteNamespaces", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, nsNames),
	))
	defer closer()

	return rwt.delegate.WriteNamespaces(ctx, newConfigs...)
}

func (rwt *observableRWT) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	ctx, closer := observe(ctx, "DeleteNamespaces", "", trace.WithAttributes(
		attribute.StringSlice(otelconv.AttrDatastoreNames, nsNames),
	))
	defer closer()

	return rwt.delegate.DeleteNamespaces(ctx, nsNames...)
}

func (rwt *observableRWT) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, options ...options.DeleteOptionsOption) (uint64, bool, error) {
	ctx, closer := observe(ctx, "DeleteRelationships", "", trace.WithAttributes(
		filterToAttributes(filter)...,
	))
	defer closer()

	return rwt.delegate.DeleteRelationships(ctx, filter, options...)
}

func (rwt *observableRWT) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	ctx, closer := observe(ctx, "BulkLoad", "")
	defer closer()

	return rwt.delegate.BulkLoad(ctx, iter)
}

// nolint:spancheck
func observe(ctx context.Context, name string, queryShape string, opts ...trace.SpanStartOption) (context.Context, func()) {
	if queryShape == "" {
		queryShape = "(none)"
	}

	ctx, span := tracer.Start(ctx, name, opts...)
	timer := prometheus.NewTimer(queryLatency.WithLabelValues(name, queryShape))
	closed := false

	return ctx, func() {
		if closed {
			return
		}

		closed = true
		timer.ObserveDuration()
		span.End()
	}
}

var (
	_ datastore.Datastore            = (*observableProxy)(nil)
	_ datastore.Reader               = (*observableReader)(nil)
	_ datastore.ReadWriteTransaction = (*observableRWT)(nil)
)

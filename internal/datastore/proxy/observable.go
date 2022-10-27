package proxy

import (
	"context"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var tracer = otel.Tracer("spicedb/datastore/proxy/observable")

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

func (p *observableProxy) ReadWriteTx(ctx context.Context, f datastore.TxUserFunc) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, func(delegateRWT datastore.ReadWriteTransaction) error {
		return f(&observableRWT{&observableReader{delegateRWT}, delegateRWT})
	})
}

func (p *observableProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	return p.delegate.OptimizedRevision(ctx)
}

func (p *observableProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "CheckRevision", trace.WithAttributes(
		attribute.String("revision", revision.String()),
	))
	defer span.End()

	return p.delegate.CheckRevision(ctx, revision)
}

func (p *observableProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "HeadRevision")
	defer span.End()

	return p.delegate.HeadRevision(ctx)
}

func (p *observableProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *observableProxy) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision)
}

func (p *observableProxy) Features(ctx context.Context) (*datastore.Features, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "Features")
	defer span.End()

	return p.delegate.Features(ctx)
}

func (p *observableProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "Statistics")
	defer span.End()

	return p.delegate.Statistics(ctx)
}

func (p *observableProxy) IsReady(ctx context.Context) (bool, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "IsReady")
	defer span.End()

	return p.delegate.IsReady(ctx)
}

func (p *observableProxy) Close() error { return p.delegate.Close() }

type observableReader struct{ delegate datastore.Reader }

func (r *observableReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "ReadCaveatByName", trace.WithAttributes(
		attribute.String("name", name),
	))
	defer span.End()

	return r.delegate.ReadCaveatByName(ctx, name)
}

func (r *observableReader) ListCaveats(ctx context.Context, caveatNamesForFiltering ...string) ([]*core.CaveatDefinition, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "ListCaveats", trace.WithAttributes(
		attribute.StringSlice("names", caveatNamesForFiltering),
	))
	defer span.End()

	return r.delegate.ListCaveats(ctx, caveatNamesForFiltering...)
}

func (r *observableReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "ListNamespaces")
	defer span.End()

	return r.delegate.ListNamespaces(ctx)
}

func (r *observableReader) LookupNamespaces(ctx context.Context, nsNames []string) ([]*core.NamespaceDefinition, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "LookupNamespaces", trace.WithAttributes(
		attribute.StringSlice("names", nsNames),
	))
	defer span.End()

	return r.delegate.ListNamespaces(ctx)
}

func (r *observableReader) ReadNamespace(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	return r.delegate.ReadNamespace(ctx, nsName)
}

func (r *observableReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "QueryRelationships")

	iterator, err := r.delegate.QueryRelationships(ctx, filter, options...)
	if err != nil {
		return iterator, err
	}
	return observableRelationshipIterator{span, iterator}, nil
}

type observableRelationshipIterator struct {
	span     trace.Span
	delegate datastore.RelationshipIterator
}

func (i observableRelationshipIterator) Next() *core.RelationTuple { return i.delegate.Next() }
func (i observableRelationshipIterator) Err() error                { return i.delegate.Err() }
func (i observableRelationshipIterator) Close()                    { i.span.End(); i.delegate.Close() }

func (r *observableReader) ReverseQueryRelationships(ctx context.Context, subjectFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "ReverseQueryRelationships")

	iterator, err := r.delegate.ReverseQueryRelationships(ctx, subjectFilter, options...)
	if err != nil {
		return iterator, err
	}
	return observableRelationshipIterator{span, iterator}, nil
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

	ctx, span := tracer.Start(
		ctx,
		"WriteCaveats",
		trace.WithAttributes(attribute.StringSlice("names", caveatNames)),
	)
	defer span.End()

	return rwt.delegate.WriteCaveats(ctx, caveats)
}

func (rwt *observableRWT) DeleteCaveats(ctx context.Context, names []string) error {
	ctx, span := tracer.Start(ctx, "DeleteCaveats", trace.WithAttributes(
		attribute.StringSlice("names", names),
	))
	defer span.End()

	return rwt.delegate.DeleteCaveats(ctx, names)
}

func (rwt *observableRWT) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "WriteRelationships", trace.WithAttributes(
		attribute.Int("mutations", len(mutations)),
	))
	defer span.End()

	return rwt.delegate.WriteRelationships(ctx, mutations)
}

func (rwt *observableRWT) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	nsNames := make([]string, 0, len(newConfigs))
	for _, ns := range newConfigs {
		nsNames = append(nsNames, ns.Name)
	}

	var span trace.Span
	ctx, span = tracer.Start(ctx, "WriteNamespaces", trace.WithAttributes(
		attribute.StringSlice("name", nsNames),
	))
	defer span.End()

	return rwt.delegate.WriteNamespaces(ctx, newConfigs...)
}

func (rwt *observableRWT) DeleteNamespace(ctx context.Context, nsName string) error {
	var span trace.Span
	ctx, span = tracer.Start(ctx, "DeleteNamespace", trace.WithAttributes(
		attribute.String("name", nsName),
	))
	defer span.End()

	return rwt.delegate.DeleteNamespace(ctx, nsName)
}

func (rwt *observableRWT) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	var span trace.Span
	ctx, span = tracer.Start(
		ctx,
		"DeleteRelationships",
		trace.WithAttributes(filterToAttributes(filter)...),
	)
	defer span.End()

	return rwt.delegate.DeleteRelationships(ctx, filter)
}

var (
	_ datastore.Datastore            = (*observableProxy)(nil)
	_ datastore.Reader               = (*observableReader)(nil)
	_ datastore.ReadWriteTransaction = (*observableRWT)(nil)
	_ datastore.RelationshipIterator = (*observableRelationshipIterator)(nil)
)

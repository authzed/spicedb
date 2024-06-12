package datastore

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/test"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var defaultTimeout = 5 * time.Second

// SeparateContextWithTracing is a utility method which allows for severing the
// context between grpc and the datastore to prevent context cancellation from
// killing database connections that should otherwise go back to the connection
// pool.
//
// A safety timeout is set so that connections don't hang forever
func SeparateContextWithTracing(ctx context.Context) (context.Context, func()) {
	span := trace.SpanFromContext(ctx)
	ctxWithObservability := trace.ContextWithSpan(context.Background(), span)

	loggerFromContext := log.Ctx(ctx)
	if loggerFromContext != nil {
		ctxWithObservability = loggerFromContext.WithContext(ctxWithObservability)
	}

	return context.WithTimeout(ctxWithObservability, defaultTimeout)
}

// NewSeparatingContextDatastoreProxy severs any timeouts in the context being
// passed to the datastore and only retains tracing metadata.
//
// This is useful for datastores that do not want to close connections when a
// cancel or deadline occurs.
func NewSeparatingContextDatastoreProxy(d datastore.Datastore) datastore.Datastore {
	return &ctxProxy{d}
}

type ctxProxy struct{ delegate datastore.Datastore }

func (p *ctxProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, f, opts...)
}

func (p *ctxProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()
	return p.delegate.OptimizedRevision(ctx)
}

func (p *ctxProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()
	return p.delegate.CheckRevision(ctx, revision)
}

func (p *ctxProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return p.delegate.HeadRevision(ctx)
}

func (p *ctxProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *ctxProxy) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan *datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *ctxProxy) Features(ctx context.Context) (*datastore.Features, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return p.delegate.Features(ctx)
}

func (p *ctxProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return p.delegate.Statistics(ctx)
}

func (p *ctxProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return p.delegate.ReadyState(ctx)
}

func (p *ctxProxy) Close() error { return p.delegate.Close() }

func (p *ctxProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &ctxReader{delegateReader}
}

func (p *ctxProxy) Unwrap() datastore.Datastore {
	return p.delegate
}

// ExampleRetryableError implements the TestableDatastore interface
func (p *ctxProxy) ExampleRetryableError() error {
	return p.delegate.(test.TestableDatastore).ExampleRetryableError()
}

type ctxReader struct{ delegate datastore.Reader }

func (r *ctxReader) CountRelationships(ctx context.Context, name string) (int, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.CountRelationships(ctx, name)
}

func (r *ctxReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.LookupCounters(ctx)
}

func (r *ctxReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.ReadCaveatByName(ctx, name)
}

func (r *ctxReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.ListAllCaveats(ctx)
}

func (r *ctxReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.LookupCaveatsWithNames(ctx, caveatNames)
}

func (r *ctxReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.ListAllNamespaces(ctx)
}

func (r *ctxReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.LookupNamespacesWithNames(ctx, nsNames)
}

func (r *ctxReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.ReadNamespaceByName(ctx, nsName)
}

func (r *ctxReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.QueryRelationships(ctx, filter, options...)
}

func (r *ctxReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	ctx, cancel := SeparateContextWithTracing(ctx)
	defer cancel()

	return r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, options...)
}

var (
	_ datastore.Datastore = (*ctxProxy)(nil)
	_ datastore.Reader    = (*ctxReader)(nil)
)

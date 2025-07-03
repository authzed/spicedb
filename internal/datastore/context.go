package datastore

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/test"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewSeparatingContextDatastoreProxy severs any timeouts in the context being
// passed to the datastore and only retains tracing metadata.
//
// This is useful for datastores that do not want to close connections when a
// cancel or deadline occurs.
func NewSeparatingContextDatastoreProxy(d datastore.Datastore) datastore.StrictReadDatastore {
	return &ctxProxy{d}
}

type ctxProxy struct{ delegate datastore.Datastore }

func (p *ctxProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *ctxProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, f, opts...)
}

func (p *ctxProxy) IsStrictReadModeEnabled() bool {
	ds := p.delegate
	unwrapped, ok := p.delegate.(datastore.UnwrappableDatastore)
	if ok {
		ds = unwrapped.Unwrap()
	}

	if srm, ok := ds.(datastore.StrictReadDatastore); ok {
		return srm.IsStrictReadModeEnabled()
	}

	return false
}

func (p *ctxProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	return p.delegate.OptimizedRevision(context.WithoutCancel(ctx))
}

func (p *ctxProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return p.delegate.CheckRevision(context.WithoutCancel(ctx), revision)
}

func (p *ctxProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return p.delegate.HeadRevision(context.WithoutCancel(ctx))
}

func (p *ctxProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *ctxProxy) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *ctxProxy) Features(ctx context.Context) (*datastore.Features, error) {
	return p.delegate.Features(context.WithoutCancel(ctx))
}

func (p *ctxProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *ctxProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	return p.delegate.Statistics(context.WithoutCancel(ctx))
}

func (p *ctxProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return p.delegate.ReadyState(context.WithoutCancel(ctx))
}

func (p *ctxProxy) Close() error { return p.delegate.Close() }

func (p *ctxProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &ctxReader{delegateReader}
}

func (p *ctxProxy) Unwrap() datastore.Datastore {
	return p.delegate
}

// Implement the TestableDatastore interface
func (p *ctxProxy) ExampleRetryableError() error {
	return p.delegate.(test.TestableDatastore).ExampleRetryableError()
}

type ctxReader struct{ delegate datastore.Reader }

func (r *ctxReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.delegate.CountRelationships(context.WithoutCancel(ctx), name)
}

func (r *ctxReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return r.delegate.LookupCounters(context.WithoutCancel(ctx))
}

func (r *ctxReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return r.delegate.ReadCaveatByName(context.WithoutCancel(ctx), name)
}

func (r *ctxReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.ListAllCaveats(context.WithoutCancel(ctx))
}

func (r *ctxReader) LookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]datastore.RevisionedCaveat, error) {
	return r.delegate.LookupCaveatsWithNames(context.WithoutCancel(ctx), caveatNames)
}

func (r *ctxReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return r.delegate.ListAllNamespaces(context.WithoutCancel(ctx))
}

func (r *ctxReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	return r.delegate.LookupNamespacesWithNames(context.WithoutCancel(ctx), nsNames)
}

func (r *ctxReader) ReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, datastore.Revision, error) {
	return r.delegate.ReadNamespaceByName(context.WithoutCancel(ctx), nsName)
}

func (r *ctxReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, options ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return r.delegate.QueryRelationships(context.WithoutCancel(ctx), filter, options...)
}

func (r *ctxReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, options ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	return r.delegate.ReverseQueryRelationships(context.WithoutCancel(ctx), subjectsFilter, options...)
}

var (
	_ datastore.Datastore = (*ctxProxy)(nil)
	_ datastore.Reader    = (*ctxReader)(nil)
)

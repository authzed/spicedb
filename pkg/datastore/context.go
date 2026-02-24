package datastore

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewSeparatingContextDatastoreProxy severs any timeouts in the context being
// passed to the datastore and only retains tracing metadata.
//
// This is useful for datastores that do not want to close connections when a
// cancel or deadline occurs.
func NewSeparatingContextDatastoreProxy(d Datastore) StrictReadDatastore {
	return &ctxProxy{d}
}

type ctxProxy struct{ delegate Datastore }

func (p *ctxProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *ctxProxy) UniqueID(ctx context.Context) (string, error) {
	return p.delegate.UniqueID(ctx)
}

func (p *ctxProxy) ReadWriteTx(
	ctx context.Context,
	f TxUserFunc,
	opts ...options.RWTOptionsOption,
) (Revision, error) {
	return p.delegate.ReadWriteTx(ctx, f, opts...)
}

func (p *ctxProxy) IsStrictReadModeEnabled() bool {
	ds := p.delegate
	unwrapped, ok := p.delegate.(UnwrappableDatastore)
	if ok {
		ds = unwrapped.Unwrap()
	}

	if srm, ok := ds.(StrictReadDatastore); ok {
		return srm.IsStrictReadModeEnabled()
	}

	return false
}

func (p *ctxProxy) OptimizedRevision(ctx context.Context) (RevisionWithSchemaHash, error) {
	return p.delegate.OptimizedRevision(context.WithoutCancel(ctx))
}

func (p *ctxProxy) CheckRevision(ctx context.Context, revision Revision) error {
	return p.delegate.CheckRevision(context.WithoutCancel(ctx), revision)
}

func (p *ctxProxy) HeadRevision(ctx context.Context) (RevisionWithSchemaHash, error) {
	return p.delegate.HeadRevision(context.WithoutCancel(ctx))
}

func (p *ctxProxy) RevisionFromString(serialized string) (Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *ctxProxy) Watch(ctx context.Context, afterRevision Revision, options WatchOptions) (<-chan RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *ctxProxy) Features(ctx context.Context) (*Features, error) {
	return p.delegate.Features(context.WithoutCancel(ctx))
}

func (p *ctxProxy) OfflineFeatures() (*Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *ctxProxy) Statistics(ctx context.Context) (Stats, error) {
	return p.delegate.Statistics(context.WithoutCancel(ctx))
}

func (p *ctxProxy) ReadyState(ctx context.Context) (ReadyState, error) {
	return p.delegate.ReadyState(context.WithoutCancel(ctx))
}

func (p *ctxProxy) Close() error { return p.delegate.Close() }

func (p *ctxProxy) SnapshotReader(rev Revision) Reader {
	delegateReader := p.delegate.SnapshotReader(rev)
	return &ctxReader{delegateReader}
}

func (p *ctxProxy) Unwrap() Datastore {
	return p.delegate
}

type ctxReader struct{ delegate Reader }

func (r *ctxReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return r.delegate.CountRelationships(context.WithoutCancel(ctx), name)
}

func (r *ctxReader) LookupCounters(ctx context.Context) ([]RelationshipCounter, error) {
	return r.delegate.LookupCounters(context.WithoutCancel(ctx))
}

func (r *ctxReader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, Revision, error) {
	return r.delegate.LegacyReadCaveatByName(context.WithoutCancel(ctx), name)
}

func (r *ctxReader) LegacyListAllCaveats(ctx context.Context) ([]RevisionedCaveat, error) {
	return r.delegate.LegacyListAllCaveats(context.WithoutCancel(ctx))
}

func (r *ctxReader) LegacyLookupCaveatsWithNames(ctx context.Context, caveatNames []string) ([]RevisionedCaveat, error) {
	return r.delegate.LegacyLookupCaveatsWithNames(context.WithoutCancel(ctx), caveatNames)
}

func (r *ctxReader) LegacyListAllNamespaces(ctx context.Context) ([]RevisionedNamespace, error) {
	return r.delegate.LegacyListAllNamespaces(context.WithoutCancel(ctx))
}

func (r *ctxReader) LegacyLookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]RevisionedNamespace, error) {
	return r.delegate.LegacyLookupNamespacesWithNames(context.WithoutCancel(ctx), nsNames)
}

func (r *ctxReader) LegacyReadNamespaceByName(ctx context.Context, nsName string) (*core.NamespaceDefinition, Revision, error) {
	return r.delegate.LegacyReadNamespaceByName(context.WithoutCancel(ctx), nsName)
}

func (r *ctxReader) QueryRelationships(ctx context.Context, filter RelationshipsFilter, options ...options.QueryOptionsOption) (RelationshipIterator, error) {
	return r.delegate.QueryRelationships(context.WithoutCancel(ctx), filter, options...)
}

func (r *ctxReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter SubjectsFilter, options ...options.ReverseQueryOptionsOption) (RelationshipIterator, error) {
	return r.delegate.ReverseQueryRelationships(context.WithoutCancel(ctx), subjectsFilter, options...)
}

func (r *ctxReader) ReadStoredSchema(ctx context.Context) (*ReadOnlyStoredSchema, error) {
	return r.delegate.ReadStoredSchema(context.WithoutCancel(ctx))
}

var (
	_ Datastore = (*ctxProxy)(nil)
	_ Reader    = (*ctxReader)(nil)
)

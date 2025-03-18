package proxy

import (
	"context"

	"resenje.org/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// NewSingleflightDatastoreProxy creates a new Datastore proxy which
// deduplicates calls to Datastore methods that can share results.
func NewSingleflightDatastoreProxy(d datastore.Datastore) datastore.Datastore {
	return &singleflightProxy{delegate: d}
}

type singleflightProxy struct {
	headRevGroup  singleflight.Group[string, datastore.Revision]
	checkRevGroup singleflight.Group[string, string]
	statsGroup    singleflight.Group[string, datastore.Stats]
	delegate      datastore.Datastore
}

var _ datastore.Datastore = (*singleflightProxy)(nil)

func (p *singleflightProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *singleflightProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return p.delegate.SnapshotReader(rev)
}

func (p *singleflightProxy) ReadWriteTx(ctx context.Context, f datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, f, opts...)
}

func (p *singleflightProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	// NOTE: Optimized revisions are singleflighted by the underlying datastore via the
	// CachedOptimizedRevisions struct.
	return p.delegate.OptimizedRevision(ctx)
}

func (p *singleflightProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	_, _, err := p.checkRevGroup.Do(ctx, revision.String(), func(ctx context.Context) (string, error) {
		return "", p.delegate.CheckRevision(ctx, revision)
	})
	return err
}

func (p *singleflightProxy) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	rev, _, err := p.headRevGroup.Do(ctx, "", func(ctx context.Context) (datastore.Revision, error) {
		return p.delegate.HeadRevision(ctx)
	})
	return rev, err
}

func (p *singleflightProxy) RevisionFromString(serialized string) (datastore.Revision, error) {
	return p.delegate.RevisionFromString(serialized)
}

func (p *singleflightProxy) Watch(ctx context.Context, afterRevision datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	return p.delegate.Watch(ctx, afterRevision, options)
}

func (p *singleflightProxy) Statistics(ctx context.Context) (datastore.Stats, error) {
	stats, _, err := p.statsGroup.Do(ctx, "", func(ctx context.Context) (datastore.Stats, error) {
		return p.delegate.Statistics(ctx)
	})
	return stats, err
}

func (p *singleflightProxy) Features(ctx context.Context) (*datastore.Features, error) {
	return p.delegate.Features(ctx)
}

func (p *singleflightProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *singleflightProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return p.delegate.ReadyState(ctx)
}

func (p *singleflightProxy) Close() error                { return p.delegate.Close() }
func (p *singleflightProxy) Unwrap() datastore.Datastore { return p.delegate }

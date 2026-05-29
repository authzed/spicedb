package proxy

import (
	"context"
	"time"

	"resenje.org/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// singleflightTimeout is the maximum time that a caller of a singleflighted method
// will wait before giving up on the singleflight executor.
// This prevents a possible deadlock when all datastore connections are held by goroutines waiting on the
// singleflight while the singleflight executor is blocked waiting for a connection.
const singleFlightTimeout = 1 * time.Second

// NewSingleflightDatastoreProxy creates a new Datastore proxy which
// deduplicates calls to Datastore methods that can share results.
func NewSingleflightDatastoreProxy(d datastore.Datastore) datastore.Datastore {
	return &singleflightProxy{delegate: d}
}

type singleflightProxy struct {
	headRevGroup  singleflight.Group[string, datastore.RevisionWithSchemaHash]
	checkRevGroup singleflight.Group[string, string]
	statsGroup    singleflight.Group[string, datastore.Stats]
	featuresGroup singleflight.Group[string, *datastore.Features]
	delegate      datastore.Datastore
}

var _ datastore.Datastore = (*singleflightProxy)(nil)

func (p *singleflightProxy) MetricsID() (string, error) {
	return p.delegate.MetricsID()
}

func (p *singleflightProxy) UniqueID(ctx context.Context) (string, error) {
	return p.delegate.UniqueID(ctx)
}

func (p *singleflightProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return p.delegate.SnapshotReader(rev)
}

func (p *singleflightProxy) ReadWriteTx(ctx context.Context, f datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	return p.delegate.ReadWriteTx(ctx, f, opts...)
}

func (p *singleflightProxy) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	// NOTE: Optimized revisions are singleflighted by the underlying datastore via the
	// CachedOptimizedRevisions struct.
	ctx, span := tracer.Start(ctx, "singleflightProxy.OptimizedRevision")
	defer span.End()
	return p.delegate.OptimizedRevision(ctx)
}

func (p *singleflightProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "singleflightProxy.CheckRevision")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, singleFlightTimeout)
	defer cancel()

	_, _, err := p.checkRevGroup.Do(ctx, revision.String(), func(ctx context.Context) (string, error) {
		return "", p.delegate.CheckRevision(ctx, revision)
	})
	return err
}

func (p *singleflightProxy) HeadRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.HeadRevision")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, singleFlightTimeout)
	defer cancel()

	rev, _, err := p.headRevGroup.Do(ctx, "", func(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
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
	ctx, span := tracer.Start(ctx, "singleflightProxy.Statistics")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, singleFlightTimeout)
	defer cancel()

	stats, _, err := p.statsGroup.Do(ctx, "", func(ctx context.Context) (datastore.Stats, error) {
		return p.delegate.Statistics(ctx)
	})
	return stats, err
}

func (p *singleflightProxy) Features(ctx context.Context) (*datastore.Features, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.Features")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, singleFlightTimeout)
	defer cancel()

	features, _, err := p.featuresGroup.Do(ctx, "", func(ctx context.Context) (*datastore.Features, error) {
		return p.delegate.Features(ctx)
	})
	return features, err
}

func (p *singleflightProxy) OfflineFeatures() (*datastore.Features, error) {
	return p.delegate.OfflineFeatures()
}

func (p *singleflightProxy) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return p.delegate.ReadyState(ctx)
}

func (p *singleflightProxy) Close() error                { return p.delegate.Close() }
func (p *singleflightProxy) Unwrap() datastore.Datastore { return p.delegate }

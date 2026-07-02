package proxy

import (
	"context"
	"time"

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
	headRevGroup  singleflight.Group[string, datastore.RevisionWithSchemaHash]
	optRevGroup   singleflight.Group[string, optimizedRevision]
	checkRevGroup singleflight.Group[string, string]
	statsGroup    singleflight.Group[string, datastore.Stats]
	delegate      datastore.Datastore
}

// optimizedRevision carries the multi-valued result of OptimizedRevision through
// the singleflight group.
type optimizedRevision struct {
	revision   datastore.Revision
	validFor   time.Duration
	schemaHash string
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

func (p *singleflightProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, time.Duration, string, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.OptimizedRevision")
	defer span.End()

	res, _, err := p.optRevGroup.Do(ctx, "", func(ctx context.Context) (optimizedRevision, error) {
		rev, validFor, schemaHash, err := p.delegate.OptimizedRevision(ctx)
		return optimizedRevision{revision: rev, validFor: validFor, schemaHash: schemaHash}, err
	})
	return res.revision, res.validFor, res.schemaHash, err
}

func (p *singleflightProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "singleflightProxy.CheckRevision")
	defer span.End()

	_, _, err := p.checkRevGroup.Do(ctx, revision.String(), func(ctx context.Context) (string, error) {
		return "", p.delegate.CheckRevision(ctx, revision)
	})
	return err
}

func (p *singleflightProxy) HeadRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.HeadRevision")
	defer span.End()

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

	stats, _, err := p.statsGroup.Do(ctx, "", func(ctx context.Context) (datastore.Stats, error) {
		return p.delegate.Statistics(ctx)
	})
	return stats, err
}

func (p *singleflightProxy) Features(ctx context.Context) (*datastore.Features, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.Features")
	defer span.End()

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

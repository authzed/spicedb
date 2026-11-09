package proxy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	"resenje.org/singleflight"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// singleflight strips the callers' deadlines from the shared call and cancels it
// only once every caller has gone away, so under steady traffic a call wedged on
// a half-open connection would otherwise block every caller indefinitely. These
// mirror the bounds optimizedRevisionProxy uses for OptimizedRevision.
const (
	defaultRevisionSharedTimeout = 2 * time.Second
	defaultRevisionRetryTimeout  = 10 * time.Second
)

var errSharedCallTimedOut = errors.New("shared datastore call timed out")

// NewSingleflightDatastoreProxy creates a new Datastore proxy which
// deduplicates calls to Datastore methods that can share results.
func NewSingleflightDatastoreProxy(d datastore.Datastore) datastore.Datastore {
	return &singleflightProxy{
		delegate:      d,
		sharedTimeout: defaultRevisionSharedTimeout,
		retryTimeout:  defaultRevisionRetryTimeout,
	}
}

type singleflightProxy struct {
	headRevGroup  singleflight.Group[string, datastore.RevisionWithSchemaHash]
	checkRevGroup singleflight.Group[string, string]
	statsGroup    singleflight.Group[string, datastore.Stats]
	delegate      datastore.Datastore
	sharedTimeout time.Duration
	retryTimeout  time.Duration
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

// OptimizedRevision is deliberately not singleflighted here.
// optimizedRevisionProxy caches and singleflights it, and relies on seeing the
// caller's own context so its direct retry runs under the caller's deadline.
func (p *singleflightProxy) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
	return p.delegate.OptimizedRevision(ctx)
}

func (p *singleflightProxy) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "singleflightProxy.CheckRevision")
	defer span.End()

	_, err := doBounded(ctx, p, &p.checkRevGroup, revision.String(), func(ctx context.Context) (string, error) {
		return "", p.delegate.CheckRevision(ctx, revision)
	})
	return err
}

func (p *singleflightProxy) HeadRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	ctx, span := tracer.Start(ctx, "singleflightProxy.HeadRevision")
	defer span.End()

	return doBounded(ctx, p, &p.headRevGroup, "", p.delegate.HeadRevision)
}

// doBounded runs fn under group with the shared call bounded by p.sharedTimeout.
// If the shared call times out while the caller is still waiting, fn is retried
// once directly on the caller's context.
func doBounded[V any](ctx context.Context, p *singleflightProxy, group *singleflight.Group[string, V], key string, fn func(context.Context) (V, error)) (V, error) {
	v, _, err := group.Do(ctx, key, func(sfCtx context.Context) (V, error) {
		sfCtx, cancel := context.WithTimeout(sfCtx, p.sharedTimeout)
		defer cancel()

		v, err := fn(sfCtx)
		if err != nil && errors.Is(sfCtx.Err(), context.DeadlineExceeded) {
			return v, fmt.Errorf("%w: %w", errSharedCallTimedOut, err)
		}
		return v, err
	})
	if !errors.Is(err, errSharedCallTimedOut) || ctx.Err() != nil {
		return v, err
	}

	trace.SpanFromContext(ctx).AddEvent(otelconv.EventDatastoreRevisionsSharedFailedRetrying)
	log.Ctx(ctx).Warn().Err(err).Msg("shared datastore revision call timed out; retrying directly")

	retryCtx, cancel := context.WithTimeout(ctx, p.retryTimeout)
	defer cancel()
	return fn(retryCtx)
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

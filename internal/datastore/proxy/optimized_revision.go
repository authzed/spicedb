package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel/trace"
	"resenje.org/singleflight"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
)

// defaultOptimizedRevisionSharedTimeout aggressively bounds the *shared*,
// singleflighted computation of the optimized revision.
//
// The computation runs under singleflight, which replaces the caller's context
// with one that has *no* deadline (see resenje.org/singleflight: the work context
// is `context.WithoutCancel(ctx)`, cancelled only once every awaiting caller has
// gone away). The per-request gRPC deadline that would normally abort a stuck
// datastore query is therefore stripped. Worse, because every API request needs
// an optimized revision and they all de-duplicate onto this one in-flight call,
// every concurrent caller observes its latency directly: a wedged computation
// (e.g. a half-open connection silently dropped by a load balancer) inflates the
// P99 of the entire system, not just one request.
//
// Keeping this bound low limits that blast radius. The healthy computation takes
// single-digit milliseconds, so this is extremely generous for the happy path. On
// failure, OptimizedRevision retries the computation directly (bypassing
// singleflight) on the caller's own context, so the aggressive bound does not turn
// a transient slow/wedged shared attempt into a failed request.
const defaultOptimizedRevisionSharedTimeout = 2 * time.Second

// defaultOptimizedRevisionFallbackTimeout bounds the *direct* retry performed
// after a failed shared attempt. The retry runs on the caller's own context, so
// its deadline applies when present; this fallback only takes effect for callers
// with no deadline of their own, preserving the guarantee that a revision
// computation can never block forever.
const defaultOptimizedRevisionFallbackTimeout = 10 * time.Second

// NewOptimizedRevisionProxy wraps a datastore with an in-process cache for its
// optimized revision. Concurrent misses are deduplicated via singleflight, the
// expiry decision is jittered by up to maxStaleness to avoid a thundering herd at
// quantization boundaries, and the shared computation is bounded so a wedged
// datastore call cannot pin the latency of every waiting caller.
//
// The wrapped datastore's OptimizedRevision is expected to be uncached: one
// database round-trip per call, returning the revision, how long it remains
// valid, and the schema hash visible at it. Caching is entirely the proxy's job.
func NewOptimizedRevisionProxy(d datastore.Datastore, maxStaleness time.Duration) datastore.Datastore {
	return &optimizedRevisionProxy{
		Datastore:       d,
		maxStaleness:    maxStaleness,
		sharedTimeout:   defaultOptimizedRevisionSharedTimeout,
		fallbackTimeout: defaultOptimizedRevisionFallbackTimeout,
		clock:           clock.New(),
	}
}

// optimizedRevisionProxy is both the proxy in the datastore chain and the cache
// itself: the candidate list, its mutex, the singleflight group, and the jitter
// all live directly on it.
type optimizedRevisionProxy struct {
	datastore.Datastore

	maxStaleness    time.Duration
	sharedTimeout   time.Duration
	fallbackTimeout time.Duration
	clock           clock.Clock

	mu         sync.Mutex
	candidates []validRevision // GUARDED_BY(mu)

	// flight consolidates concurrent misses into a single datastore round-trip.
	flight singleflight.Group[string, cachedRevision]
}

// cachedRevision is the value carried through the singleflight group.
type cachedRevision struct {
	revision   datastore.Revision
	validFor   time.Duration
	schemaHash string
}

// validRevision is a cached candidate revision and the wall-clock time through
// which it remains acceptable.
type validRevision struct {
	revision     datastore.Revision
	validThrough time.Time
	schemaHash   string
}

func (p *optimizedRevisionProxy) Unwrap() datastore.Datastore {
	return p.Datastore
}

func (p *optimizedRevisionProxy) OptimizedRevision(ctx context.Context) (datastore.Revision, time.Duration, string, error) {
	ctx, span := tracer.Start(ctx, "optimizedRevisionProxy.OptimizedRevision")
	defer span.End()

	localNow := p.clock.Now()

	// Subtract a random amount of time from now, to let barely expired candidates get selected.
	adjustedNow := localNow
	if p.maxStaleness > 0 {
		// nolint:gosec
		// G404 use of non cryptographically secure random number generator is not a security concern here,
		// as we are using it to introduce randomness to the accepted staleness of a revision and reduce the odds of
		// a thundering herd to the datastore
		adjustedNow = localNow.Add(-1 * time.Duration(rand.Int63n(p.maxStaleness.Nanoseconds())) * time.Nanosecond)
	}

	p.mu.Lock()
	for _, candidate := range p.candidates {
		if candidate.validThrough.After(adjustedNow) {
			p.mu.Unlock()
			log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", candidate.validThrough).Msg("returning cached revision")
			span.AddEvent(otelconv.EventDatastoreRevisionsCacheReturned)
			return candidate.revision, candidate.validThrough.Sub(localNow), candidate.schemaHash, nil
		}
	}
	p.mu.Unlock()

	// Compute the revision under singleflight so concurrent callers share a single
	// datastore round-trip. The shared call is aggressively bounded (see
	// defaultOptimizedRevisionSharedTimeout) so a wedged computation cannot pin the
	// latency of every waiting caller.
	result, _, err := p.flight.Do(ctx, "", func(sfCtx context.Context) (cachedRevision, error) {
		// NOTE: singleflight hands this function a context with the caller's
		// deadline stripped. Re-impose a (low) deadline so a hung datastore call
		// cannot block this in-flight call, and therefore every caller waiting on
		// it, beyond the bound.
		if p.sharedTimeout > 0 {
			var cancel context.CancelFunc
			sfCtx, cancel = context.WithTimeout(sfCtx, p.sharedTimeout)
			defer cancel()
		}

		return p.compute(sfCtx, localNow, span)
	})
	if err == nil {
		return result.revision, result.validFor, result.schemaHash, nil
	}

	// If this caller's own context is already done, surface its cancellation /
	// deadline error directly; there is nothing to retry.
	if ctx.Err() != nil {
		return datastore.NoRevision, 0, "", ctx.Err()
	}

	// The shared, aggressively-bounded attempt failed (e.g. a wedged connection
	// tripped the timeout, or the datastore returned a transient error). Give this
	// request one more chance, directly and outside of singleflight, on its own
	// context: this is not capped by the low shared bound and cannot re-attach to a
	// poisoned in-flight call, so a brief wedge does not fail the request. For the
	// pooled SQL datastores the retry is naturally throttled by the pool's maximum
	// size; Spanner's client similarly bounds concurrent calls via its own session
	// pool. Even when many waiters retry at once, the datastore is not overwhelmed.
	span.AddEvent(otelconv.EventDatastoreRevisionsSharedFailedRetrying)
	log.Ctx(ctx).Warn().Err(err).Msg("shared optimized revision computation failed; retrying directly")

	retryCtx, cancel := context.WithTimeout(ctx, p.fallbackTimeout)
	defer cancel()
	result, err = p.compute(retryCtx, p.clock.Now(), span)
	if err != nil {
		return datastore.NoRevision, 0, "", err
	}
	return result.revision, result.validFor, result.schemaHash, nil
}

// compute fetches an uncached optimized revision from the wrapped datastore and
// records it as a cache candidate. It is invoked both from the shared singleflight
// path and from the direct retry that follows a failed shared attempt.
func (p *optimizedRevisionProxy) compute(ctx context.Context, localNow time.Time, span trace.Span) (cachedRevision, error) {
	log.Ctx(ctx).Debug().Time("now", localNow).Msg("computing new revision")

	optimized, validFor, schemaHash, err := p.Datastore.OptimizedRevision(ctx)
	if err != nil {
		return cachedRevision{}, fmt.Errorf("unable to compute optimized revision: %w", err)
	}

	rvt := localNow.Add(validFor)

	// Prune the candidates that have definitely expired.
	p.mu.Lock()
	var numToDrop uint
	for _, candidate := range p.candidates {
		if candidate.validThrough.Add(p.maxStaleness).Before(localNow) {
			numToDrop++
		} else {
			break
		}
	}

	p.candidates = p.candidates[numToDrop:]
	p.candidates = append(p.candidates, validRevision{optimized, rvt, schemaHash})
	p.mu.Unlock()

	span.AddEvent(otelconv.EventDatastoreRevisionsComputed)
	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rvt).Stringer("validFor", validFor).Msg("setting valid through")
	return cachedRevision{revision: optimized, validFor: validFor, schemaHash: schemaHash}, nil
}

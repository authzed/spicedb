package revisions

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"resenje.org/singleflight"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
	"github.com/authzed/spicedb/pkg/datastore"
)

var tracer = otel.Tracer("spicedb/internal/datastore/common/revisions")

// defaultOptimizedRevisionTimeout aggressively bounds the *shared*,
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
const defaultOptimizedRevisionTimeout = 2 * time.Second

// defaultOptimizedRevisionFallbackTimeout bounds the *direct* retry performed
// after a failed shared attempt. The retry runs on the caller's own context, so
// its deadline applies when present; this fallback only takes effect for callers
// with no deadline of their own, preserving the guarantee that a revision
// computation can never block forever.
const defaultOptimizedRevisionFallbackTimeout = 10 * time.Second

// OptimizedRevisionFunction instructs the datastore to compute its own current
// optimized revision given the specific quantization, and return for how long
// it will remain valid, along with the schema hash at that revision (or "" if
// the datastore does not provide one on this code path).
type OptimizedRevisionFunction func(context.Context) (rev datastore.Revision, validFor time.Duration, schemaHash string, err error)

// NewCachedOptimizedRevisions returns a CachedOptimizedRevisions for the given configuration
func NewCachedOptimizedRevisions(maxRevisionStaleness time.Duration) *CachedOptimizedRevisions {
	return &CachedOptimizedRevisions{
		maxRevisionStaleness:             maxRevisionStaleness,
		optimizedRevisionTimeout:         defaultOptimizedRevisionTimeout,
		optimizedRevisionFallbackTimeout: defaultOptimizedRevisionFallbackTimeout,
		clockFn:                          clock.New(),
	}
}

// SetOptimizedRevisionFunc must be called after construction, and is the method
// by which one specializes this helper for a specific datastore.
func (cor *CachedOptimizedRevisions) SetOptimizedRevisionFunc(revisionFunc OptimizedRevisionFunction) {
	cor.optimizedFunc = revisionFunc
}

// SetOptimizedRevisionTimeout overrides the maximum duration the shared,
// singleflighted call to the optimized revision function is allowed to run before
// it is cancelled. See defaultOptimizedRevisionTimeout for why this bound is
// required.
func (cor *CachedOptimizedRevisions) SetOptimizedRevisionTimeout(timeout time.Duration) {
	cor.optimizedRevisionTimeout = timeout
}

// SetOptimizedRevisionFallbackTimeout overrides the bound applied to the direct
// retry performed after a failed shared attempt for callers that have no deadline
// of their own. See defaultOptimizedRevisionFallbackTimeout.
func (cor *CachedOptimizedRevisions) SetOptimizedRevisionFallbackTimeout(timeout time.Duration) {
	cor.optimizedRevisionFallbackTimeout = timeout
}

func (cor *CachedOptimizedRevisions) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	ctx, span := tracer.Start(ctx, "CachedOptimizedRevisions.OptimizedRevision")
	defer span.End()

	localNow := cor.clockFn.Now()

	// Subtract a random amount of time from now, to let barely expired candidates get selected
	adjustedNow := localNow
	if cor.maxRevisionStaleness > 0 {
		// nolint:gosec
		// G404 use of non cryptographically secure random number generator is not a security concern here,
		// as we are using it to introduce randomness to the accepted staleness of a revision and reduce the odds of
		// a thundering herd to the datastore
		adjustedNow = localNow.Add(-1 * time.Duration(rand.Int63n(cor.maxRevisionStaleness.Nanoseconds())) * time.Nanosecond)
	}

	cor.RLock()
	for _, candidate := range cor.candidates {
		if candidate.validThrough.After(adjustedNow) {
			cor.RUnlock()
			log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", candidate.validThrough).Msg("returning cached revision")
			span.AddEvent(otelconv.EventDatastoreRevisionsCacheReturned)
			return datastore.RevisionWithSchemaHash{Revision: candidate.revision, SchemaHash: candidate.schemaHash}, nil
		}
	}
	cor.RUnlock()

	// Compute the revision under singleflight so concurrent callers share a single
	// datastore round-trip. The shared call is aggressively bounded (see
	// defaultOptimizedRevisionTimeout) so a wedged computation cannot pin the
	// latency of every waiting caller.
	result, _, err := cor.updateGroup.Do(ctx, "", func(sfCtx context.Context) (datastore.RevisionWithSchemaHash, error) {
		// NOTE: singleflight hands this function a context with the caller's
		// deadline stripped. Re-impose a (low) deadline so a hung datastore call
		// cannot block this in-flight call, and therefore every caller waiting on
		// it, beyond the bound.
		if cor.optimizedRevisionTimeout > 0 {
			var cancel context.CancelFunc
			sfCtx, cancel = context.WithTimeout(sfCtx, cor.optimizedRevisionTimeout)
			defer cancel()
		}

		return cor.computeOptimizedRevision(sfCtx, localNow, span)
	})
	if err == nil {
		return result, nil
	}

	// If this caller's own context is already done, the shared call returned its
	// error; there is nothing to retry.
	if ctx.Err() != nil {
		return datastore.RevisionWithSchemaHash{}, err
	}

	// The shared, aggressively-bounded attempt failed (e.g. a wedged connection
	// tripped the timeout, or the datastore returned a transient error). Give this
	// request one more chance, directly and outside of singleflight, on its own
	// context: this is not capped by the low shared bound and cannot re-attach to a
	// poisoned in-flight call, so a brief wedge does not fail the request. For the
	// pooled SQL datastores the retry is naturally throttled by the pool's maximum
	// size; Spanner's client similarly bounds concurrent calls via its own session
	// pool. Even when many waiters retry at once, the datastore is not overwhelmed.
	log.Ctx(ctx).Warn().Err(err).Msg("shared optimized revision computation failed; retrying directly")

	retryCtx, cancel := context.WithTimeout(ctx, cor.optimizedRevisionFallbackTimeout)
	defer cancel()
	return cor.computeOptimizedRevision(retryCtx, cor.clockFn.Now(), span)
}

// computeOptimizedRevision invokes the datastore's optimized revision function and
// records the result as a cache candidate. It is invoked both from the shared
// singleflight path and from the direct retry that follows a failed shared attempt.
func (cor *CachedOptimizedRevisions) computeOptimizedRevision(ctx context.Context, localNow time.Time, span trace.Span) (datastore.RevisionWithSchemaHash, error) {
	log.Ctx(ctx).Debug().Time("now", localNow).Msg("computing new revision")

	optimized, validFor, schemaHash, err := cor.optimizedFunc(ctx)
	if err != nil {
		return datastore.RevisionWithSchemaHash{}, fmt.Errorf("unable to compute optimized revision: %w", err)
	}

	rvt := localNow.Add(validFor)

	// Prune the candidates that have definitely expired
	cor.Lock()
	var numToDrop uint
	for _, candidate := range cor.candidates {
		if candidate.validThrough.Add(cor.maxRevisionStaleness).Before(localNow) {
			numToDrop++
		} else {
			break
		}
	}

	cor.candidates = cor.candidates[numToDrop:]
	cor.candidates = append(cor.candidates, validRevision{optimized, rvt, schemaHash})
	cor.Unlock()

	span.AddEvent(otelconv.EventDatastoreRevisionsComputed)
	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rvt).Stringer("validFor", validFor).Msg("setting valid through")
	return datastore.RevisionWithSchemaHash{Revision: optimized, SchemaHash: schemaHash}, nil
}

// CachedOptimizedRevisions does caching and deduplication for requests for optimized revisions.
type CachedOptimizedRevisions struct {
	sync.RWMutex

	maxRevisionStaleness             time.Duration
	optimizedRevisionTimeout         time.Duration
	optimizedRevisionFallbackTimeout time.Duration
	optimizedFunc                    OptimizedRevisionFunction
	clockFn                          clock.Clock

	// these values are read and set by multiple consumers
	candidates []validRevision // GUARDED_BY(RWMutex)

	// the updategroup consolidates concurrent requests to the database into 1
	updateGroup singleflight.Group[string, datastore.RevisionWithSchemaHash]
}

type validRevision struct {
	revision     datastore.Revision
	validThrough time.Time
	schemaHash   string
}

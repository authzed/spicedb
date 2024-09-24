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
	"golang.org/x/sync/singleflight"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

var tracer = otel.Tracer("spicedb/internal/datastore/common/revisions")

// OptimizedRevisionFunction instructs the datastore to compute its own current
// optimized revision given the specific quantization, and return for how long
// it will remain valid.
type OptimizedRevisionFunction func(context.Context) (rev datastore.Revision, validFor time.Duration, err error)

// NewCachedOptimizedRevisions returns a CachedOptimizedRevisions for the given configuration
func NewCachedOptimizedRevisions(maxRevisionStaleness time.Duration) *CachedOptimizedRevisions {
	return &CachedOptimizedRevisions{
		maxRevisionStaleness: maxRevisionStaleness,
		clockFn:              clock.New(),
	}
}

// SetOptimizedRevisionFunc must be called after construction, and is the method
// by which one specializes this helper for a specific datastore.
func (cor *CachedOptimizedRevisions) SetOptimizedRevisionFunc(revisionFunc OptimizedRevisionFunction) {
	cor.optimizedFunc = revisionFunc
}

func (cor *CachedOptimizedRevisions) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	span := trace.SpanFromContext(ctx)
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
			span.AddEvent("returning cached revision")
			return candidate.revision, nil
		}
	}
	cor.RUnlock()

	newQuantizedRevision, err, _ := cor.updateGroup.Do("", func() (interface{}, error) {
		log.Ctx(ctx).Debug().Time("now", localNow).Msg("computing new revision")
		span.AddEvent("computing new revision")

		optimized, validFor, err := cor.optimizedFunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to compute optimized revision: %w", err)
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
		cor.candidates = append(cor.candidates, validRevision{optimized, rvt})
		cor.Unlock()

		log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rvt).Stringer("validFor", validFor).Msg("setting valid through")
		return optimized, nil
	})
	if err != nil {
		return datastore.NoRevision, err
	}
	return newQuantizedRevision.(datastore.Revision), err
}

// CachedOptimizedRevisions does caching and deduplication for requests for optimized revisions.
type CachedOptimizedRevisions struct {
	sync.RWMutex

	maxRevisionStaleness time.Duration
	optimizedFunc        OptimizedRevisionFunction
	clockFn              clock.Clock

	// these values are read and set by multiple consumers, they're protected
	// by a mutex
	candidates []validRevision

	// the updategroup consolidates concurrent requests to the database into 1
	updateGroup singleflight.Group
}

type validRevision struct {
	revision     datastore.Revision
	validThrough time.Time
}

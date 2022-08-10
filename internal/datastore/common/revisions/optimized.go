package revisions

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/pkg/datastore"
)

var tracer = otel.Tracer("spicedb/internal/datastore/common/revisions")

// OptimizedRevisionFunction instructs the datastore to compute its own current
// optimized revision given the specific quantization, and return for how long
// it will remain valid.
type OptimizedRevisionFunction func(context.Context) (rev datastore.Revision, validFor time.Duration, err error)

// NewCachedOptimizedRevisions returns a CachedOptimizedRevisions for the given configuration
func NewCachedOptimizedRevisions(maxRevisionStaleness time.Duration) *CachedOptimizedRevisions {
	rev := atomicRevision{}
	rev.set(validRevision{decimal.Zero, time.Time{}})
	return &CachedOptimizedRevisions{
		maxRevisionStaleness:  maxRevisionStaleness,
		lastQuantizedRevision: &rev,
		clockFn:               clock.New(),
	}
}

// SetOptimizedRevisionFunc must be called after construction, and is the method
// by which one specializes this helper for a specific datastore.
func (cor *CachedOptimizedRevisions) SetOptimizedRevisionFunc(revisionFunc OptimizedRevisionFunction) {
	cor.optimizedFunc = revisionFunc
}

func (cor *CachedOptimizedRevisions) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	localNow := cor.clockFn.Now()
	lastRevision := cor.lastQuantizedRevision.get()
	if localNow.Before(lastRevision.validThrough) {
		log.Debug().Time("now", localNow).Time("valid", lastRevision.validThrough).Msg("returning cached revision")
		span.AddEvent("returning cached revision")
		return lastRevision.revision, nil
	}

	lastQuantizedRevision, err, _ := cor.updateGroup.Do("", func() (interface{}, error) {
		log.Debug().Time("now", localNow).Time("valid", lastRevision.validThrough).Msg("computing new revision")
		span.AddEvent("computing new revision")

		optimized, validFor, err := cor.optimizedFunc(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to compute optimized revision: %w", err)
		}

		rvt := localNow.
			Add(validFor).
			Add(cor.maxRevisionStaleness)
		cor.lastQuantizedRevision.set(validRevision{optimized, rvt})
		log.Debug().Time("now", localNow).Time("valid", rvt).Stringer("validFor", validFor).Msg("setting valid through")

		return optimized, nil
	})
	if err != nil {
		return datastore.NoRevision, err
	}
	return lastQuantizedRevision.(decimal.Decimal), err
}

// CachedOptimizedRevisions does caching and deduplication for requests for optimized revisions.
type CachedOptimizedRevisions struct {
	maxRevisionStaleness time.Duration
	optimizedFunc        OptimizedRevisionFunction
	clockFn              clock.Clock

	// this value is read and set by multiple consumers, it's protected
	// by atomic load/store
	lastQuantizedRevision *atomicRevision

	// the updategroup consolidates concurrent requests to the database into 1
	updateGroup singleflight.Group
}

type validRevision struct {
	revision     decimal.Decimal
	validThrough time.Time
}

// safeRevision is a wrapper that protects a revision with atomic
type atomicRevision struct {
	v atomic.Value
}

func (r *atomicRevision) get() validRevision {
	return r.v.Load().(validRevision)
}

func (r *atomicRevision) set(revision validRevision) {
	r.v.Store(revision)
}

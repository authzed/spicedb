package common

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/internal/datastore"
)

var tracer = otel.Tracer("spicedb/internal/datastore/common")

// RemoteNowFunction queries the datastore to get a current revision
type RemoteNowFunction func(context.Context) (datastore.Revision, error)

// RemoteClockRevisions handles revision calculation for datastores that provide
// their own clocks.
type RemoteClockRevisions struct {
	quantizationNanos      int64
	gcWindowNanos          int64
	followerReadDelayNanos int64
	maxRevisionStaleness   time.Duration
	nowFunc                RemoteNowFunction

	// these values are read and set by multiple consumers, they're protected
	// by atomic load/store
	lastQuantizedRevision *safeRevision
	revisionValidThrough  *safeTime

	// the updategroup consolidates concurrent requests to the database into 1
	updateGroup singleflight.Group
}

// NewRemoteClockRevisions returns a RemoteClockRevisions for the given configuration
func NewRemoteClockRevisions(quantizationNanos, gcWindowNanos, followerReadDelayNanos int64, maxRevisionStaleness time.Duration) *RemoteClockRevisions {
	rev := safeRevision{}
	rev.set(decimal.Zero)
	t := safeTime{}
	t.set(time.Time{})
	return &RemoteClockRevisions{
		quantizationNanos:      quantizationNanos,
		gcWindowNanos:          gcWindowNanos,
		followerReadDelayNanos: followerReadDelayNanos,
		maxRevisionStaleness:   maxRevisionStaleness,
		lastQuantizedRevision:  &rev,
		revisionValidThrough:   &t,
	}
}

// SetNowFunc sets the function used to determine the head revision
func (rcr *RemoteClockRevisions) SetNowFunc(nowFunc RemoteNowFunction) {
	rcr.nowFunc = nowFunc
}

// OptimizedRevision picks a revision that is valid for the request's
// consistency level and most likely to have valid cached subproblems.
func (rcr *RemoteClockRevisions) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	localNow := time.Now()
	revisionValidThrough := rcr.revisionValidThrough.get()
	if localNow.Before(revisionValidThrough) {
		log.Debug().Time("now", localNow).Time("valid", revisionValidThrough).Msg("returning cached revision")
		return rcr.lastQuantizedRevision.get(), nil
	}

	lastQuantizedRevision, err, _ := rcr.updateGroup.Do("", func() (interface{}, error) {
		log.Debug().Time("now", localNow).Time("valid", revisionValidThrough).Msg("computing new revision")

		nowHLC, err := rcr.nowFunc(ctx)
		if err != nil {
			return datastore.NoRevision, err
		}

		// Round the revision down to the nearest quantization
		// Apply a delay to enable follower reads: https://www.cockroachlabs.com/docs/stable/follower-reads.html
		// This is currently only used for crdb, but other datastores may have similar features in the future
		now := nowHLC.IntPart() - rcr.followerReadDelayNanos
		quantized := now
		if rcr.quantizationNanos > 0 {
			quantized -= (now % rcr.quantizationNanos)
		}
		log.Debug().Int64("readSkew", rcr.followerReadDelayNanos).Int64("totalSkew", nowHLC.IntPart()-quantized).Msg("revision skews")

		validForNanos := (quantized + rcr.quantizationNanos) - now

		rvt := localNow.
			Add(time.Duration(validForNanos) * time.Nanosecond).
			Add(rcr.maxRevisionStaleness)
		rcr.revisionValidThrough.set(rvt)
		log.Debug().Time("now", localNow).Time("valid", rvt).Int64("validForNanos", validForNanos).Msg("setting valid through")

		lqr := decimal.NewFromInt(quantized)
		rcr.lastQuantizedRevision.set(lqr)

		return lqr, nil
	})

	return lastQuantizedRevision.(decimal.Decimal), err
}

// CheckRevision asserts whether a given revision is valid
func (rcr *RemoteClockRevisions) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	// Make sure the system time indicated is within the software GC window
	now, err := rcr.nowFunc(ctx)
	if err != nil {
		return err
	}

	nowNanos := now.IntPart()
	revisionNanos := revision.IntPart()

	isStale := revisionNanos < (nowNanos - rcr.gcWindowNanos)
	if isStale {
		log.Debug().Stringer("now", now).Stringer("revision", revision).Msg("stale revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	isFuture := revisionNanos > nowNanos
	if isFuture {
		log.Debug().Stringer("now", now).Stringer("revision", revision).Msg("future revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	return nil
}

// safeRevision is a wrapper that protects a revision with atomic
type safeRevision struct {
	v atomic.Value
}

func (r *safeRevision) get() decimal.Decimal {
	return r.v.Load().(decimal.Decimal)
}

func (r *safeRevision) set(revision decimal.Decimal) {
	r.v.Store(revision)
}

// safeTime is a wrapper that protects a time with atomic
type safeTime struct {
	v atomic.Value
}

func (t *safeTime) get() time.Time {
	return t.v.Load().(time.Time)
}

func (t *safeTime) set(in time.Time) {
	t.v.Store(in)
}

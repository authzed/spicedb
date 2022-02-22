package common

import (
	"context"
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
	QuantizationNanos      int64
	GCWindowNanos          int64
	FollowerReadDelayNanos int64
	MaxRevisionStaleness   time.Duration
	NowFunc                RemoteNowFunction

	updateGroup           singleflight.Group
	lastQuantizedRevision decimal.Decimal
	revisionValidThrough  time.Time
}

// OptimizedRevision picks a revision that is valid for the request's
// consistency level and most likely to have valid cached subproblems.
func (rcr *RemoteClockRevisions) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "OptimizedRevision")
	defer span.End()

	localNow := time.Now()
	if localNow.Before(rcr.revisionValidThrough) {
		log.Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Msg("returning cached revision")
		return rcr.lastQuantizedRevision, nil
	}

	lastQuantizedRevision, err, _ := rcr.updateGroup.Do("", func() (interface{}, error) {
		log.Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Msg("computing new revision")

		nowHLC, err := rcr.NowFunc(ctx)
		if err != nil {
			return datastore.NoRevision, err
		}

		// Round the revision down to the nearest quantization
		// Apply a delay to enable follower reads: https://www.cockroachlabs.com/docs/stable/follower-reads.html
		// This is currently only used for crdb, but other datastores may have similar features in the future
		now := nowHLC.IntPart() - rcr.FollowerReadDelayNanos
		quantized := now
		if rcr.QuantizationNanos > 0 {
			quantized -= (now % rcr.QuantizationNanos)
		}
		log.Debug().Int64("readSkew", rcr.FollowerReadDelayNanos).Int64("totalSkew", nowHLC.IntPart()-quantized).Msg("revision skews")

		validForNanos := (quantized + rcr.QuantizationNanos) - now

		rcr.revisionValidThrough = localNow.
			Add(time.Duration(validForNanos) * time.Nanosecond).
			Add(rcr.MaxRevisionStaleness)
		log.Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Int64("validForNanos", validForNanos).Msg("setting valid through")
		rcr.lastQuantizedRevision = decimal.NewFromInt(quantized)

		return rcr.lastQuantizedRevision, nil
	})

	return lastQuantizedRevision.(decimal.Decimal), err
}

// CheckRevision asserts whether a given revision is valid
func (rcr *RemoteClockRevisions) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	// Make sure the system time indicated is within the software GC window
	now, err := rcr.NowFunc(ctx)
	if err != nil {
		return err
	}

	nowNanos := now.IntPart()
	revisionNanos := revision.IntPart()

	isStale := revisionNanos < (nowNanos - rcr.GCWindowNanos)
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

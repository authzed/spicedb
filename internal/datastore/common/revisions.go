package common

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
)

type RemoteNowFunction func(context.Context) (datastore.Revision, error)

type RemoteClockRevisions struct {
	QuantizationNanos      int64
	GCWindowNanos          int64
	FollowerReadDelayNanos int64
	MaxRevisionStaleness   time.Duration
	NowFunc                RemoteNowFunction

	lastQuantizedRevision decimal.Decimal
	revisionValidThrough  time.Time
}

func (rcr *RemoteClockRevisions) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	// ctx, span := tracer.Start(ctx, "OptimizedRevision")
	// defer span.End()

	localNow := time.Now()
	if localNow.Before(rcr.revisionValidThrough) {
		log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Msg("returning cached revision")
		return rcr.lastQuantizedRevision, nil
	}

	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Msg("computing new revision")

	nowHLC, err := rcr.NowFunc(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	// Round the revision down to the nearest quantization
	// Apply a delay to enable follower reads: https://www.cockroachlabs.com/docs/stable/follower-reads.html
	crdbNow := nowHLC.IntPart() - rcr.FollowerReadDelayNanos
	quantized := crdbNow
	if rcr.QuantizationNanos > 0 {
		quantized -= (crdbNow % rcr.QuantizationNanos)
	}
	log.Ctx(ctx).Debug().Int64("readSkew", rcr.FollowerReadDelayNanos).Int64("totalSkew", nowHLC.IntPart()-quantized).Msg("revision skews")

	validForNanos := (quantized + rcr.QuantizationNanos) - crdbNow

	rcr.revisionValidThrough = localNow.
		Add(time.Duration(validForNanos) * time.Nanosecond).
		Add(rcr.MaxRevisionStaleness)
	log.Ctx(ctx).Debug().Time("now", localNow).Time("valid", rcr.revisionValidThrough).Int64("validForNanos", validForNanos).Msg("setting valid through")
	rcr.lastQuantizedRevision = decimal.NewFromInt(quantized)

	return rcr.lastQuantizedRevision, nil
}

func (rcr *RemoteClockRevisions) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	// ctx, span := tracer.Start(ctx, "CheckRevision")
	// defer span.End()

	// Make sure the system time indicated is within the software GC window
	now, err := rcr.NowFunc(ctx)
	if err != nil {
		return err
	}

	nowNanos := now.IntPart()
	revisionNanos := revision.IntPart()

	staleRevision := revisionNanos < (nowNanos - rcr.GCWindowNanos)
	if staleRevision {
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("stale revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	futureRevision := revisionNanos > nowNanos
	if futureRevision {
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("future revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	return nil
}

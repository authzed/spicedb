package revisions

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

// RemoteNowFunction queries the datastore to get a current revision.
type RemoteNowFunction func(context.Context) (revision.Decimal, error)

// RemoteClockRevisions handles revision calculation for datastores that provide
// their own clocks.
type RemoteClockRevisions struct {
	*CachedOptimizedRevisions

	gcWindowNanos          int64
	nowFunc                RemoteNowFunction
	followerReadDelayNanos int64
	quantizationNanos      int64
}

// NewRemoteClockRevisions returns a RemoteClockRevisions for the given configuration
func NewRemoteClockRevisions(gcWindow, maxRevisionStaleness, followerReadDelay, quantization time.Duration) *RemoteClockRevisions {
	revisions := &RemoteClockRevisions{
		CachedOptimizedRevisions: NewCachedOptimizedRevisions(
			maxRevisionStaleness,
		),
		gcWindowNanos:          gcWindow.Nanoseconds(),
		followerReadDelayNanos: followerReadDelay.Nanoseconds(),
		quantizationNanos:      quantization.Nanoseconds(),
	}

	revisions.SetOptimizedRevisionFunc(revisions.optimizedRevisionFunc)

	return revisions
}

func (rcr *RemoteClockRevisions) optimizedRevisionFunc(ctx context.Context) (datastore.Revision, time.Duration, error) {
	nowHLC, err := rcr.nowFunc(ctx)
	if err != nil {
		return revision.NoRevision, 0, err
	}

	delayedNow := nowHLC.IntPart() - rcr.followerReadDelayNanos
	quantized := delayedNow
	validForNanos := int64(0)
	if rcr.quantizationNanos > 0 {
		afterLastQuantization := delayedNow % rcr.quantizationNanos
		quantized -= afterLastQuantization
		validForNanos = rcr.quantizationNanos - afterLastQuantization
	}
	log.Ctx(ctx).Debug().Int64("readSkew", rcr.followerReadDelayNanos).Int64("totalSkew", nowHLC.IntPart()-quantized).Msg("revision skews")

	return revision.NewFromDecimal(decimal.NewFromInt(quantized)), time.Duration(validForNanos) * time.Nanosecond, nil
}

// SetNowFunc sets the function used to determine the head revision
func (rcr *RemoteClockRevisions) SetNowFunc(nowFunc RemoteNowFunction) {
	rcr.nowFunc = nowFunc
}

func (rcr *RemoteClockRevisions) CheckRevision(ctx context.Context, dsRevision datastore.Revision) error {
	if dsRevision == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(dsRevision, datastore.CouldNotDetermineRevision)
	}

	revision := dsRevision.(revision.Decimal)

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
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("stale revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	isUnknown := revisionNanos > nowNanos
	if isUnknown {
		log.Ctx(ctx).Debug().Stringer("now", now).Stringer("revision", revision).Msg("unknown revision")
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	return nil
}

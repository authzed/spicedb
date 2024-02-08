package revisions

import (
	"context"
	"time"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// RemoteNowFunction queries the datastore to get a current revision.
type RemoteNowFunction func(context.Context) (datastore.Revision, error)

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
	// Ensure the max revision staleness never exceeds the GC window.
	if maxRevisionStaleness > gcWindow {
		log.Warn().
			Dur("maxRevisionStaleness", maxRevisionStaleness).
			Dur("gcWindow", gcWindow).
			Msg("the configured maximum revision staleness exceeds the configured gc window, so capping to gcWindow")
		maxRevisionStaleness = gcWindow - 1
	}

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
	nowRev, err := rcr.nowFunc(ctx)
	if err != nil {
		return datastore.NoRevision, 0, err
	}

	if nowRev == datastore.NoRevision {
		return datastore.NoRevision, 0, datastore.NewInvalidRevisionErr(nowRev, datastore.CouldNotDetermineRevision)
	}

	nowTS, ok := nowRev.(WithTimestampRevision)
	if !ok {
		return datastore.NoRevision, 0, spiceerrors.MustBugf("expected with-timestamp revision, got %T", nowRev)
	}

	delayedNow := nowTS.TimestampNanoSec() - rcr.followerReadDelayNanos
	quantized := delayedNow
	validForNanos := int64(0)
	if rcr.quantizationNanos > 0 {
		afterLastQuantization := delayedNow % rcr.quantizationNanos
		quantized -= afterLastQuantization
		validForNanos = rcr.quantizationNanos - afterLastQuantization
	}
	log.Ctx(ctx).Debug().
		Time("quantized", time.Unix(0, quantized)).
		Int64("readSkew", rcr.followerReadDelayNanos).
		Int64("totalSkew", nowTS.TimestampNanoSec()-quantized).
		Msg("revision skews")

	return nowTS.ConstructForTimestamp(quantized), time.Duration(validForNanos) * time.Nanosecond, nil
}

// SetNowFunc sets the function used to determine the head revision
func (rcr *RemoteClockRevisions) SetNowFunc(nowFunc RemoteNowFunction) {
	rcr.nowFunc = nowFunc
}

func (rcr *RemoteClockRevisions) CheckRevision(ctx context.Context, dsRevision datastore.Revision) error {
	if dsRevision == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(dsRevision, datastore.CouldNotDetermineRevision)
	}

	revision := dsRevision.(WithTimestampRevision)

	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	// Make sure the system time indicated is within the software GC window
	now, err := rcr.nowFunc(ctx)
	if err != nil {
		return err
	}

	nowTS, ok := now.(WithTimestampRevision)
	if !ok {
		return spiceerrors.MustBugf("expected HLC revision, got %T", now)
	}

	nowNanos := nowTS.TimestampNanoSec()
	revisionNanos := revision.TimestampNanoSec()

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

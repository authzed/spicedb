package revisions

import (
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// QuantizeHLC rounds an HLC "now" revision down to a quantization boundary,
// after subtracting the follower-read delay, and reports how long the resulting
// quantized revision remains valid (i.e. until the next quantization boundary).
//
// This is the in-Go quantization used by datastores whose only revision
// primitive is a raw HLC clock (CockroachDB, Spanner). Datastores that quantize
// in SQL (Postgres, MySQL) compute validFor directly in their query and do not
// use this helper.
func QuantizeHLC(now WithTimestampRevision, followerReadDelay, quantization time.Duration) (datastore.Revision, time.Duration) {
	delayedNow := now.TimestampNanoSec() - followerReadDelay.Nanoseconds()
	quantized := delayedNow
	validForNanos := int64(0)
	if quantization.Nanoseconds() > 0 {
		afterLastQuantization := delayedNow % quantization.Nanoseconds()
		quantized -= afterLastQuantization
		validForNanos = quantization.Nanoseconds() - afterLastQuantization
	}

	return now.ConstructForTimestamp(quantized), time.Duration(validForNanos) * time.Nanosecond
}

// CheckHLCGCWindow verifies that the given revision is within the datastore's
// software GC window relative to the current HLC time: not so old that it has
// (likely) been garbage collected, and not from the future.
func CheckHLCGCWindow(now, rev WithTimestampRevision, gcWindow time.Duration) error {
	nowNanos := now.TimestampNanoSec()
	revisionNanos := rev.TimestampNanoSec()

	isStale := revisionNanos < (nowNanos - gcWindow.Nanoseconds())
	if isStale {
		return datastore.NewInvalidRevisionErr(rev, datastore.RevisionStale)
	}

	isUnknown := revisionNanos > nowNanos
	if isUnknown {
		return datastore.NewInvalidRevisionErr(rev, datastore.CouldNotDetermineRevision)
	}

	return nil
}

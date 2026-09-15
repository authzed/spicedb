package revisions

import (
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
)

// Quantize rounds a timestamp-based "now" revision down to a quantization
// boundary, after subtracting the follower-read delay, and reports how long the
// result remains valid: until the next boundary. A zero quantization returns
// the delayed now unchanged with no validity.
//
// This is the in-Go quantization used by datastores whose revision is a clock
// reading (CockroachDB, Spanner, memdb). Datastores that quantize in SQL
// (Postgres, MySQL) compute validFor in their query and do not use this helper.
func Quantize(now WithTimestampRevision, followerReadDelay, quantization time.Duration) (WithTimestampRevision, time.Duration) {
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

// CheckGCWindow verifies that the given revision is within the datastore's
// software GC window relative to the current time: not so old that it has
// (likely) been garbage collected, and not from the future.
func CheckGCWindow(now, rev WithTimestampRevision, gcWindow time.Duration) error {
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

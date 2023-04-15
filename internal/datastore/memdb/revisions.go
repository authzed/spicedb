package memdb

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

func revisionFromTimestamp(t time.Time) revision.Decimal {
	return revision.NewFromDecimal(decimal.NewFromInt(t.UnixNano()))
}

func (mdb *memdbDatastore) newRevisionID() revision.Decimal {
	mdb.Lock()
	defer mdb.Unlock()

	existing := mdb.revisions[len(mdb.revisions)-1].revision
	created := revisionFromTimestamp(time.Now().UTC()).Decimal

	// NOTE: The time.Now().UTC() only appears to have *microsecond* level
	// precision on macOS Monterey in Go 1.19.1. This means that HeadRevision
	// and the result of a ReadWriteTx could return the *same* transaction ID
	// if both are executed in sequence without any other forms of delay on
	// macOS. We therefore check if the created transaction ID matches that
	// previously created and, if not, add to it.
	//
	// See: https://github.com/golang/go/issues/22037 which appeared to fix
	// this in Go 1.9.2, but there appears to have been a reversion with either
	// the new version of macOS or Go.
	if created.Equals(existing) {
		return revision.NewFromDecimal(created.Add(decimal.NewFromInt(1)))
	}
	return revision.NewFromDecimal(created)
}

func (mdb *memdbDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	return revision.NewFromDecimal(mdb.headRevisionNoLock()), nil
}

func (mdb *memdbDatastore) SquashRevisionsForTesting() {
	mdb.revisions = []snapshot{
		{
			revision: revisionFromTimestamp(time.Now().UTC()).Decimal,
			db:       mdb.db,
		},
	}
}

func (mdb *memdbDatastore) headRevisionNoLock() decimal.Decimal {
	return mdb.revisions[len(mdb.revisions)-1].revision
}

func (mdb *memdbDatastore) OptimizedRevision(_ context.Context) (datastore.Revision, error) {
	now := revisionFromTimestamp(time.Now().UTC())
	return revision.NewFromDecimal(now.Sub(now.Mod(mdb.quantizationPeriod))), nil
}

func (mdb *memdbDatastore) CheckRevision(_ context.Context, revisionRaw datastore.Revision) error {
	mdb.RLock()
	defer mdb.RUnlock()

	dr, ok := revisionRaw.(revision.Decimal)
	if !ok {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}
	return mdb.checkRevisionLocalCallerMustLock(dr)
}

func (mdb *memdbDatastore) checkRevisionLocalCallerMustLock(revisionRaw revision.Decimal) error {
	now := revisionFromTimestamp(time.Now().UTC())

	// Ensure the revision has not fallen outside of the GC window. If it has, it is considered
	// invalid.
	if mdb.revisionOutsideGCWindow(now, revisionRaw) {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.RevisionStale)
	}

	// If the revision <= now and later than the GC window, it is assumed to be valid, even if
	// HEAD revision is behind it.
	if revisionRaw.GreaterThan(now) {
		// If the revision is in the "future", then check to ensure that it is <= of HEAD to handle
		// the microsecond granularity on macos (see comment above in newRevisionID)
		headRevision := mdb.headRevisionNoLock()
		if revisionRaw.LessThanOrEqual(headRevision) {
			return nil
		}

		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (mdb *memdbDatastore) revisionOutsideGCWindow(now revision.Decimal, revisionRaw revision.Decimal) bool {
	// make an exception for head revision - it will be acceptable even if outside GC Window
	if revisionRaw.Equals(mdb.headRevisionNoLock()) {
		return false
	}
	oldest := revision.NewFromDecimal(now.Add(mdb.negativeGCWindow))
	return revisionRaw.LessThan(oldest)
}

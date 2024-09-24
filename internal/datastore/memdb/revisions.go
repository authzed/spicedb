package memdb

import (
	"context"
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

var ParseRevisionString = revisions.RevisionParser(revisions.Timestamp)

func nowRevision() revisions.TimestampRevision {
	return revisions.NewForTime(time.Now().UTC())
}

func (mdb *memdbDatastore) newRevisionID() revisions.TimestampRevision {
	mdb.Lock()
	defer mdb.Unlock()

	existing := mdb.revisions[len(mdb.revisions)-1].revision
	created := nowRevision()

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
	if created.Equal(existing) {
		return revisions.NewForTimestamp(created.TimestampNanoSec() + 1)
	}

	return created
}

func (mdb *memdbDatastore) HeadRevision(_ context.Context) (datastore.Revision, error) {
	mdb.RLock()
	defer mdb.RUnlock()
	if mdb.db == nil {
		return nil, fmt.Errorf("datastore has been closed")
	}

	return mdb.headRevisionNoLock(), nil
}

func (mdb *memdbDatastore) SquashRevisionsForTesting() {
	mdb.revisions = []snapshot{
		{
			revision: nowRevision(),
			db:       mdb.db,
		},
	}
}

func (mdb *memdbDatastore) headRevisionNoLock() revisions.TimestampRevision {
	return mdb.revisions[len(mdb.revisions)-1].revision
}

func (mdb *memdbDatastore) OptimizedRevision(_ context.Context) (datastore.Revision, error) {
	mdb.RLock()
	defer mdb.RUnlock()
	if mdb.db == nil {
		return nil, fmt.Errorf("datastore has been closed")
	}

	now := nowRevision()
	return revisions.NewForTimestamp(now.TimestampNanoSec() - now.TimestampNanoSec()%mdb.quantizationPeriod), nil
}

func (mdb *memdbDatastore) CheckRevision(_ context.Context, dr datastore.Revision) error {
	mdb.RLock()
	defer mdb.RUnlock()
	if mdb.db == nil {
		return fmt.Errorf("datastore has been closed")
	}

	return mdb.checkRevisionLocalCallerMustLock(dr)
}

func (mdb *memdbDatastore) checkRevisionLocalCallerMustLock(dr datastore.Revision) error {
	now := nowRevision()

	// Ensure the revision has not fallen outside of the GC window. If it has, it is considered
	// invalid.
	if mdb.revisionOutsideGCWindow(now, dr) {
		return datastore.NewInvalidRevisionErr(dr, datastore.RevisionStale)
	}

	// If the revision <= now and later than the GC window, it is assumed to be valid, even if
	// HEAD revision is behind it.
	if dr.GreaterThan(now) {
		// If the revision is in the "future", then check to ensure that it is <= of HEAD to handle
		// the microsecond granularity on macos (see comment above in newRevisionID)
		headRevision := mdb.headRevisionNoLock()
		if dr.LessThan(headRevision) || dr.Equal(headRevision) {
			return nil
		}

		return datastore.NewInvalidRevisionErr(dr, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (mdb *memdbDatastore) revisionOutsideGCWindow(now revisions.TimestampRevision, revisionRaw datastore.Revision) bool {
	// make an exception for head revision - it will be acceptable even if outside GC Window
	if revisionRaw.Equal(mdb.headRevisionNoLock()) {
		return false
	}

	oldest := revisions.NewForTimestamp(now.TimestampNanoSec() + mdb.negativeGCWindow)
	return revisionRaw.LessThan(oldest)
}

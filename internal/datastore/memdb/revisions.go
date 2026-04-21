package memdb

import (
	"context"
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

func (mdb *memdbDatastore) HeadRevision(_ context.Context) (datastore.RevisionWithSchemaHash, error) {
	mdb.RLock()
	defer mdb.RUnlock()
	if err := mdb.checkNotClosed(); err != nil {
		return datastore.RevisionWithSchemaHash{}, err
	}

	rev, hash := mdb.headRevisionWithHashNoLock()
	return datastore.RevisionWithSchemaHash{Revision: rev, SchemaHash: hash}, nil
}

func (mdb *memdbDatastore) SquashRevisionsForTesting() {
	mdb.revisions = []snapshot{
		{
			revision:   nowRevision(),
			schemaHash: "",
			db:         mdb.db,
		},
	}
}

func (mdb *memdbDatastore) headRevisionWithHashNoLock() (revisions.TimestampRevision, string) {
	head := mdb.revisions[len(mdb.revisions)-1]
	return head.revision, head.schemaHash
}

func (mdb *memdbDatastore) headRevisionNoLock() revisions.TimestampRevision {
	return mdb.revisions[len(mdb.revisions)-1].revision
}

func (mdb *memdbDatastore) OptimizedRevision(_ context.Context) (datastore.RevisionWithSchemaHash, error) {
	mdb.RLock()
	defer mdb.RUnlock()
	if err := mdb.checkNotClosed(); err != nil {
		return datastore.RevisionWithSchemaHash{}, err
	}

	now := nowRevision()
	var optimized revisions.TimestampRevision
	if mdb.quantizationPeriod > 0 {
		optimized = revisions.NewForTimestamp(now.TimestampNanoSec() - now.TimestampNanoSec()%mdb.quantizationPeriod)
	} else {
		optimized = now
	}

	// Find the schema hash visible at the optimized revision: walk the
	// revisions list backward for the most recent snapshot whose revision
	// is at or before `optimized`.
	var hash string
	for i := len(mdb.revisions) - 1; i >= 0; i-- {
		if !mdb.revisions[i].revision.GreaterThan(optimized) {
			hash = mdb.revisions[i].schemaHash
			break
		}
	}

	return datastore.RevisionWithSchemaHash{Revision: optimized, SchemaHash: hash}, nil
}

func (mdb *memdbDatastore) CheckRevision(_ context.Context, dr datastore.Revision) error {
	mdb.RLock()
	defer mdb.RUnlock()
	if err := mdb.checkNotClosed(); err != nil {
		return err
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

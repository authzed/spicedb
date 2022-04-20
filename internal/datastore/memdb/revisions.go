package memdb

import (
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errRevision      = "unable to find revision: %w"
	errCheckRevision = "unable to check revision: %w"
)

func (mds *memdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	// Compute the current revision
	txn := db.Txn(false)
	defer txn.Abort()

	lastRaw, err := txn.Last(tableTransaction, indexID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}
	if lastRaw != nil {
		return revisionFromVersion(lastRaw.(*transaction).id), nil
	}
	return datastore.NoRevision, nil
}

func (mds *memdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	rounded := uint64(time.Now().Round(mds.revisionQuantization).UnixNano())

	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableTransaction, indexTimestamp, rounded)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errRevision, err)
	}

	selected := iter.Next()
	if selected == nil {
		return mds.HeadRevision(ctx)
	}

	return revisionFromVersion(selected.(*transaction).id), nil
}

func (mds *memdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	// We need to know the highest possible revision
	time.Sleep(mds.simulatedLatency)
	lastRaw, err := txn.Last(tableTransaction, indexID)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}
	if lastRaw == nil {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	highest := revisionFromVersion(lastRaw.(*transaction).id)

	if revision.GreaterThan(highest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	lowerBound := uint64(time.Now().Add(mds.gcWindowInverted).UnixNano())
	time.Sleep(mds.simulatedLatency)
	iter, err := txn.LowerBound(tableTransaction, indexTimestamp, lowerBound)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	firstValid := iter.Next()
	if firstValid == nil && !revision.Equal(highest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	if firstValid != nil && revision.LessThan(revisionFromVersion(firstValid.(*transaction).id)) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

func revisionFromVersion(version uint64) datastore.Revision {
	return decimal.NewFromInt(int64(version))
}

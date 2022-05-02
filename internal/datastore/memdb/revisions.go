package memdb

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

func revisionFromTimestamp(t time.Time) datastore.Revision {
	return decimal.NewFromInt(t.UnixNano())
}

func (mdb *memdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	head, err := mdb.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	return head.Sub(head.Mod(mdb.quantizationPeriod)), nil
}

func (mdb *memdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return revisionFromTimestamp(time.Now().UTC()), nil
}

func (mdb *memdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return mdb.checkRevisionLocal(revision)
}

func (mdb *memdbDatastore) checkRevisionLocal(revision datastore.Revision) error {
	now := revisionFromTimestamp(time.Now().UTC())

	if revision.GreaterThan(now) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionInFuture)
	}

	oldest := now.Add(mdb.negativeGCWindow)
	if revision.LessThan(oldest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

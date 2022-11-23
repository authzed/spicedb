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

func (mdb *memdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	head, err := mdb.headRevision(ctx)
	if err != nil {
		return nil, err
	}

	return revision.NewFromDecimal(head), nil
}

func (mdb *memdbDatastore) headRevision(ctx context.Context) (decimal.Decimal, error) {
	mdb.Lock()
	defer mdb.Unlock()

	return mdb.revisions[len(mdb.revisions)-1].revision, nil
}

func (mdb *memdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	now := revisionFromTimestamp(time.Now().UTC())
	return revision.NewFromDecimal(now.Sub(now.Mod(mdb.quantizationPeriod))), nil
}

func (mdb *memdbDatastore) CheckRevision(ctx context.Context, revisionRaw datastore.Revision) error {
	dr, ok := revisionRaw.(revision.Decimal)
	if !ok {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}
	return mdb.checkRevisionLocal(dr)
}

func (mdb *memdbDatastore) checkRevisionLocal(revisionRaw revision.Decimal) error {
	now := revisionFromTimestamp(time.Now().UTC())

	if revisionRaw.GreaterThan(now) {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.CouldNotDetermineRevision)
	}

	oldest := revision.NewFromDecimal(now.Add(mdb.negativeGCWindow))
	if revisionRaw.LessThan(oldest) {
		return datastore.NewInvalidRevisionErr(revisionRaw, datastore.RevisionStale)
	}

	return nil
}

package memdb

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

// NOTE: The time.Now().UTC() only appears to have *microsecond* level
// precision on macOS Monterey in Go 1.19.1. This means that HeadRevision
// and the result of a ReadWriteTx can return the *same* transaction ID
// if both are executed in sequence without any other forms of delay on
// macOS. As memdb should only be used for testing and tooling this *should*
// be okay, but we should investigate if there is a further fix.
//
// See: https://github.com/golang/go/issues/22037 which appeared to fix
// this in Go 1.9.2, but there appears to have been a reversion with either
// the new version of macOS or Go.
//
// TODO(jschorr): Investigate if there is a way to fix this.
func newRevisionID() revision.Decimal {
	return revisionFromTimestamp(time.Now().UTC())
}

func revisionFromTimestamp(t time.Time) revision.Decimal {
	return revision.NewFromDecimal(decimal.NewFromInt(t.UnixNano()))
}

func (mdb *memdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	head := revisionFromTimestamp(time.Now().UTC())
	return revision.NewFromDecimal(head.Sub(head.Mod(mdb.quantizationPeriod))), nil
}

func (mdb *memdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return revisionFromTimestamp(time.Now().UTC()), nil
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

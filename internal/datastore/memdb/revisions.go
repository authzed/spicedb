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

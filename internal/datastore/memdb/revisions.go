package memdb

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func revisionFromTimestamp(t time.Time) revisions.DecimalRevision {
	return revisions.NewFromDecimal(decimal.NewFromInt(t.UnixNano()))
}

func (mdb *memdbDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, error) {
	head := revisionFromTimestamp(time.Now().UTC())
	return revisions.NewFromDecimal(head.Sub(head.Mod(mdb.quantizationPeriod))), nil
}

func (mdb *memdbDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	return revisionFromTimestamp(time.Now().UTC()), nil
}

func (mdb *memdbDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	dr, ok := revision.(revisions.DecimalRevision)
	if !ok {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}
	return mdb.checkRevisionLocal(dr)
}

func (mdb *memdbDatastore) checkRevisionLocal(revision revisions.DecimalRevision) error {
	now := revisionFromTimestamp(time.Now().UTC())

	if revision.GreaterThan(now) {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	oldest := revisions.NewFromDecimal(now.Add(mdb.negativeGCWindow))
	if revision.LessThan(oldest) {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

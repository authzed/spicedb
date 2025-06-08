package tidb

import (
	"context"
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var (
	_ common.GarbageCollectableDatastore = (*Datastore)(nil)
	_ common.GarbageCollector            = (*tidbGarbageCollector)(nil)
)

type tidbGarbageCollector struct {
	ds       *Datastore
	isClosed bool
}

func (ds *Datastore) BuildGarbageCollector(ctx context.Context) (common.GarbageCollector, error) {
	return &tidbGarbageCollector{ds: ds, isClosed: false}, nil
}

func (ds *Datastore) HasGCRun() bool {
	return ds.gcHasRun.Load()
}

func (ds *Datastore) MarkGCCompleted() {
	ds.gcHasRun.Store(true)
}

func (ds *Datastore) ResetGCCompleted() {
	ds.gcHasRun.Store(false)
}

func (tgc *tidbGarbageCollector) Close() {
	tgc.isClosed = true
}

func (tgc *tidbGarbageCollector) LockForGCRun(ctx context.Context) (bool, error) {
	return tgc.ds.tryAcquireLock(ctx, gcRunLock)
}

func (tgc *tidbGarbageCollector) UnlockAfterGCRun() error {
	return tgc.ds.releaseLock(context.Background(), gcRunLock)
}

func (tgc *tidbGarbageCollector) Now(ctx context.Context) (time.Time, error) {
	if tgc.isClosed {
		return time.Time{}, spiceerrors.MustBugf("tidbGarbageCollector is closed")
	}

	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var now time.Time
	err = tgc.ds.db.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}

	// This conversion should just be for convenience while debugging --
	// TiDB and the driver do properly timezones properly.
	return now.UTC(), nil
}

// - main difference is how the PSQL driver handles null values
func (tgc *tidbGarbageCollector) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	if tgc.isClosed {
		return datastore.NoRevision, spiceerrors.MustBugf("tidbGarbageCollector is closed")
	}

	// Find the highest transaction ID before the GC window.
	query, args, err := tgc.ds.GetLastRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	var value sql.NullInt64
	err = tgc.ds.db.QueryRowContext(ctx, query, args...).Scan(&value)
	if err != nil {
		return datastore.NoRevision, err
	}

	if !value.Valid {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return datastore.NoRevision, nil
	}

	uintValue, err := safecast.ToUint64(value.Int64)
	if err != nil {
		return datastore.NoRevision, spiceerrors.MustBugf("value could not be cast to uint64: %v", err)
	}

	return revisions.NewForTransactionID(uintValue), nil
}

// - implementation misses metrics
func (tgc *tidbGarbageCollector) DeleteBeforeTx(
	ctx context.Context,
	txID datastore.Revision,
) (removed common.DeletionCounts, err error) {
	if tgc.isClosed {
		return removed, spiceerrors.MustBugf("tidbGarbageCollector is closed")
	}

	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	removed.Relationships, err = tgc.batchDelete(ctx, tgc.ds.driver.RelationTuple(), sq.LtOrEq{colDeletedTxn: txID})
	if err != nil {
		return
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = tgc.batchDelete(ctx, tgc.ds.driver.RelationTupleTransaction(), sq.Lt{colID: txID})
	if err != nil {
		return
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = tgc.batchDelete(ctx, tgc.ds.driver.Namespace(), sq.LtOrEq{colDeletedTxn: txID})
	return
}

func (tgc *tidbGarbageCollector) DeleteExpiredRels(ctx context.Context) (int64, error) {
	if tgc.ds.schema.ExpirationDisabled {
		return 0, nil
	}

	now, err := tgc.Now(ctx)
	if err != nil {
		return 0, err
	}

	return tgc.batchDelete(
		ctx,
		tgc.ds.driver.RelationTuple(),
		sq.Lt{colExpiration: now.Add(-1 * tgc.ds.gcWindow)},
	)
}

// - query was reworked to make it compatible with Vitess
// - API differences with PSQL driver
func (tgc *tidbGarbageCollector) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	if tgc.isClosed {
		return -1, spiceerrors.MustBugf("tidbGarbageCollector is closed")
	}

	query, args, err := sb.Delete(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	var deletedCount int64
	for {
		cr, err := tgc.ds.db.ExecContext(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted, err := cr.RowsAffected()
		if err != nil {
			return deletedCount, err
		}
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

package mysql

import (
	"context"
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var (
	_ common.GarbageCollectableDatastore = (*Datastore)(nil)
	_ common.GarbageCollector            = (*mysqlGarbageCollector)(nil)
)

type mysqlGarbageCollector struct {
	mds      *Datastore
	isClosed bool
}

func (mds *Datastore) BuildGarbageCollector(ctx context.Context) (common.GarbageCollector, error) {
	return &mysqlGarbageCollector{mds: mds, isClosed: false}, nil
}

func (mds *Datastore) HasGCRun() bool {
	return mds.gcHasRun.Load()
}

func (mds *Datastore) MarkGCCompleted() {
	mds.gcHasRun.Store(true)
}

func (mds *Datastore) ResetGCCompleted() {
	mds.gcHasRun.Store(false)
}

func (mcc *mysqlGarbageCollector) Close() {
	mcc.isClosed = true
}

func (mcc *mysqlGarbageCollector) LockForGCRun(ctx context.Context) (bool, error) {
	return mcc.mds.tryAcquireLock(ctx, gcRunLock)
}

func (mcc *mysqlGarbageCollector) UnlockAfterGCRun() error {
	return mcc.mds.releaseLock(context.Background(), gcRunLock)
}

func (mcc *mysqlGarbageCollector) Now(ctx context.Context) (time.Time, error) {
	if mcc.isClosed {
		return time.Time{}, spiceerrors.MustBugf("mysqlGarbageCollector is closed")
	}

	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var now time.Time
	err = mcc.mds.db.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}

	// This conversion should just be for convenience while debugging --
	// MySQL and the driver do properly timezones properly.
	return now.UTC(), nil
}

// - main difference is how the PSQL driver handles null values
func (mcc *mysqlGarbageCollector) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	if mcc.isClosed {
		return datastore.NoRevision, spiceerrors.MustBugf("mysqlGarbageCollector is closed")
	}

	// Find the highest transaction ID before the GC window.
	query, args, err := mcc.mds.GetLastRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	var value sql.NullInt64
	err = mcc.mds.db.QueryRowContext(ctx, query, args...).Scan(&value)
	if err != nil {
		return datastore.NoRevision, err
	}

	if !value.Valid {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return datastore.NoRevision, nil
	}

	uintValue, err := safecast.Convert[uint64](value.Int64)
	if err != nil {
		return datastore.NoRevision, spiceerrors.MustBugf("value could not be cast to uint64: %v", err)
	}

	return revisions.NewForTransactionID(uintValue), nil
}

// - implementation misses metrics
func (mcc *mysqlGarbageCollector) DeleteBeforeTx(
	ctx context.Context,
	txID datastore.Revision,
) (removed common.DeletionCounts, err error) {
	if mcc.isClosed {
		return removed, spiceerrors.MustBugf("mysqlGarbageCollector is closed")
	}

	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	removed.Relationships, err = mcc.batchDelete(ctx, mcc.mds.driver.RelationTuple(), sq.LtOrEq{colDeletedTxn: txID})
	if err != nil {
		return removed, err
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = mcc.batchDelete(ctx, mcc.mds.driver.RelationTupleTransaction(), sq.Lt{colID: txID})
	if err != nil {
		return removed, err
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = mcc.batchDelete(ctx, mcc.mds.driver.Namespace(), sq.LtOrEq{colDeletedTxn: txID})
	return removed, err
}

func (mcc *mysqlGarbageCollector) DeleteExpiredRels(ctx context.Context) (int64, error) {
	if mcc.mds.schema.ExpirationDisabled {
		return 0, nil
	}

	now, err := mcc.Now(ctx)
	if err != nil {
		return 0, err
	}

	return mcc.batchDelete(
		ctx,
		mcc.mds.driver.RelationTuple(),
		sq.Lt{colExpiration: now.Add(-1 * mcc.mds.gcWindow)},
	)
}

// - query was reworked to make it compatible with Vitess
// - API differences with PSQL driver
func (mcc *mysqlGarbageCollector) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	if mcc.isClosed {
		return -1, spiceerrors.MustBugf("mysqlGarbageCollector is closed")
	}

	query, args, err := sb.Delete(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	var deletedCount int64
	for {
		cr, err := mcc.mds.db.ExecContext(ctx, query, args...)
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

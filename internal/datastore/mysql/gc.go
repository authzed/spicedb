package mysql

import (
	"context"
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

var _ common.GarbageCollector = (*Datastore)(nil)

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) Now(ctx context.Context) (time.Time, error) {
	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var now time.Time
	err = mds.db.QueryRowContext(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}

	// This conversion should just be for convenience while debugging --
	// MySQL and the driver do properly timezones properly.
	return now.UTC(), nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - main difference is how the PSQL driver handles null values
func (mds *Datastore) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	// Find the highest transaction ID before the GC window.
	query, args, err := mds.GetLastRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	var value sql.NullInt64
	err = mds.db.QueryRowContext(ctx, query, args...).Scan(&value)
	if err != nil {
		return datastore.NoRevision, err
	}

	if !value.Valid {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return datastore.NoRevision, nil
	}
	return revision.NewFromDecimal(decimal.NewFromInt(value.Int64)), nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - implementation misses metrics
func (mds *Datastore) DeleteBeforeTx(
	ctx context.Context,
	txID datastore.Revision,
) (removed common.DeletionCounts, err error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	removed.Relationships, err = mds.batchDelete(ctx, mds.driver.RelationTuple(), sq.LtOrEq{colDeletedTxn: txID})
	if err != nil {
		return
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = mds.batchDelete(ctx, mds.driver.RelationTupleTransaction(), sq.Lt{colID: txID})
	if err != nil {
		return
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = mds.batchDelete(ctx, mds.driver.Namespace(), sq.LtOrEq{colDeletedTxn: txID})
	return
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
// - query was reworked to make it compatible with Vitess
// - API differences with PSQL driver
func (mds *Datastore) batchDelete(ctx context.Context, tableName string, filter sqlFilter) (int64, error) {
	query, args, err := sb.Delete(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	var deletedCount int64
	for {
		cr, err := mds.db.ExecContext(ctx, query, args...)
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

package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var (
	_ common.GarbageCollectableDatastore = (*pgDatastore)(nil)
	_ common.GarbageCollector            = (*pgGarbageCollector)(nil)

	// we are using "tableoid" to globally identify the row through the "ctid" in partitioned environments
	// as it's not guaranteed 2 rows in different partitions have different "ctid" values
	// See https://www.postgresql.org/docs/current/ddl-system-columns.html#DDL-SYSTEM-COLUMNS-TABLEOID
	gcPKCols = []string{"tableoid", "ctid"}
)

type pgGarbageCollector struct {
	conn     *pgxpool.Conn
	pgd      *pgDatastore
	isClosed bool
}

func (pgd *pgDatastore) BuildGarbageCollector(ctx context.Context) (common.GarbageCollector, error) {
	conn, err := pgd.writePool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return &pgGarbageCollector{conn: conn, pgd: pgd, isClosed: false}, nil
}

func (pgd *pgDatastore) HasGCRun() bool {
	return pgd.gcHasRun.Load()
}

func (pgd *pgDatastore) MarkGCCompleted() {
	pgd.gcHasRun.Store(true)
}

func (pgd *pgDatastore) ResetGCCompleted() {
	pgd.gcHasRun.Store(false)
}

func (pgg *pgGarbageCollector) Close() {
	pgg.isClosed = true
	pgg.conn.Release()
}

func (pgg *pgGarbageCollector) LockForGCRun(ctx context.Context) (bool, error) {
	return pgg.pgd.tryAcquireLock(ctx, pgg.conn, gcRunLock)
}

func (pgg *pgGarbageCollector) UnlockAfterGCRun() error {
	return pgg.pgd.releaseLock(context.Background(), pgg.conn, gcRunLock)
}

func (pgg *pgGarbageCollector) Now(ctx context.Context) (time.Time, error) {
	if pgg.isClosed {
		return time.Time{}, spiceerrors.MustBugf("pgGarbageCollector is closed")
	}

	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var now time.Time
	err = pgg.conn.QueryRow(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}

	// RelationTupleTransaction is not timezone aware -- explicitly use UTC
	// before using as a query arg.
	return now.UTC(), nil
}

func (pgg *pgGarbageCollector) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	if pgg.isClosed {
		return datastore.NoRevision, spiceerrors.MustBugf("pgGarbageCollector is closed")
	}

	// Find any transaction ID before the GC window.
	sql, args, err := getRevisionForGC.Where(sq.Lt{schema.ColTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	// Force the proper index for the GC operation.
	sql = "/*+ IndexOnlyScan(" + schema.TableTransaction + " " + schema.IndexSortedRelationTupleTransaction.Name + ") */" + sql

	var value xid8
	var snapshot pgSnapshot
	err = pgg.conn.QueryRow(ctx, sql, args...).Scan(&value, &snapshot)
	if err != nil {
		return datastore.NoRevision, err
	}

	return postgresRevision{snapshot: snapshot, optionalTxID: value}, nil
}

func (pgg *pgGarbageCollector) DeleteExpiredRels(ctx context.Context) (int64, error) {
	if pgg.isClosed {
		return 0, spiceerrors.MustBugf("pgGarbageCollector is closed")
	}

	if pgg.pgd.schema.ExpirationDisabled {
		return 0, nil
	}

	now, err := pgg.Now(ctx)
	if err != nil {
		return 0, err
	}

	return pgg.batchDelete(
		ctx,
		pgg.conn,
		schema.TableTuple,
		gcPKCols,
		sq.Lt{schema.ColExpiration: now.Add(-1 * pgg.pgd.gcWindow)},
		&schema.IndexExpiringRelationships,
	)
}

type exec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (pgg *pgGarbageCollector) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (common.DeletionCounts, error) {
	if pgg.isClosed {
		return common.DeletionCounts{}, spiceerrors.MustBugf("pgGarbageCollector is closed")
	}

	return pgg.deleteBeforeTx(ctx, pgg.conn, txID)
}

func (pgg *pgGarbageCollector) deleteBeforeTx(ctx context.Context, conn exec, txID datastore.Revision) (common.DeletionCounts, error) {
	revision := txID.(postgresRevision)

	minTxAlive := NewXid8(revision.snapshot.xmin)
	removed := common.DeletionCounts{}
	var err error
	// Delete any relationship rows that were already dead when this transaction started
	removed.Relationships, err = pgg.batchDelete(
		ctx,
		conn,
		schema.TableTuple,
		gcPKCols,
		sq.Lt{schema.ColDeletedXid: minTxAlive},
		&schema.IndexGCDeadRelationships,
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC relationships table: %w", err)
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = pgg.batchDelete(
		ctx,
		conn,
		schema.TableTransaction,
		gcPKCols,
		sq.Lt{schema.ColXID: minTxAlive},
		nil,
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC transactions table: %w", err)
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = pgg.batchDelete(
		ctx,
		conn,
		schema.TableNamespace,
		gcPKCols,
		sq.Lt{schema.ColDeletedXid: minTxAlive},
		nil,
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC namespaces table: %w", err)
	}

	return removed, err
}

func (pgg *pgGarbageCollector) batchDelete(
	ctx context.Context,
	conn exec,
	tableName string,
	pkCols []string,
	filter sqlFilter,
	index *common.IndexDefinition,
) (int64, error) {
	sql, args, err := psql.Select(pkCols...).From(tableName).Where(filter).Limit(gcBatchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}
	if index != nil {
		// Force the proper index for the GC operation.
		sql = "/*+ IndexOnlyScan(" + tableName + " " + index.Name + ") */" + sql
	}

	pkColsExpression := strings.Join(pkCols, ", ")
	query := fmt.Sprintf(`WITH rows AS (%[1]s)
		  DELETE FROM %[2]s
		  WHERE (%[3]s) IN (SELECT %[3]s FROM rows);
	`, sql, tableName, pkColsExpression)

	var deletedCount int64
	for {
		cr, err := conn.Exec(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted := cr.RowsAffected()
		deletedCount += rowsDeleted
		if rowsDeleted < gcBatchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

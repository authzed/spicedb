package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	_ common.GarbageCollector = (*pgDatastore)(nil)

	// we are using "tableoid" to globally identify the row through the "ctid" in partitioned environments
	// as it's not guaranteed 2 rows in different partitions have different "ctid" values
	// See https://www.postgresql.org/docs/current/ddl-system-columns.html#DDL-SYSTEM-COLUMNS-TABLEOID
	gcPKCols = []string{"tableoid", "ctid"}
)

func (pgd *pgDatastore) LockForGCRun(ctx context.Context) (bool, error) {
	return pgd.tryAcquireLock(ctx, gcRunLock)
}

func (pgd *pgDatastore) UnlockAfterGCRun() error {
	return pgd.releaseLock(context.Background(), gcRunLock)
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

func (pgd *pgDatastore) Now(ctx context.Context) (time.Time, error) {
	// Retrieve the `now` time from the database.
	nowSQL, nowArgs, err := getNow.ToSql()
	if err != nil {
		return time.Time{}, err
	}

	var now time.Time
	err = pgd.readPool.QueryRow(ctx, nowSQL, nowArgs...).Scan(&now)
	if err != nil {
		return time.Time{}, err
	}

	// RelationTupleTransaction is not timezone aware -- explicitly use UTC
	// before using as a query arg.
	return now.UTC(), nil
}

func (pgd *pgDatastore) TxIDBefore(ctx context.Context, before time.Time) (datastore.Revision, error) {
	// Find the highest transaction ID before the GC window.
	sql, args, err := getRevision.Where(sq.Lt{schema.ColTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	var value xid8
	var snapshot pgSnapshot
	err = pgd.readPool.QueryRow(ctx, sql, args...).Scan(&value, &snapshot)
	if err != nil {
		return datastore.NoRevision, err
	}

	return postgresRevision{snapshot: snapshot, optionalTxID: value}, nil
}

func (pgd *pgDatastore) DeleteExpiredRels(ctx context.Context) (int64, error) {
	if pgd.schema.ExpirationDisabled {
		return 0, nil
	}

	now, err := pgd.Now(ctx)
	if err != nil {
		return 0, err
	}

	return pgd.batchDelete(
		ctx,
		schema.TableTuple,
		gcPKCols,
		sq.Lt{schema.ColExpiration: now.Add(-1 * pgd.gcWindow)},
	)
}

func (pgd *pgDatastore) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (common.DeletionCounts, error) {
	revision := txID.(postgresRevision)

	minTxAlive := newXid8(revision.snapshot.xmin)
	removed := common.DeletionCounts{}
	var err error
	// Delete any relationship rows that were already dead when this transaction started
	removed.Relationships, err = pgd.batchDelete(
		ctx,
		schema.TableTuple,
		gcPKCols,
		sq.Lt{schema.ColDeletedXid: minTxAlive},
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC relationships table: %w", err)
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = pgd.batchDelete(
		ctx,
		schema.TableTransaction,
		gcPKCols,
		sq.Lt{schema.ColXID: minTxAlive},
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC transactions table: %w", err)
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = pgd.batchDelete(
		ctx,
		schema.TableNamespace,
		gcPKCols,
		sq.Lt{schema.ColDeletedXid: minTxAlive},
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC namespaces table: %w", err)
	}
	// Delete objects that are no longer referenced by any active relationships
	// We only delete objects where:
	// 1. The object itself was marked as deleted (deleted_xid < minTxAlive)
	// 2. There are no active relationships (deleted_xid = liveDeletedTxnID) referencing this object
	//    either as a resource or as a subject
	deleteUnreferencedObjects := fmt.Sprintf(
		`WITH rows AS (
			SELECT tableoid, ctid 
			FROM %s op
			WHERE NOT EXISTS (
				SELECT 1 
				FROM %s rt
				WHERE rt.%s = '%d'::xid8
				AND (
					(rt.%s = op.%s AND rt.%s = op.%s) 
					OR 
					(rt.%s = op.%s AND rt.%s = op.%s)
				)
			)
			LIMIT %d
		)
		DELETE FROM %s
		WHERE (tableoid, ctid) IN (SELECT tableoid, ctid FROM rows)
		RETURNING 1`,
		schema.TableObjectData,
		schema.TableTuple,
		schema.ColDeletedXid,
		liveDeletedTxnID,
		schema.ColNamespace,
		schema.ColOdType,
		schema.ColObjectID,
		schema.ColOdID,
		schema.ColUsersetNamespace,
		schema.ColOdType,
		schema.ColUsersetObjectID,
		schema.ColOdID,
		gcBatchDeleteSize,
		schema.TableObjectData,
	)

	var objectsRemoved int64
	for {
		rows, err := pgd.writePool.Query(ctx, deleteUnreferencedObjects)
		if err != nil {
			return removed, fmt.Errorf("failed to GC %s table: %w", schema.TableObjectData, err)
		}
		defer rows.Close()

		var rowsDeleted int64
		for rows.Next() {
			rowsDeleted++
		}
		objectsRemoved += rowsDeleted

		if rowsDeleted < gcBatchDeleteSize {
			break
		}
	}

	removed.Objects = objectsRemoved
	return removed, err
}

func (pgd *pgDatastore) batchDelete(
	ctx context.Context,
	tableName string,
	pkCols []string,
	filter sqlFilter,
) (int64, error) {
	sql, args, err := psql.Select(pkCols...).From(tableName).Where(filter).Limit(gcBatchDeleteSize).ToSql()
	if err != nil {
		return -1, err
	}

	pkColsExpression := strings.Join(pkCols, ", ")
	query := fmt.Sprintf(`WITH rows AS (%[1]s)
		  DELETE FROM %[2]s
		  WHERE (%[3]s) IN (SELECT %[3]s FROM rows);
	`, sql, tableName, pkColsExpression)

	var deletedCount int64
	for {
		cr, err := pgd.writePool.Exec(ctx, query, args...)
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

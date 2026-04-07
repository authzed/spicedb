package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var (
	_ datastore.GarbageCollectableDatastore = (*pgDatastore)(nil)
	_ datastore.GarbageCollector            = (*pgGarbageCollector)(nil)

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

func (pgd *pgDatastore) BuildGarbageCollector(ctx context.Context) (datastore.GarbageCollector, error) {
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

func (pgg *pgGarbageCollector) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (datastore.DeletionCounts, error) {
	if pgg.isClosed {
		return datastore.DeletionCounts{}, spiceerrors.MustBugf("pgGarbageCollector is closed")
	}

	return pgg.deleteBeforeTx(ctx, pgg.conn, txID)
}

// txBeginner is satisfied by *pgxpool.Conn (used in production) and by any
// ConnPooler (used in tests to route batch transactions through the
// QueryInterceptor).
type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func (pgg *pgGarbageCollector) deleteBeforeTx(ctx context.Context, beginner txBeginner, txID datastore.Revision) (datastore.DeletionCounts, error) {
	revision := txID.(postgresRevision)

	minTxAlive := NewXid8(revision.snapshot.xmin)
	removed := datastore.DeletionCounts{}
	var err error
	removed.Relationships, err = pgg.batchDelete(
		ctx,
		beginner,
		schema.TableTuple,
		gcPKCols,
		sq.Lt{schema.ColDeletedXid: minTxAlive},
		&schema.IndexGCDeadRelationships,
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC relationships table: %w", err)
	}

	removed.Transactions, err = pgg.batchDelete(
		ctx,
		beginner,
		schema.TableTransaction,
		gcPKCols,
		sq.Lt{schema.ColXID: minTxAlive},
		nil,
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC transactions table: %w", err)
	}

	removed.Namespaces, err = pgg.batchDelete(
		ctx,
		beginner,
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
	beginner txBeginner,
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
		sql = "/*+ IndexOnlyScan(" + tableName + " " + index.Name + ") */" + sql
	}

	pkColsExpression := strings.Join(pkCols, ", ")
	query := fmt.Sprintf(`WITH rows AS (%[1]s)
		  DELETE FROM %[2]s
		  WHERE (%[3]s) IN (SELECT %[3]s FROM rows);
	`, sql, tableName, pkColsExpression)

	var deletedCount int64
	for {
		tx, err := beginner.Begin(ctx)
		if err != nil {
			return deletedCount, fmt.Errorf("error starting transaction for gc batch: %w", err)
		}

		locked, err := pgg.pgd.tryAcquireXactLock(ctx, tx, gcRunLock)
		if err != nil {
			_ = tx.Rollback(ctx)
			return deletedCount, fmt.Errorf("error acquiring gc lock: %w", err)
		}

		if !locked {
			_ = tx.Rollback(ctx)
			return deletedCount, datastore.ErrGCPreempted
		}

		cr, err := tx.Exec(ctx, query, args...)
		if err != nil {
			_ = tx.Rollback(ctx)
			return deletedCount, err
		}

		if err := tx.Commit(ctx); err != nil {
			return deletedCount, fmt.Errorf("error committing gc batch: %w", err)
		}

		rowsDeleted := cr.RowsAffected()
		deletedCount += rowsDeleted
		if rowsDeleted < gcBatchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

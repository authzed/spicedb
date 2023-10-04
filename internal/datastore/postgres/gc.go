package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

var (
	_ common.GarbageCollector = (*pgDatastore)(nil)

	relationTuplePKCols = []string{
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedXid,
		colDeletedXid,
	}

	namespacePKCols = []string{colNamespace, colCreatedXid, colDeletedXid}

	transactionPKCols = []string{colXID}
)

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
	sql, args, err := getRevision.Where(sq.Lt{colTimestamp: before}).ToSql()
	if err != nil {
		return datastore.NoRevision, err
	}

	var value xid8
	var snapshot pgSnapshot
	err = pgd.readPool.QueryRow(ctx, sql, args...).Scan(&value, &snapshot)
	if err != nil {
		return datastore.NoRevision, err
	}

	return postgresRevision{snapshot}, nil
}

func (pgd *pgDatastore) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (common.DeletionCounts, error) {
	revision := txID.(postgresRevision)

	minTxAlive := newXid8(revision.snapshot.xmin)
	removed := common.DeletionCounts{}
	var err error
	// Delete any relationship rows that were already dead when this transaction started
	removed.Relationships, err = pgd.batchDelete(
		ctx,
		tableTuple,
		relationTuplePKCols,
		sq.Lt{colDeletedXid: minTxAlive},
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
		tableTransaction,
		transactionPKCols,
		sq.Lt{colXID: minTxAlive},
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC transactions table: %w", err)
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = pgd.batchDelete(
		ctx,
		tableNamespace,
		namespacePKCols,
		sq.Lt{colDeletedXid: minTxAlive},
	)
	if err != nil {
		return removed, fmt.Errorf("failed to GC namespaces table: %w", err)
	}

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

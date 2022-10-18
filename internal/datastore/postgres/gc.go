package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgtype"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	log "github.com/authzed/spicedb/pkg/logging"
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
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), nowSQL, nowArgs...).Scan(&now)
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

	value := xid8{}
	err = pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), sql, args...,
	).Scan(&value)
	if err != nil {
		return datastore.NoRevision, err
	}

	if value.Status != pgtype.Present {
		log.Ctx(ctx).Debug().Time("before", before).Msg("no stale transactions found in the datastore")
		return datastore.NoRevision, err
	}

	return revisionFromTransaction(value), nil
}

func (pgd *pgDatastore) DeleteBeforeTx(ctx context.Context, txID datastore.Revision) (removed common.DeletionCounts, err error) {
	// Delete any relationship rows with deleted_transaction <= the transaction ID.
	removed.Relationships, err = pgd.batchDelete(
		ctx,
		tableTuple,
		relationTuplePKCols,
		sq.LtOrEq{colDeletedXid: transactionFromRevision(txID)},
	)
	if err != nil {
		return
	}

	// Delete all transaction rows with ID < the transaction ID.
	//
	// We don't delete the transaction itself to ensure there is always at least
	// one transaction present.
	removed.Transactions, err = pgd.batchDelete(
		ctx,
		tableTransaction,
		transactionPKCols,
		sq.Lt{colXID: txID},
	)
	if err != nil {
		return
	}

	// Delete any namespace rows with deleted_transaction <= the transaction ID.
	removed.Namespaces, err = pgd.batchDelete(
		ctx,
		tableNamespace,
		namespacePKCols,
		sq.LtOrEq{colDeletedXid: txID},
	)
	if err != nil {
		return
	}

	return
}

func (pgd *pgDatastore) batchDelete(
	ctx context.Context,
	tableName string,
	pkCols []string,
	filter sqlFilter,
) (int64, error) {
	sql, args, err := psql.Select(pkCols...).From(tableName).Where(filter).Limit(batchDeleteSize).ToSql()
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
		cr, err := pgd.dbpool.Exec(ctx, query, args...)
		if err != nil {
			return deletedCount, err
		}

		rowsDeleted := cr.RowsAffected()
		deletedCount += rowsDeleted
		if rowsDeleted < batchDeleteSize {
			break
		}
	}

	return deletedCount, nil
}

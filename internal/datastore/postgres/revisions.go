package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	errRevision      = "unable to find revision: %w"
	errCheckRevision = "unable to check revision: %w"

	// querySelectRevision will round the database's timestamp down to the nearest
	// quantization period, and then find the first transaction after that. If there
	// are no transactions newer than the quantization period, it just picks the latest
	// transaction. It will also return the amount of nanoseconds until the next
	// optimized revision would be selected server-side, for use with caching.
	//
	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Quantization period (in nanoseconds)
	querySelectRevision = `
	SELECT COALESCE(
		(SELECT MIN(%[1]s::text::bigint) FROM %[2]s WHERE %[3]s >= TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 / %[4]d) * %[4]d / 1000000000) AT TIME ZONE 'utc'),
		(SELECT MAX(%[1]s::text::bigint) FROM %[2]s)
	),
	%[4]d - CAST(EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000 as bigint) %% %[4]d;`

	// queryValidTransaction will return a single row with two values, one boolean
	// for whether the specified transaction ID is newer than the garbage collection
	// window, and one boolean for whether the transaction ID represents a transaction
	// that will occur in the future.
	//
	//   %[1] Name of xid column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Inverse of GC window (in seconds)
	queryValidTransaction = `
	SELECT $1 >= (
		SELECT MIN(%[1]s::text::bigint) FROM %[2]s WHERE %[3]s >= NOW() - INTERVAL '%[4]f seconds'
	) as fresh, $1 > (
		SELECT MAX(%[1]s::text::bigint) FROM %[2]s
	) as unknown;`
)

func (pgd *pgDatastore) optimizedRevisionFunc(ctx context.Context) (datastore.Revision, time.Duration, error) {
	var revision XID8
	var validForNanos time.Duration
	if err := pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), pgd.optimizedRevisionQuery,
	).Scan(&revision, &validForNanos); err != nil {
		return datastore.NoRevision, 0, fmt.Errorf(errRevision, err)
	}

	return revisionFromTransaction(revision), validForNanos, nil
}

func (pgd *pgDatastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "HeadRevision")
	defer span.End()

	revision, err := pgd.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}

	return revisionFromTransaction(revision), nil
}

func (pgd *pgDatastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	ctx, span := tracer.Start(ctx, "CheckRevision")
	defer span.End()

	revisionTx := transactionFromRevision(revision)

	var freshEnough, unknown bool
	if err := pgd.dbpool.QueryRow(
		datastore.SeparateContextWithTracing(ctx), pgd.validTransactionQuery, revisionTx,
	).Scan(&freshEnough, &unknown); err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if unknown {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}
	if !freshEnough {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}

	return nil
}

func (pgd *pgDatastore) loadRevision(ctx context.Context) (XID8, error) {
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	sql, args, err := getRevision.ToSql()
	if err != nil {
		return XID8{}, fmt.Errorf(errRevision, err)
	}

	var revision XID8
	err = pgd.dbpool.QueryRow(datastore.SeparateContextWithTracing(ctx), sql, args...).Scan(&revision)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return XID8{}, nil
		}
		return XID8{}, fmt.Errorf(errRevision, err)
	}

	return revision, nil
}

func revisionFromTransaction(txID XID8) datastore.Revision {
	return decimal.NewFromInt(int64(txID.Uint))
}

func transactionFromRevision(revision datastore.Revision) XID8 {
	return XID8{
		Uint:   uint64(revision.IntPart()),
		Status: pgtype.Present,
	}
}

func createNewTransaction(ctx context.Context, tx pgx.Tx) (newTxnID uint64, newXID XID8, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	err = tx.QueryRow(ctx, createTxn).Scan(&newTxnID, &newXID)
	return
}

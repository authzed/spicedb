package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var ParseRevisionString = revisions.RevisionParser(revisions.TransactionID)

const (
	errRevision      = "unable to find revision: %w"
	errCheckRevision = "unable to check revision: %w"

	// querySelectRevision will round the database's timestamp down to the nearest
	// quantization period, and then find the first transaction after that. If there
	// are no transactions newer than the quantization period, it just picks the latest
	// transaction. It will also return the amount of nanoseconds until the next
	// optimized revision would be selected server-side, for use with caching.
	//
	//   %[1] Name of id column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Quantization period (in nanoseconds)
	//   %[5] Follower read delay (in nanoseconds)
	querySelectRevision = `SELECT COALESCE((
			SELECT MIN(%[1]s)
			FROM   %[2]s
			WHERE  %[3]s >= FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) * 1000000000 - %[5]d) / %[4]d) * %[4]d / 1000000000)
		), (
			SELECT MAX(%[1]s)
			FROM   %[2]s
		)) as revision,
		%[4]d - CAST(UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) * 1000000000 AS UNSIGNED INTEGER) %% %[4]d as validForNanos;`

	// queryValidTransaction will return a single row with two values, one boolean
	// for whether the specified transaction ID is newer than the garbage collection
	// window, and one boolean for whether the transaction ID represents a transaction
	// that will occur in the future.
	// It treats the current head transaction as always valid even if it falls
	// outside the GC window.
	//
	//   %[1] Name of id column
	//   %[2] Relationship tuple transaction table
	//   %[3] Name of timestamp column
	//   %[4] Inverse of GC window (in seconds)
	queryValidTransaction = `
		SELECT ? >= COALESCE((
			SELECT MIN(%[1]s)
			FROM   %[2]s
			WHERE  %[3]s >= TIMESTAMPADD(SECOND, %.6[4]f, UTC_TIMESTAMP(6))
		),( 
		    SELECT MAX(%[1]s)
		    FROM %[2]s
		    LIMIT 1
		)) as fresh, ? > (
			SELECT MAX(%[1]s)
			FROM   %[2]s
		) as unknown;`
)

func (mds *Datastore) optimizedRevisionFunc(ctx context.Context) (datastore.Revision, time.Duration, error) {
	var rev uint64
	var validForNanos time.Duration
	if err := mds.db.QueryRowContext(ctx, mds.optimizedRevisionQuery).
		Scan(&rev, &validForNanos); err != nil {
		return datastore.NoRevision, 0, fmt.Errorf(errRevision, err)
	}
	return revisions.NewForTransactionID(rev), validForNanos, nil
}

func (mds *Datastore) HeadRevision(ctx context.Context) (datastore.Revision, error) {
	revision, err := mds.loadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, err
	}
	if revision == 0 {
		return datastore.NoRevision, nil
	}

	return revisions.NewForTransactionID(revision), nil
}

func (mds *Datastore) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	if revision == datastore.NoRevision {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	rev, ok := revision.(revisions.TransactionIDRevision)
	if !ok {
		return fmt.Errorf("expected transaction revision, got %T", revision)
	}

	revisionTx := rev.TransactionID()
	freshEnough, unknown, err := mds.checkValidTransaction(ctx, revisionTx)
	if err != nil {
		return fmt.Errorf(errCheckRevision, err)
	}

	if !freshEnough {
		return datastore.NewInvalidRevisionErr(revision, datastore.RevisionStale)
	}
	if unknown {
		return datastore.NewInvalidRevisionErr(revision, datastore.CouldNotDetermineRevision)
	}

	return nil
}

func (mds *Datastore) loadRevision(ctx context.Context) (uint64, error) {
	// slightly changed to support no revisions at all, needed for runtime seeding of first transaction
	ctx, span := tracer.Start(ctx, "loadRevision")
	defer span.End()

	query, args, err := mds.GetLastRevision.ToSql()
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}

	var revision *uint64
	err = mds.db.QueryRowContext(ctx, query, args...).Scan(&revision)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf(errRevision, err)
	}

	if revision == nil {
		return 0, nil
	}

	return *revision, nil
}

func (mds *Datastore) checkValidTransaction(ctx context.Context, revisionTx uint64) (bool, bool, error) {
	ctx, span := tracer.Start(ctx, "checkValidTransaction")
	defer span.End()

	var freshEnough, unknown sql.NullBool

	err := mds.db.QueryRowContext(ctx, mds.validTransactionQuery, revisionTx, revisionTx).
		Scan(&freshEnough, &unknown)
	if err != nil {
		return false, false, fmt.Errorf(errCheckRevision, err)
	}

	span.AddEvent("DB returned validTransaction checks")

	return freshEnough.Bool, unknown.Bool, nil
}

func (mds *Datastore) createNewTransaction(ctx context.Context, tx *sql.Tx, metadata map[string]any) (newTxnID uint64, err error) {
	ctx, span := tracer.Start(ctx, "createNewTransaction")
	defer span.End()

	var wrappedMetadata structpbWrapper
	if len(metadata) > 0 {
		wrappedMetadata = metadata
	}

	createQuery := mds.createTxn.Values(&wrappedMetadata)
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: %w", err)
	}

	sql, args, err := createQuery.ToSql()
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: %w", err)
	}

	result, err := tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: %w", err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("createNewTransaction: failed to get last inserted id: %w", err)
	}

	uintLastInsertID, err := safecast.ToUint64(lastInsertID)
	if err != nil {
		return 0, spiceerrors.MustBugf("lastInsertID was negative: %v", err)
	}

	return uintLastInsertID, nil
}

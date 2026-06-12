//go:build datastore

package crdb

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// newCancellationTestDatastore builds an unwrapped *crdbDatastore with
// in-band query cancellation enabled, against a dockerized CRDB.
func newCancellationTestDatastore(t *testing.T) *crdbDatastore {
	t.Helper()
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	var cds *crdbDatastore
	b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := newCRDBDatastore(t.Context(), uri,
			GCWindow(veryLargeGCWindow),
			RevisionQuantization(0),
			WithQueryCancellation(true),
			WithAcquireTimeout(5*time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ds.Close() })
		cds = ds.(*crdbDatastore)
		return ds
	})
	require.NotNil(t, cds)
	return cds
}

func scanIgnore(_ context.Context, row pgx.Row) error {
	var ignored any
	return row.Scan(&ignored)
}

func TestQueryCancellationSessionIDsRegistered(t *testing.T) {
	cds := newCancellationTestDatastore(t)
	ctx := t.Context()

	// Force at least one write connection to exist.
	require.NoError(t, cds.writePool.QueryRowFunc(ctx, scanIgnore, "SELECT 1"))

	found := 0
	cds.writePool.Range(func(conn *pgx.Conn, _ uint32) {
		if _, ok := cds.canceler.SessionID(conn.PgConn()); ok {
			found++
		}
	})
	require.Positive(t, found, "write pool connections must have registered session IDs")
}

func TestQueryCancellationRollsBackWriteAndKeepsConnection(t *testing.T) {
	cds := newCancellationTestDatastore(t)
	ctx := t.Context()

	require.NoError(t, cds.writePool.ExecFunc(ctx,
		func(_ context.Context, _ pgconn.CommandTag, err error) error { return err },
		"CREATE TABLE IF NOT EXISTS cancellation_test (id INT PRIMARY KEY)"))

	baselineNewConns := cds.writePool.Stat().NewConnsCount()

	cancelCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err := cds.writePool.BeginTxFunc(cancelCtx, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(cancelCtx, "INSERT INTO cancellation_test (id) VALUES (1)"); err != nil {
			return err
		}
		_, err := tx.Exec(cancelCtx, "SELECT pg_sleep(10)")
		return err
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The transaction must have been rolled back on a live connection.
	var count int
	require.NoError(t, cds.writePool.QueryRowFunc(ctx, func(_ context.Context, row pgx.Row) error {
		return row.Scan(&count)
	}, "SELECT COUNT(*) FROM cancellation_test"))
	require.Zero(t, count, "canceled transaction must be rolled back")
	require.Equal(t, baselineNewConns, cds.writePool.Stat().NewConnsCount(),
		"write cancellation must not destroy and replace the connection")
}

// TestQueryCancellationStressNoStaleErrors races cancellation against query
// completion many times and verifies that no cancellation ever bleeds into a
// subsequent query on the same pool — the failure mode that forced the revert
// of pgwire-protocol cancellation in #2434.
func TestQueryCancellationStressNoStaleErrors(t *testing.T) {
	cds := newCancellationTestDatastore(t)
	ctx := t.Context()

	tripwireBefore := testutil.ToFloat64(pool.UnexpectedCancellationErrors)

	for i := 0; i < 300; i++ {
		// Jitter the deadline around the query duration (~10ms) so the
		// cancellation races completion in both directions.
		timeout := time.Duration(5+rand.IntN(15)) * time.Millisecond
		cancelCtx, cancel := context.WithTimeout(ctx, timeout)
		_ = cds.writePool.QueryRowFunc(cancelCtx, scanIgnore, "SELECT pg_sleep(0.01)")
		cancel()

		err := cds.writePool.QueryRowFunc(ctx, scanIgnore, "SELECT 1")
		require.NoError(t, err,
			"iteration %d: follow-up query failed — possible stale cancellation", i)
	}

	require.Equal(t, tripwireBefore, testutil.ToFloat64(pool.UnexpectedCancellationErrors),
		"no query may observe 57014 without its own context being canceled")
}

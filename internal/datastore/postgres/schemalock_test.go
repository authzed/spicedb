//go:build datastore && postgres

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TestSchemaHashLockShareSkipsWaitingExclusive checks how Postgres orders the row locks that
// assertSchemaHash takes on the schema_revision row. Relationship writes take FOR SHARE and
// schema writes take FOR UPDATE.
//
// It runs three transactions:
//
//	A: FOR SHARE                 granted
//	B: FOR UPDATE                waits for A
//	C: FOR SHARE                 granted while B waits: it does not queue behind B
//	A commits                    B still waits, now for C
//	C commits                    B is granted
//
// If C were queued behind B, a schema write could not be starved by relationship writes. Because
// C is granted, a schema write waits until no relationship write holds FOR SHARE, which under
// overlapping relationship write traffic may not happen.
func TestSchemaHashLockShareSkipsWaitingExclusive(t *testing.T) {
	if postgresConfig.pgbouncer {
		t.Skip("observes backend PIDs, which pgbouncer does not map to client connections")
	}

	ctx := t.Context()
	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	var uri string
	b.NewDatastore(t, func(engine, u string) datastore.Datastore {
		uri = u
		ds, err := NewPostgresDatastore(ctx, u)
		require.NoError(t, err)
		return ds
	})

	connect := func() *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, uri)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}

	monitor := connect()
	const hash = "somehash"
	_, err := monitor.Exec(ctx, "INSERT INTO schema_revision (name, hash) VALUES ('current', $1)", []byte(hash))
	require.NoError(t, err)

	// begin opens a transaction at the datastore's default write isolation level and takes the
	// schema hash lock with the same query the datastore uses.
	begin := func(ctx context.Context, conn *pgx.Conn, exclusive bool) (pgx.Tx, error) {
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
		if err != nil {
			return nil, err
		}
		if err := assertSchemaHash(ctx, tx, hash, exclusive); err != nil {
			_ = tx.Rollback(context.Background())
			return nil, err
		}
		return tx, nil
	}

	connA, connB, connC := connect(), connect(), connect()

	var pidB int
	require.NoError(t, connB.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pidB))

	// waitingOnLock reports whether B's backend is waiting on a heavyweight lock.
	waitingOnLock := func() bool {
		var waitEventType *string
		require.NoError(t, monitor.QueryRow(ctx,
			"SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1", pidB).Scan(&waitEventType))
		return waitEventType != nil && *waitEventType == "Lock"
	}
	requireBWaiting := func(msg string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !waitingOnLock() {
			require.True(t, time.Now().Before(deadline), msg)
			time.Sleep(10 * time.Millisecond)
		}
	}

	// A takes FOR SHARE.
	txA, err := begin(ctx, connA, false)
	require.NoError(t, err)

	// B requests FOR UPDATE and waits for A.
	bDone := make(chan error, 1)
	go func() {
		txB, err := begin(ctx, connB, true)
		if err == nil {
			err = txB.Commit(ctx)
		}
		bDone <- err
	}()
	requireBWaiting("B's FOR UPDATE should wait for A's FOR SHARE")

	// C requests FOR SHARE while B is waiting. A short timeout turns "queued behind B" into a
	// failure here instead of a hang.
	cCtx, cCancel := context.WithTimeout(ctx, 3*time.Second)
	defer cCancel()
	txC, err := begin(cCtx, connC, false)
	require.NoError(t, err, "C's FOR SHARE was not granted while B's FOR UPDATE was waiting: new FOR SHARE requests queue behind a waiting FOR UPDATE")

	// A commits. B keeps waiting, now for C.
	require.NoError(t, txA.Commit(ctx))
	time.Sleep(200 * time.Millisecond)
	require.True(t, waitingOnLock(), "B's FOR UPDATE should still wait while C holds FOR SHARE")
	select {
	case err := <-bDone:
		t.Fatalf("B finished while C held FOR SHARE: %v", err)
	default:
	}

	// C commits. B is granted.
	require.NoError(t, txC.Commit(ctx))
	select {
	case err := <-bDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("B's FOR UPDATE was not granted after every FOR SHARE holder committed")
	}
}

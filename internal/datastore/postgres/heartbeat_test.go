//go:build datastore && postgres

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

const (
	heartbeatTestTimeout = 30 * time.Second
	heartbeatTestTick    = 100 * time.Millisecond
)

func TestRevisionHeartbeatFollowerRecoversFromSessionLoss(t *testing.T) {
	ctx := t.Context()
	uri, ds := newHeartbeatTestDatastore(t)
	admin := connectForHeartbeatTest(t, uri)

	// Hold the heartbeat lock externally so the datastore stays a follower.
	var acquired bool
	require.NoError(t, admin.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, revisionHeartbeatLock).Scan(&acquired))
	require.True(t, acquired)

	ds()
	require.Eventually(t, func() bool {
		var count int
		err := admin.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE '%pg_try_advisory_lock%'
		`).Scan(&count)
		return err == nil && count > 0
	}, heartbeatTestTimeout, heartbeatTestTick, "datastore never attempted to acquire the heartbeat lock")

	terminateOtherBackends(t, admin)

	var released bool
	require.NoError(t, admin.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, revisionHeartbeatLock).Scan(&released))
	require.True(t, released)

	requireHeartbeatLeader(t, admin, 0)
	requireHeartbeatsWritten(t, admin)
}

func TestRevisionHeartbeatLeaderRecoversFromSessionLoss(t *testing.T) {
	uri, ds := newHeartbeatTestDatastore(t)
	admin := connectForHeartbeatTest(t, uri)

	ds()
	leaderPID := requireHeartbeatLeader(t, admin, 0)

	terminateOtherBackends(t, admin)

	requireHeartbeatLeader(t, admin, leaderPID)
	requireHeartbeatsWritten(t, admin)
}

func TestRevisionHeartbeatReleasesLockOnShutdown(t *testing.T) {
	uri, ds := newHeartbeatTestDatastore(t)
	admin := connectForHeartbeatTest(t, uri)

	leader, ok := ds().(*pgDatastore)
	require.True(t, ok)
	requireHeartbeatLeader(t, admin, 0)

	// Stop the background workers, keeping the pool open, so a session returned to
	// the pool while still holding the lock would be observed. A session closed by
	// pgx is torn down asynchronously, so the lock may linger briefly.
	leader.cancelGc()
	err := leader.workerGroup.Wait()
	require.ErrorIs(t, err, context.Canceled)

	require.Eventually(t, func() bool {
		pid, err := heartbeatLockHolder(t.Context(), admin)
		require.NoError(t, err)
		return pid == 0
	}, 10*time.Second, 100*time.Millisecond, "heartbeat lock still held after worker shutdown")
}

// newHeartbeatTestDatastore returns the URI of a new migrated database and a function
// that starts a heartbeat-enabled datastore against it.
func newHeartbeatTestDatastore(t *testing.T) (string, func() datastore.Datastore) {
	t.Helper()

	var uri string
	testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, false).NewDatastore(t, func(_, u string) datastore.Datastore {
		uri = u
		return nil
	})

	return uri, func() datastore.Datastore {
		ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID,
			RevisionQuantization(time.Second),
			GCInterval(disableBackgroundGC),
			WithRevisionHeartbeat(true),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ds.Close() })
		return ds
	}
}

func connectForHeartbeatTest(t *testing.T, uri string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), uri)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func terminateOtherBackends(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	_, err := admin.Exec(t.Context(), `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid()
	`)
	require.NoError(t, err)
}

// heartbeatLockHolder returns the pid of the session holding the heartbeat lock, or 0.
func heartbeatLockHolder(ctx context.Context, admin *pgx.Conn) (uint32, error) {
	var pid uint32
	err := admin.QueryRow(ctx, `
		SELECT l.pid FROM pg_locks l
		JOIN pg_database d ON d.oid = l.database
		WHERE l.locktype = 'advisory' AND l.objid = $1 AND l.granted AND d.datname = current_database()
	`, revisionHeartbeatLock).Scan(&pid)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pid, err
}

// requireHeartbeatLeader waits for a session other than the admin connection and
// previousPID to hold the heartbeat lock and returns its pid.
func requireHeartbeatLeader(t *testing.T, admin *pgx.Conn, previousPID uint32) uint32 {
	t.Helper()
	adminPID := admin.PgConn().PID()
	var pid uint32
	require.Eventually(t, func() bool {
		holder, err := heartbeatLockHolder(t.Context(), admin)
		if err != nil || holder == 0 || holder == adminPID || holder == previousPID {
			return false
		}
		pid = holder
		return true
	}, heartbeatTestTimeout, heartbeatTestTick, "no datastore session acquired the heartbeat lock")
	return pid
}

func requireHeartbeatsWritten(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	countRows := func() (int, error) {
		var count int
		err := admin.QueryRow(t.Context(), `SELECT count(*) FROM relation_tuple_transaction`).Scan(&count)
		return count, err
	}

	initial, err := countRows()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		count, err := countRows()
		return err == nil && count >= initial+2
	}, heartbeatTestTimeout, heartbeatTestTick, "heartbeat revisions are not being written")
}

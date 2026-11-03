//go:build datastore && postgres

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
)

func TestRevisionHeartbeatFollowerReconnects(t *testing.T) {
	engine := testdatastore.RunPostgresForTesting(t, postgresTestVersion(), false)
	uri := engine.NewDatabase(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	admin, err := pgx.Connect(ctx, uri)
	require.NoError(t, err)
	defer admin.Close(context.Background())
	_, err = admin.Exec(ctx, "CREATE TABLE heartbeat_test (id integer)")
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "SELECT pg_advisory_lock($1)", revisionHeartbeatLock)
	require.NoError(t, err)

	config, err := pgxpool.ParseConfig(uri)
	require.NoError(t, err)
	config.MaxConns = 2
	config.ConnConfig.RuntimeParams["application_name"] = "heartbeat-follower-test"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()
	pgd := &pgDatastore{
		writePool:              pool,
		revisionHeartbeatQuery: "INSERT INTO heartbeat_test VALUES (1)",
	}
	done := make(chan error, 1)
	go func() { done <- pgd.startRevisionHeartbeat(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("heartbeat worker did not stop")
		}
	}()

	var oldPID int
	require.Eventually(t, func() bool {
		return admin.QueryRow(ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='heartbeat-follower-test'
			AND query LIKE '%pg_try_advisory_lock%'`).Scan(&oldPID) == nil
	}, 5*time.Second, 20*time.Millisecond)
	var terminated bool
	require.NoError(t, admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", oldPID).Scan(&terminated))
	require.True(t, terminated)
	_, err = admin.Exec(ctx, "SELECT pg_advisory_unlock($1)", revisionHeartbeatLock)
	require.NoError(t, err)

	// The same worker must take over on a healthy connection and resume heartbeats.
	require.Eventually(t, func() bool {
		var recovered bool
		err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM heartbeat_test)
			AND EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING(pid)
				WHERE a.datname=current_database() AND a.application_name='heartbeat-follower-test'
				AND l.locktype='advisory' AND l.classid=0 AND l.objid=2 AND l.granted
				AND a.pid<>$1)`, oldPID).Scan(&recovered)
		return err == nil && recovered
	}, 8*time.Second, 20*time.Millisecond)
}

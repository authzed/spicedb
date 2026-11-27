//go:build datastore && postgres
// +build datastore,postgres

package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// TestStrictReadGuardDetectsOutOfOrderCommitOnStandby covers a write whose
// transaction commits after a transaction with a higher xid, which is routine
// whenever a write stays open across network round trips to the primary.
//
// A hot standby that has applied the later commit but not the write reports a
// snapshot whose xmax is past the write and whose in-progress list is empty:
// standby snapshots keep in-progress xids in subxip, which pg_current_snapshot()
// does not export. Only the snapshot's xmin still shows the write as running.
// The guard must raise here instead of serving a read that misses the write.
func TestStrictReadGuardDetectsOutOfOrderCommitOnStandby(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := runPGReplicaCluster(t)

	primary, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer primary.Close(ctx)

	_, err = primary.Exec(ctx, `CREATE TABLE nsdemo (name text NOT NULL, created_xid xid8 NOT NULL)`)
	require.NoError(t, err)
	cluster.waitForReplicaCaughtUp(ctx, t)

	// The write under test records its xid and statement snapshot, as
	// createNewTransaction does, and stays open.
	writeConn, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer writeConn.Close(ctx)
	writeTx, err := writeConn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = writeTx.Rollback(ctx) }()

	var writeXidStr, writeSnapshotText string
	require.NoError(t, writeTx.QueryRow(ctx,
		`INSERT INTO nsdemo (name, created_xid) VALUES ('auth/platform', pg_current_xact_id()) RETURNING created_xid::text, pg_current_snapshot()::text`,
	).Scan(&writeXidStr, &writeSnapshotText))

	// A transaction with a higher xid commits first, and the replica applies it.
	_, err = primary.Exec(ctx, `INSERT INTO nsdemo (name, created_xid) VALUES ('later', pg_current_xact_id())`)
	require.NoError(t, err)
	cluster.waitForReplicaCaughtUp(ctx, t)

	// The write commits on the primary but not yet on the replica.
	cluster.pauseReplica(ctx, t)
	require.NoError(t, writeTx.Commit(ctx))

	// The revision SpiceDB returns for the write.
	writeXid := mustUint64(t, writeXidStr)
	rev := postgresRevision{snapshot: snapshotFromText(ctx, t, primary, writeSnapshotText).markComplete(writeXid)}

	replica, err := pgx.Connect(ctx, cluster.replicaURI)
	require.NoError(t, err)
	defer replica.Close(ctx)

	var replicaSnapshot string
	require.NoError(t, replica.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&replicaSnapshot))
	t.Logf("write xid=%d, revision=%s, replica snapshot=%s", writeXid, rev.snapshot, replicaSnapshot)

	innerSelect := fmt.Sprintf(
		`SELECT name FROM nsdemo WHERE name = 'auth/platform' AND pg_visible_in_snapshot(created_xid, '%s'::pg_snapshot) = true`,
		rev.snapshot,
	)
	found, err := runStrictGuardedSelect(ctx, replica, rev, innerSelect)
	require.ErrorAs(t, err, &common.RevisionUnavailableError{},
		"the guard must raise while the replica has not applied the write; got rows %v", found)

	cluster.resumeReplica(ctx, t)
	cluster.waitForReplicaCaughtUp(ctx, t)

	found, err = runStrictGuardedSelect(ctx, replica, rev, innerSelect)
	require.NoError(t, err, "after catch-up the guard must pass")
	require.Equal(t, []string{"auth/platform"}, found)
}

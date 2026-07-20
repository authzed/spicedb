//go:build datastore && postgres
// +build datastore,postgres

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

// These tests cover https://github.com/authzed/spicedb/issues/2525: intermittent
// "object definition X not found" errors when reading from a Postgres read
// replica, even though the follower-read delay exceeds the replication latency.
//
// A revision snapshot makes visible any committed transaction with xid < xmax
// that is not in its in-progress list, so data can be written by transactions
// with xid >= xmin. When a concurrent, still-open transaction holds xmin below a
// freshly-committed write, a lagging replica can be missing that write while still
// appearing (to a naive xmin-based guard) to be caught up. The strict-read guard
// must instead confirm the replica's WAL replay has reached the revision's xmax
// before serving the read, and otherwise fall back to the primary.
//
// At production write QPS there is almost always an in-progress write transaction
// with a lower xid than the most recently committed ones, so this occurs
// naturally; the tests make it deterministic with an explicit held-open
// transaction plus paused WAL replay.

// TestStrictReadGuardDetectsVisibleDataGapOnReplica exercises the strict-read
// guard at the Postgres level: a transaction whose xid is >= the revision's xmin
// is visible in the revision but not yet present on a lagging replica. The guard
// must detect this and raise a replication-lag error (so the caller falls back to
// the primary), and once the replica catches up the same read must succeed.
func TestStrictReadGuardDetectsVisibleDataGapOnReplica(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := runPGReplicaCluster(t)

	primary, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer primary.Close(ctx)

	// A table that mimics a SpiceDB metadata table: each row records the xid of
	// the transaction that created it, exactly like namespace_config.created_xid.
	_, err = primary.Exec(ctx, `CREATE TABLE nsdemo (name text NOT NULL, created_xid xid8 NOT NULL)`)
	require.NoError(t, err)

	// Ensure the replica has the (empty) table and has replayed everything up to now.
	cluster.waitForReplicaCaughtUp(ctx, t)

	// Open a transaction on a SEPARATE connection and force it to be assigned an
	// xid. While it stays open it is the lowest in-progress xid, so it becomes the
	// `xmin` of any snapshot taken now. This is the crux: it depresses xmin below
	// the write we are about to make. It must be its own connection so the write
	// below is a distinct, committed transaction.
	holdConn, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer holdConn.Close(ctx)
	holdTx, err := holdConn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holdTx.Rollback(ctx) }()

	var xidHoldStr string
	require.NoError(t, holdTx.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidHoldStr))

	// Pause WAL replay: anything committed on the primary from here on is received
	// but not applied by the replica.
	cluster.pauseReplica(ctx, t)

	// Write the "namespace" row in a separate (auto-commit) transaction. Because
	// the held-open transaction already consumed a lower xid, this write's xid is
	// strictly greater than xmin.
	var xidWriteStr string
	require.NoError(t, primary.QueryRow(ctx,
		`INSERT INTO nsdemo (name, created_xid) VALUES ('auth/platform', pg_current_xact_id()) RETURNING created_xid::text`,
	).Scan(&xidWriteStr))

	// Capture the head snapshot on the primary, exactly as HeadRevision does.
	var snapshotText string
	var xminStr string
	require.NoError(t, primary.QueryRow(ctx, `
		SELECT s::text, pg_snapshot_xmin(s)::text
		FROM (SELECT pg_current_snapshot() AS s) x
	`).Scan(&snapshotText, &xminStr))

	xidHold := mustUint64(t, xidHoldStr)
	xidWrite := mustUint64(t, xidWriteStr)
	xmin := mustUint64(t, xminStr)

	t.Logf("held-open xid=%d, write xid=%d, head snapshot=%s (xmin=%d)", xidHold, xidWrite, snapshotText, xmin)

	// Preconditions that make this a faithful reproduction. xmin is at most the
	// held-open xid (a background reader such as autovacuum could depress it
	// further, which only strengthens the effect); the essential condition is that
	// the write's xid is greater than xmin, so it is visible in the revision yet
	// lives above the transaction an xmin-based guard would have checked.
	require.LessOrEqual(t, xmin, xidHold, "held-open transaction should bound the snapshot xmin")
	require.Greater(t, xidWrite, xmin, "the write's xid must be > xmin for the gap to exist")

	// The row IS visible in the head snapshot (this is what SpiceDB's row filter
	// uses: pg_visible_in_snapshot(created_xid, revision.snapshot)).
	var visibleOnPrimary bool
	require.NoError(t, primary.QueryRow(ctx,
		`SELECT pg_visible_in_snapshot($1::xid8, $2::pg_snapshot)`, xidWriteStr, snapshotText,
	).Scan(&visibleOnPrimary))
	require.True(t, visibleOnPrimary, "write must be visible in the head snapshot")

	// ...but the visible row is genuinely absent on the lagging replica: its
	// transaction is "in the future" there, because replay is paused.
	replica, err := pgx.Connect(ctx, cluster.replicaURI)
	require.NoError(t, err)
	defer replica.Close(ctx)

	var writeStatus string
	err = replica.QueryRow(ctx, `SELECT pg_xact_status($1::xid8)`, xidWriteStr).Scan(&writeStatus)
	require.Error(t, err, "the write xid should be unresolvable (in the future) on the lagging replica; got status %q", writeStatus)
	t.Logf("pg_xact_status(write xid=%d) on replica errored as expected: %v", xidWrite, err)

	// Run the real strict-read guard (SQL + drain + error rewriting) against the
	// replica. It must raise a RevisionUnavailableError so the strict replicated
	// reader falls back to the primary instead of returning zero rows.
	innerSelect := fmt.Sprintf(
		`SELECT name FROM nsdemo WHERE name = 'auth/platform' AND pg_visible_in_snapshot(created_xid, '%s'::pg_snapshot) = true`,
		snapshotText,
	)
	rev := postgresRevision{snapshot: snapshotFromText(ctx, t, primary, snapshotText)}

	found, err := runStrictGuardedSelect(ctx, replica, rev, innerSelect)
	require.Error(t, err, "the strict guard must raise when the replica is missing visible data")
	require.ErrorAs(t, err, &common.RevisionUnavailableError{},
		"guard error must map to RevisionUnavailableError so the reader falls back to the primary")
	require.Empty(t, found)
	t.Logf("guard correctly reported the replica is behind: %v", err)

	// The guard must also raise for a query that matches nothing. A read whose
	// rows are missing *because* of the lag looks exactly like a query with no
	// matches, so a guard that only runs once there is something to filter would
	// stay silent in precisely the case that matters.
	_, err = runStrictGuardedSelect(ctx, replica, rev, fmt.Sprintf(
		`SELECT name FROM nsdemo WHERE name = 'auth/nonexistent' AND pg_visible_in_snapshot(created_xid, '%s'::pg_snapshot) = true`,
		snapshotText,
	))
	require.ErrorAs(t, err, &common.RevisionUnavailableError{},
		"the guard must raise on a lagging replica even when the query matches no rows")

	// After catch-up, the same guarded read succeeds and returns the row.
	require.NoError(t, holdTx.Rollback(ctx))
	cluster.resumeReplica(ctx, t)
	cluster.waitForReplicaCaughtUp(ctx, t)

	found, err = runStrictGuardedSelect(ctx, replica, rev, innerSelect)
	require.NoError(t, err, "after catch-up the guard must pass")
	require.Equal(t, []string{"auth/platform"}, found, "after catch-up the row is present")
}

// runStrictGuardedSelect runs innerSelect through the production strictReaderQueryFuncs
// against conn, returning the collected first-column strings and any (rewritten) error.
func runStrictGuardedSelect(ctx context.Context, conn *pgx.Conn, rev postgresRevision, innerSelect string) ([]string, error) {
	srqf := strictReaderQueryFuncs{wrapped: pgxcommon.QuerierFuncsFor(conn), revision: rev}
	var found []string
	err := srqf.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			found = append(found, name)
		}
		return rows.Err()
	}, innerSelect)
	return found, err
}

// TestStrictReadGuardHoldsAcrossStatementBoundary covers the residual failure
// reported against this fix: the guard is evaluated twice — once as the SELECT's
// filter, once in the trailing DO block — and under READ COMMITTED *each
// statement of a multi-statement simple query takes its own snapshot*. If the
// replica catches up in between, the filter drops every row (its snapshot was
// behind) while the assertion passes (its snapshot is not), so the caller sees
// zero rows and no error: the silent "object definition not found".
//
// In production that window is microseconds wide, which is why it only appears
// under sustained load. Here it is driven deterministically: many guarded reads
// are held in flight against a replica with WAL replay paused, then replay is
// resumed, so some of those reads straddle the instant the replica catches up.
func TestStrictReadGuardHoldsAcrossStatementBoundary(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := runPGReplicaCluster(t)

	primary, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer primary.Close(ctx)

	_, err = primary.Exec(ctx, `CREATE TABLE nsdemo (name text NOT NULL, created_xid xid8 NOT NULL)`)
	require.NoError(t, err)
	cluster.waitForReplicaCaughtUp(ctx, t)

	const (
		readers = 16
		cycles  = 20
	)

	conns := make([]*pgx.Conn, 0, readers)
	for range readers {
		conn, err := pgx.Connect(ctx, cluster.replicaURI)
		require.NoError(t, err)
		defer conn.Close(ctx)
		conns = append(conns, conn)
	}

	var servedFromReplica, reportedLag, silentlyEmpty atomic.Int64
	var unexpectedErr error
	var unexpectedErrOnce sync.Once

	for cycle := range cycles {
		cluster.pauseReplica(ctx, t)

		// Commit a row on the primary that the (now frozen) replica cannot have.
		name := fmt.Sprintf("auth/ns%d", cycle)
		_, err = primary.Exec(ctx,
			`INSERT INTO nsdemo (name, created_xid) VALUES ($1, pg_current_xact_id())`, name)
		require.NoError(t, err)

		// The revision a FullyConsistent read would use: head on the primary,
		// which makes the row above visible.
		var snapshotText string
		require.NoError(t, primary.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snapshotText))
		rev := postgresRevision{snapshot: snapshotFromText(ctx, t, primary, snapshotText)}

		innerSelect := fmt.Sprintf(
			`SELECT name FROM nsdemo WHERE name = '%s' AND pg_visible_in_snapshot(created_xid, '%s'::pg_snapshot) = true`,
			name, snapshotText,
		)

		found, err := runStrictGuardedSelect(ctx, primary, rev, innerSelect)
		require.NoError(t, err)
		require.Equal(t, []string{name}, found, "the row must be visible at this revision on the primary")

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for _, conn := range conns {
			wg.Add(1)
			go func(conn *pgx.Conn) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}

					found, err := runStrictGuardedSelect(ctx, conn, rev, innerSelect)
					switch {
					case err != nil:
						// The correct behavior while the replica is behind: the
						// strict replicated reader falls back to the primary.
						if !errors.As(err, &common.RevisionUnavailableError{}) {
							unexpectedErrOnce.Do(func() { unexpectedErr = err })
							return
						}
						reportedLag.Add(1)
					case len(found) == 1:
						// The correct behavior once the replica has caught up.
						servedFromReplica.Add(1)
					default:
						// No rows and no error: the caller concludes the object
						// does not exist. This is the bug.
						silentlyEmpty.Add(1)
					}
				}
			}(conn)
		}

		// Let reads pile up against the lagging replica, then let it catch up
		// underneath them.
		time.Sleep(100 * time.Millisecond)
		cluster.resumeReplica(ctx, t)
		cluster.waitForReplicaCaughtUp(ctx, t)
		time.Sleep(100 * time.Millisecond)

		close(stop)
		wg.Wait()
		require.NoError(t, unexpectedErr)

		if silentlyEmpty.Load() > 0 {
			t.Logf("cycle %d: guard passed with zero rows", cycle)
			break
		}
	}

	t.Logf("served by replica: %d, reported lag (falls back to primary): %d, silently empty: %d",
		servedFromReplica.Load(), reportedLag.Load(), silentlyEmpty.Load())

	require.Positive(t, reportedLag.Load(), "reads against the paused replica must report lag")
	require.Positive(t, servedFromReplica.Load(), "reads after catch-up must be served by the replica")
	require.Zero(t, silentlyEmpty.Load(),
		"the guard must never return zero rows without an error: the caller reports the object as not found")
}

// TestReplicaLagStrictReadFallsBackForNamespace exercises the reported scenario
// (issue #2525) through the real SpiceDB code path: a namespace is written while a
// concurrent transaction holds xmin below its xid, and the replica is lagging. The
// strict replicated datastore must detect that the replica is missing the revision
// and fall back to the primary, returning the namespace, rather than reporting it
// not-found. The checking replicated datastore must behave the same way.
func TestReplicaLagStrictReadFallsBackForNamespace(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := runPGReplicaCluster(t)

	runMigrations(ctx, t, cluster.primaryURI)

	// Primary datastore (writes). Quantization 0 so HeadRevision reflects the
	// freshest snapshot, matching the FullyConsistent path in the issue.
	primaryDS, err := newPostgresDatastore(ctx, cluster.primaryURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(90*time.Minute),
		GCInterval(disableBackgroundGC),
		WatchBufferLength(50),
		WithRevisionHeartbeat(false),
	)
	require.NoError(t, err)
	defer func() { _ = primaryDS.Close() }()

	// Baseline schema, written and fully replicated before we induce lag.
	_, err = primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, namespace.Namespace("auth/user"))
	})
	require.NoError(t, err)
	cluster.waitForReplicaCaughtUp(ctx, t)

	// Read replica datastore, with strict read mode as production requires.
	replicaDS, err := newPostgresDatastore(ctx, cluster.replicaURI, 0,
		ReadStrictMode(true),
		RevisionQuantization(0),
		WatchBufferLength(50),
		WithRevisionHeartbeat(false),
	)
	require.NoError(t, err)
	defer func() { _ = replicaDS.Close() }()

	strictDS, err := proxy.NewStrictReplicatedDatastore(primaryDS, replicaDS)
	require.NoError(t, err)

	// Hold a transaction open on the primary to depress xmin.
	rawPrimary, err := pgx.Connect(ctx, cluster.primaryURI)
	require.NoError(t, err)
	defer rawPrimary.Close(ctx)
	holdTx, err := rawPrimary.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = holdTx.Rollback(ctx) }()
	var xidHoldStr string
	require.NoError(t, holdTx.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidHoldStr))

	// Pause replay, then write the victim namespace (its xid > xmin).
	cluster.pauseReplica(ctx, t)

	_, err = primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, namespace.Namespace("auth/platform"))
	})
	require.NoError(t, err)

	headRevWithHash, err := strictDS.HeadRevision(ctx)
	require.NoError(t, err)
	headRev := headRevWithHash.Revision
	if pgRev, ok := headRev.(postgresRevision); ok {
		t.Logf("head snapshot xmin=%d (held-open xid=%s)", pgRev.snapshot.xmin, xidHoldStr)
		require.LessOrEqual(t, pgRev.snapshot.xmin, mustUint64(t, xidHoldStr),
			"held-open transaction should bound the head snapshot xmin")
	}

	// The strict replicated datastore routes this read to the lagging replica. The
	// guard must detect that the replica is missing the revision and fall back to
	// the primary, so the namespace (which the replica does not yet have) is found.
	// Finding it proves the read was served by the primary via fallback, since the
	// replica's WAL replay is paused.
	ns, _, err := strictDS.SnapshotReader(headRev).LegacyReadNamespaceByName(ctx, "auth/platform")
	require.NoError(t, err,
		"strict replicated read should fall back to the primary for a lagging replica, got: %v", err)
	require.NotNil(t, ns)
	require.Equal(t, "auth/platform", ns.Name)

	// The checking replicated datastore also falls back to the primary.
	checkingDS, err := proxy.NewCheckingReplicatedDatastore(primaryDS, replicaDS)
	require.NoError(t, err)
	ns, _, err = checkingDS.SnapshotReader(headRev).LegacyReadNamespaceByName(ctx, "auth/platform")
	require.NoError(t, err, "checking replicated datastore should fall back to the primary")
	require.NotNil(t, ns)
	require.Equal(t, "auth/platform", ns.Name)

	// Sanity: once the replica catches up, even the strict path finds it.
	require.NoError(t, holdTx.Rollback(ctx))
	cluster.resumeReplica(ctx, t)
	cluster.waitForReplicaCaughtUp(ctx, t)

	require.Eventually(t, func() bool {
		_, _, err := strictDS.SnapshotReader(headRev).LegacyReadNamespaceByName(ctx, "auth/platform")
		return err == nil
	}, 15*time.Second, 250*time.Millisecond, "strict read should succeed once the replica catches up")
}

// TestReplicaServesCaughtUpRevisionWithoutFallback verifies the happy path the
// fix must preserve: when the replica has caught up to an earlier (min-latency)
// revision, the strict replicated datastore serves the read FROM THE REPLICA and
// returns the correct data, without falling back to the primary. Without this
// guarantee a "fix" could pass merely by routing everything to the primary.
func TestReplicaServesCaughtUpRevisionWithoutFallback(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cluster := runPGReplicaCluster(t)

	runMigrations(ctx, t, cluster.primaryURI)

	primaryDS, err := newPostgresDatastore(ctx, cluster.primaryURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(90*time.Minute),
		GCInterval(disableBackgroundGC),
		WatchBufferLength(50),
		WithRevisionHeartbeat(false),
	)
	require.NoError(t, err)
	defer func() { _ = primaryDS.Close() }()

	// Write the relationship we will read back, capturing its revision as the
	// earlier/min-latency revision to read at.
	earlierRev, err := primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:123#reader@user:456")),
		})
	})
	require.NoError(t, err)

	// Advance the primary past that revision, so we are genuinely reading an
	// earlier revision (as a min-latency/follower read does).
	_, err = primaryDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:999#reader@user:456")),
		})
	})
	require.NoError(t, err)

	replicaDS, err := newPostgresDatastore(ctx, cluster.replicaURI, 0,
		ReadStrictMode(true),
		RevisionQuantization(0),
		WatchBufferLength(50),
		WithRevisionHeartbeat(false),
	)
	require.NoError(t, err)
	defer func() { _ = replicaDS.Close() }()

	replicaID, err := replicaDS.MetricsID()
	require.NoError(t, err)

	strictDS, err := proxy.NewStrictReplicatedDatastore(primaryDS, replicaDS)
	require.NoError(t, err)

	// The replica is caught up to (well past) the revision we read at.
	cluster.waitForReplicaCaughtUp(ctx, t)

	fallbacksBefore := strictReplicaFallbackCount(t, replicaID)

	rels, err := datastore.IteratorToSlice(mustQuery(ctx, t,
		strictDS.SnapshotReader(earlierRev),
		datastore.RelationshipsFilter{OptionalResourceType: "resource", OptionalResourceIds: []string{"123"}},
	))
	require.NoError(t, err)
	require.Len(t, rels, 1, "the relationship should be readable at the earlier revision")
	require.Equal(t, "123", rels[0].Resource.ObjectID)

	fallbacksAfter := strictReplicaFallbackCount(t, replicaID)
	require.Equal(t, fallbacksBefore, fallbacksAfter,
		"the read must be served by the replica, not fall back to the primary")

	t.Logf("replica served the earlier revision read with no fallback (count stayed at %v)", fallbacksAfter)
}

func mustQuery(ctx context.Context, t testing.TB, reader datastore.Reader, filter datastore.RelationshipsFilter) datastore.RelationshipIterator {
	t.Helper()
	it, err := reader.QueryRelationships(ctx, filter)
	require.NoError(t, err)
	return it
}

// strictReplicaFallbackCount reads the strict replicated proxy's fallback counter
// for a specific replica from the default Prometheus registry. The counter is
// labeled by replica id (derived from the replica's unique URL), so it isolates
// this test from others running in parallel. A missing series means zero. The
// value is a whole-number count, returned as an int for exact comparison.
func strictReplicaFallbackCount(t testing.TB, replicaID string) int {
	t.Helper()
	const metricName = "spicedb_datastore_replica_strict_replicated_fallback_query_total"
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "replica" && label.GetValue() == replicaID {
					return int(metric.GetCounter().GetValue())
				}
			}
		}
	}
	return 0
}

func runMigrations(ctx context.Context, t testing.TB, uri string) {
	t.Helper()
	// The xid-backfill migration reads its batch size from the context, as the
	// standard test bootstrap does.
	ctx = context.WithValue(ctx, migrate.BackfillBatchSize, uint64(1000))
	driver, err := pgmigrations.NewAlembicPostgresDriver(ctx, uri, datastore.NoCredentialsProvider, false)
	require.NoError(t, err)
	defer func() { _ = driver.Close(ctx) }()
	require.NoError(t, pgmigrations.DatabaseMigrations.Run(ctx, driver, "head", migrate.LiveRun))
}

func mustUint64(t testing.TB, s string) uint64 {
	t.Helper()
	var v uint64
	_, err := fmt.Sscan(s, &v)
	require.NoError(t, err)
	return v
}

// snapshotFromText reads a pg_snapshot text value back into a pgSnapshot using
// the connection's registered codec, so the guard SQL is generated from the same
// revision the primary produced.
func snapshotFromText(ctx context.Context, t testing.TB, conn *pgx.Conn, snapshotText string) pgSnapshot {
	t.Helper()
	RegisterTypes(conn.TypeMap())
	var snap pgSnapshot
	require.NoError(t, conn.QueryRow(ctx, `SELECT $1::pg_snapshot`, snapshotText).Scan(&snap))
	return snap
}

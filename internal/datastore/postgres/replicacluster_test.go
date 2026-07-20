//go:build datastore && postgres
// +build datastore,postgres

package postgres

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	replClusterDB       = "spicedb"
	replClusterUser     = "postgres"
	replClusterPassword = "testpass"
)

// pgReplicaCluster is a Postgres primary with a single streaming hot-standby
// replica, both running in containers on a shared network. It gives tests
// deterministic control over replication lag via pauseReplica / resumeReplica
// (which pause and resume WAL *replay* on the standby while streaming
// continues), so a revision can be made visible on the primary while provably
// absent on the replica.
type pgReplicaCluster struct {
	primary    testcontainers.Container
	replica    testcontainers.Container
	primaryURI string
	replicaURI string
}

// runPGReplicaCluster stands up the primary + streaming replica and returns once
// both are accepting connections and the replica is streaming from the primary.
func runPGReplicaCluster(t testing.TB) *pgReplicaCluster {
	t.Helper()
	ctx := t.Context()

	testNetwork, err := network.New(ctx)
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, testNetwork)

	// Track the same Postgres version as the rest of the datastore suite so the
	// fix is exercised across the CI version matrix. The physical-replication
	// setup depends on the image layout only through $PGDATA and SHOW hba_file.
	image := "mirror.gcr.io/library/postgres:" + postgresTestVersion()

	// The primary is configured for physical replication and commit-timestamp
	// tracking (required by the SpiceDB Postgres driver). hot_standby=on is
	// harmless on the primary and is inherited by the replica through the base
	// backup.
	primary, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Env: map[string]string{
				"POSTGRES_USER":             replClusterUser,
				"POSTGRES_PASSWORD":         replClusterPassword,
				"POSTGRES_DB":               replClusterDB,
				"POSTGRES_HOST_AUTH_METHOD": "trust",
			},
			Cmd: []string{
				"postgres",
				"-c", "wal_level=replica",
				"-c", "max_wal_senders=10",
				"-c", "max_replication_slots=10",
				"-c", "hot_standby=on",
				"-c", "track_commit_timestamp=on",
				"-c", "listen_addresses=*",
			},
			ExposedPorts:   []string{"5432/tcp"},
			Networks:       []string{testNetwork.Name},
			NetworkAliases: map[string][]string{testNetwork.Name: {"primary"}},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, primary)

	// The default pg_hba.conf only permits replication connections from
	// localhost. Allow the replica (a different container/host) to connect for
	// replication, then reload. SHOW hba_file is used so this is independent of
	// the image's data-directory layout.
	execOrFail(ctx, t, primary, []string{
		"bash", "-c",
		`f=$(psql -U ` + replClusterUser + ` -tA -c "SHOW hba_file"); ` +
			`echo "host replication all all trust" >> "$f"`,
	})
	execOrFail(ctx, t, primary, []string{
		"psql", "-U", replClusterUser, "-c", "SELECT pg_reload_conf()",
	})

	primaryHostPort := hostPort(ctx, t, primary)

	// The replica overrides the image entrypoint: it waits for the primary,
	// takes a streaming base backup (-R writes standby.signal + primary_conninfo),
	// then hands control back to the normal Postgres entrypoint, which starts it
	// as a hot standby. recovery_min_apply_delay is left at 0 because tests drive
	// lag explicitly via pg_wal_replay_pause().
	//
	// PGDATA is pinned to the classic path below (via the container env) rather
	// than the image default: Postgres 18's image uses a nested default
	// (/var/lib/postgresql/18/docker) whose parent does not exist on a fresh
	// container, which pg_basebackup cannot create. The base backup restores the
	// whole cluster regardless of the source path, so a simple destination under
	// the world-writable /var/lib/postgresql works on every version.
	replicaScript := `
set -euo pipefail
echo "replica: waiting for primary"
until pg_isready -h primary -U ` + replClusterUser + ` -d ` + replClusterDB + `; do sleep 1; done
echo "replica: taking base backup"
rm -rf "$PGDATA"/*
pg_basebackup -h primary -U ` + replClusterUser + ` -D "$PGDATA" -Fp -Xs -R
echo "recovery_min_apply_delay = 0" >> "$PGDATA/postgresql.auto.conf"
chmod 0700 "$PGDATA"
echo "replica: starting as standby"
exec docker-entrypoint.sh postgres -c hot_standby=on -c track_commit_timestamp=on -c listen_addresses='*'
`

	replica, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          image,
			Entrypoint:     []string{"bash", "-c", replicaScript},
			Env:            map[string]string{"PGDATA": "/var/lib/postgresql/data"},
			ExposedPorts:   []string{"5432/tcp"},
			Networks:       []string{testNetwork.Name},
			NetworkAliases: map[string][]string{testNetwork.Name: {"replica"}},
			WaitingFor: wait.ForLog("database system is ready to accept read-only connections").
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, replica)

	replicaHostPort := hostPort(ctx, t, replica)

	c := &pgReplicaCluster{
		primary:    primary,
		replica:    replica,
		primaryURI: connURI(primaryHostPort),
		replicaURI: connURI(replicaHostPort),
	}

	// Sanity check: the replica must be in recovery and streaming.
	require.Eventually(t, func() bool {
		conn, err := pgx.Connect(ctx, c.replicaURI)
		if err != nil {
			return false
		}
		defer conn.Close(ctx)
		var inRecovery bool
		if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
			return false
		}
		return inRecovery
	}, 30*time.Second, 250*time.Millisecond, "replica never entered recovery/streaming")

	return c
}

// pauseReplica pauses WAL replay on the standby. Streaming continues, so new WAL
// is received but not applied: revisions committed on the primary after this call
// are provably invisible on the replica until resumeReplica.
func (c *pgReplicaCluster) pauseReplica(ctx context.Context, t testing.TB) {
	t.Helper()
	c.runOnReplica(ctx, t, "SELECT pg_wal_replay_pause()")
	require.Eventually(t, func() bool {
		return c.replayPaused(ctx, t)
	}, 5*time.Second, 100*time.Millisecond, "replica did not report replay paused")
}

// resumeReplica resumes WAL replay on the standby.
func (c *pgReplicaCluster) resumeReplica(ctx context.Context, t testing.TB) {
	t.Helper()
	c.runOnReplica(ctx, t, "SELECT pg_wal_replay_resume()")
}

func (c *pgReplicaCluster) replayPaused(ctx context.Context, t testing.TB) bool {
	t.Helper()
	conn, err := pgx.Connect(ctx, c.replicaURI)
	require.NoError(t, err)
	defer conn.Close(ctx)
	var paused bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_is_wal_replay_paused()").Scan(&paused))
	return paused
}

// waitForReplicaCaughtUp blocks until the replica has replayed all WAL the
// primary had generated as of the call.
func (c *pgReplicaCluster) waitForReplicaCaughtUp(ctx context.Context, t testing.TB) {
	t.Helper()

	primaryConn, err := pgx.Connect(ctx, c.primaryURI)
	require.NoError(t, err)
	defer primaryConn.Close(ctx)

	var target string
	require.NoError(t, primaryConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&target))

	require.Eventually(t, func() bool {
		conn, err := pgx.Connect(ctx, c.replicaURI)
		if err != nil {
			return false
		}
		defer conn.Close(ctx)
		var caughtUp bool
		if err := conn.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn() >= $1::pg_lsn", target).Scan(&caughtUp); err != nil {
			return false
		}
		return caughtUp
	}, 30*time.Second, 100*time.Millisecond, "replica never caught up to primary LSN %s", target)
}

func (c *pgReplicaCluster) runOnReplica(ctx context.Context, t testing.TB, sql string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, c.replicaURI)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, sql)
	require.NoError(t, err)
}

func execOrFail(ctx context.Context, t testing.TB, ctr testcontainers.Container, cmd []string) {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, cmd, tcexec.Multiplexed())
	require.NoError(t, err)
	if code != 0 {
		out, _ := io.ReadAll(reader)
		t.Fatalf("command %v failed with exit code %d: %s", cmd, code, string(out))
	}
}

func hostPort(ctx context.Context, t testing.TB, ctr testcontainers.Container) string {
	t.Helper()
	host, err := ctr.Host(ctx)
	require.NoError(t, err)
	mapped, err := ctr.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	return net.JoinHostPort(host, mapped.Port())
}

func connURI(hostPort string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		replClusterUser, replClusterPassword, hostPort, replClusterDB)
}

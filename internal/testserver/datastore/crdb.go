package datastore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	enableRangefeeds = `SET CLUSTER SETTING kv.rangefeed.enabled = true;`
)

// crdbTester is safe for concurrent use by tests.
type crdbTester struct {
	conn      *pgx.Conn // GUARDED_BY(connMutex)
	connMutex sync.Mutex
	hostname  string
	creds     string
	port      string
}

var _ RunningEngineForTest = (*crdbTester)(nil)

// RunCRDBForTesting returns a RunningEngineForTest for CRDB
func RunCRDBForTesting(t testing.TB, bridgeNetworkName string, crdbVersion string) *crdbTester {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	name := "crds-" + uuid.New().String()
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       name,
		Repository: "cockroachdb/cockroach",
		Tag:        "v" + crdbVersion,
		Cmd:        []string{"start-single-node", "--insecure", "--max-offset=50ms"},
		NetworkID:  bridgeNetworkName,
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	builder := &crdbTester{
		hostname: "localhost",
		creds:    "root:fake",
	}
	t.Cleanup(func() {
		builder.connMutex.Lock()
		defer builder.connMutex.Unlock()
		if builder.conn != nil {
			require.NoError(t, builder.conn.Close(t.Context()))
		}
		require.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort(fmt.Sprintf("%d/tcp", 26257))
	if bridgeNetworkName != "" {
		builder.hostname = name
		builder.port = "26257"
	} else {
		builder.port = port
	}

	uri := fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", builder.creds, port)
	require.NoError(t, pool.Retry(func() error {
		var err error
		ctx, cancelConnect := context.WithTimeout(t.Context(), dockerBootTimeout)
		defer cancelConnect()
		conn, err := pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		builder.connMutex.Lock()
		builder.conn = conn
		ctx, cancelRangeFeeds := context.WithTimeout(t.Context(), dockerBootTimeout)
		defer cancelRangeFeeds()
		_, err = builder.conn.Exec(ctx, enableRangefeeds)
		builder.connMutex.Unlock()
		return err
	}))

	return builder
}

// NewDatabase creates a database.
func (r *crdbTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	r.connMutex.Lock()
	defer r.connMutex.Unlock()
	_, err = r.conn.Exec(t.Context(), "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	connectStr := fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		r.creds,
		r.hostname,
		r.port,
		newDBName,
	)
	return connectStr
}

// NewDatastore creates a database and runs migrations on it.
func (r *crdbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	connectStr := r.NewDatabase(t)

	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(t.Context(), migrationDriver, migrate.Head, migrate.LiveRun))
	defer func() {
		migrationDriver.Close(t.Context())
	}()

	return initFunc("cockroachdb", connectStr)
}

// RunCRDBClusterForTesting starts a multi-node CockroachDB cluster for testing
// and returns a RunningEngineForTest connected to the first node.
func RunCRDBClusterForTesting(t testing.TB, numNodes int, crdbVersion string) *crdbTester {
	require.GreaterOrEqual(t, numNodes, 1, "cluster must have at least 1 node")

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	clusterID := uuid.New().String()[:8]
	networkName := "crdb-cluster-" + clusterID

	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
		Name:   networkName,
		Driver: "bridge",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Client.RemoveNetwork(network.ID)
	})

	// Build the join list: all node hostnames.
	nodeNames := make([]string, numNodes)
	for i := range numNodes {
		nodeNames[i] = fmt.Sprintf("crdb-%s-%d", clusterID, i)
	}
	joinList := ""
	for i, name := range nodeNames {
		if i > 0 {
			joinList += ","
		}
		joinList += name + ":26257"
	}

	// Start all nodes.
	resources := make([]*dockertest.Resource, 0, numNodes)
	for i := range numNodes {
		resource, err := pool.RunWithOptions(&dockertest.RunOptions{
			Name:       nodeNames[i],
			Repository: "cockroachdb/cockroach",
			Tag:        "v" + crdbVersion,
			Cmd: []string{
				"start",
				"--insecure",
				"--max-offset=50ms",
				"--join=" + joinList,
				"--advertise-addr=" + nodeNames[i],
			},
			NetworkID: network.ID,
		}, func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		})
		require.NoError(t, err)
		resources = append(resources, resource)
	}

	t.Cleanup(func() {
		for _, r := range resources {
			_ = pool.Purge(r)
		}
	})

	firstNodePort := resources[0].GetPort("26257/tcp")

	// Wait for cockroach init to succeed — retry until the RPC port is up.
	pool.MaxWait = 60 * time.Second
	require.NoError(t, pool.Retry(func() error {
		exitCode, err := resources[0].Exec([]string{
			"cockroach", "init", "--insecure", "--host=" + nodeNames[0] + ":26257",
		}, dockertest.ExecOptions{})
		if err != nil {
			return err
		}
		// Exit code 0 = success, 1 = already initialized (both fine).
		if exitCode != 0 && exitCode != 1 {
			return fmt.Errorf("cockroach init exited with code %d", exitCode)
		}
		return nil
	}))

	// Wait for SQL layer to be ready.
	builder := &crdbTester{
		hostname: "localhost",
		creds:    "root",
		port:     firstNodePort,
	}

	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(t.Context(), dockerBootTimeout)
		defer cancel()
		uri := fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", builder.creds, firstNodePort)
		conn, err := pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		builder.connMutex.Lock()
		builder.conn = conn
		_, err = conn.Exec(ctx, enableRangefeeds)
		builder.connMutex.Unlock()
		return err
	}))

	t.Cleanup(func() {
		builder.connMutex.Lock()
		defer builder.connMutex.Unlock()
		if builder.conn != nil {
			_ = builder.conn.Close(t.Context())
		}
	})

	return builder
}

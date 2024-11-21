//go:build docker
// +build docker

package datastore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	POSTGRES_TEST_USER            = "postgres"
	POSTGRES_TEST_PASSWORD        = "secret"
	POSTGRES_TEST_PORT            = "5432"
	POSTGRES_TEST_MAX_CONNECTIONS = "3000"
	PGBOUNCER_TEST_PORT           = "6432"
)

type container struct {
	hostHostname      string
	hostPort          string
	containerHostname string
	containerPort     string
}

type postgresTester struct {
	container
	hostConn             *pgx.Conn
	creds                string
	targetMigration      string
	pgbouncerProxy       *container
	useContainerHostname bool
}

// RunPostgresForTesting returns a RunningEngineForTest for postgres
func RunPostgresForTesting(t testing.TB, bridgeNetworkName string, targetMigration string, pgVersion string, enablePgbouncer bool) RunningEngineForTest {
	return RunPostgresForTestingWithCommitTimestamps(t, bridgeNetworkName, targetMigration, true, pgVersion, enablePgbouncer)
}

func RunPostgresForTestingWithCommitTimestamps(t testing.TB, bridgeNetworkName string, targetMigration string, withCommitTimestamps bool, pgVersion string, enablePgbouncer bool) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	bridgeSupplied := bridgeNetworkName != ""
	if enablePgbouncer && !bridgeSupplied {
		// We will need a network bridge if we're running pgbouncer
		bridgeNetworkName = createNetworkBridge(t, pool)
	}

	postgresContainerHostname := fmt.Sprintf("postgres-%s", uuid.New().String())

	cmd := []string{"-N", POSTGRES_TEST_MAX_CONNECTIONS}
	if withCommitTimestamps {
		cmd = append(cmd, "-c", "track_commit_timestamp=1")
	}

	postgres, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       postgresContainerHostname,
		Repository: "mirror.gcr.io/library/postgres",
		Tag:        pgVersion,
		Env: []string{
			"POSTGRES_USER=" + POSTGRES_TEST_USER,
			"POSTGRES_PASSWORD=" + POSTGRES_TEST_PASSWORD,
			// use md5 auth to align postgres and pgbouncer auth methods
			"POSTGRES_HOST_AUTH_METHOD=md5",
			"POSTGRES_INITDB_ARGS=--auth=md5",
		},
		ExposedPorts: []string{POSTGRES_TEST_PORT + "/tcp"},
		NetworkID:    bridgeNetworkName,
		Cmd:          cmd,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(postgres))
	})

	builder := &postgresTester{
		container: container{
			hostHostname:      "localhost",
			hostPort:          postgres.GetPort(POSTGRES_TEST_PORT + "/tcp"),
			containerHostname: postgresContainerHostname,
			containerPort:     POSTGRES_TEST_PORT,
		},
		creds:                POSTGRES_TEST_USER + ":" + POSTGRES_TEST_PASSWORD,
		targetMigration:      targetMigration,
		useContainerHostname: bridgeSupplied,
	}

	if enablePgbouncer {
		// if we are running with pgbouncer enabled then set it up
		builder.runPgbouncerForTesting(t, pool, bridgeNetworkName)
	}

	builder.hostConn = builder.initializeHostConnection(t, pool)

	return builder
}

func (b *postgresTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	_, err = b.hostConn.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	hostname, port := b.getHostnameAndPort()
	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		b.creds,
		hostname,
		port,
		newDBName,
	)
}

const (
	retryCount         = 4
	timeBetweenRetries = 1 * time.Second
)

func (b *postgresTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	for i := 1; i <= retryCount; i++ {
		connectStr := b.NewDatabase(t)

		migrationDriver, err := pgmigrations.NewAlembicPostgresDriver(context.Background(), connectStr, datastore.NoCredentialsProvider)
		if err == nil {
			ctx := context.WithValue(context.Background(), migrate.BackfillBatchSize, uint64(1000))
			require.NoError(t, pgmigrations.DatabaseMigrations.Run(ctx, migrationDriver, b.targetMigration, migrate.LiveRun))
			return initFunc("postgres", connectStr)
		}

		if i == retryCount {
			require.NoError(t, err, "got error when trying to create migration driver")
		}

		time.Sleep(time.Duration(i) * timeBetweenRetries)
	}

	require.Fail(t, "failed to create datastore for testing")
	return nil
}

func createNetworkBridge(t testing.TB, pool *dockertest.Pool) string {
	bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())
	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: bridgeNetworkName})

	require.NoError(t, err)
	t.Cleanup(func() {
		pool.Client.RemoveNetwork(network.ID)
	})

	return bridgeNetworkName
}

func (b *postgresTester) runPgbouncerForTesting(t testing.TB, pool *dockertest.Pool, bridgeNetworkName string) {
	uniqueID := uuid.New().String()
	pgbouncerContainerHostname := fmt.Sprintf("pgbouncer-%s", uniqueID)

	pgbouncer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       pgbouncerContainerHostname,
		Repository: "mirror.gcr.io/edoburu/pgbouncer",
		Tag:        "latest",
		Env: []string{
			"DB_USER=" + POSTGRES_TEST_USER,
			"DB_PASSWORD=" + POSTGRES_TEST_PASSWORD,
			"DB_HOST=" + b.containerHostname,
			"DB_PORT=" + b.containerPort,
			"LISTEN_PORT=" + PGBOUNCER_TEST_PORT,
			"DB_NAME=*",     // Needed to make pgbouncer okay with the randomly named databases generated by the test suite
			"AUTH_TYPE=md5", // use the same auth type as postgres
			"MAX_CLIENT_CONN=" + POSTGRES_TEST_MAX_CONNECTIONS,
		},
		ExposedPorts: []string{PGBOUNCER_TEST_PORT + "/tcp"},
		NetworkID:    bridgeNetworkName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(pgbouncer))
	})

	b.pgbouncerProxy = &container{
		hostHostname:      "localhost",
		hostPort:          pgbouncer.GetPort(PGBOUNCER_TEST_PORT + "/tcp"),
		containerHostname: pgbouncerContainerHostname,
		containerPort:     PGBOUNCER_TEST_PORT,
	}
}

func (b *postgresTester) initializeHostConnection(t testing.TB, pool *dockertest.Pool) (conn *pgx.Conn) {
	hostname, port := b.getHostHostnameAndPort()
	uri := fmt.Sprintf("postgresql://%s@%s:%s/?sslmode=disable", b.creds, hostname, port)
	err := pool.Retry(func() error {
		var err error
		ctx, cancelConnect := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancelConnect()
		conn, err = pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	return conn
}

func (b *postgresTester) getHostnameAndPort() (string, string) {
	// If a bridgeNetworkName is supplied then we will return the container
	// hostname and port that is resolvable from within the container network.
	// If bridgeNetworkName is not supplied then the hostname and port will be
	// resolvable from the host.
	if b.useContainerHostname {
		return b.getContainerHostnameAndPort()
	}
	return b.getHostHostnameAndPort()
}

func (b *postgresTester) getHostHostnameAndPort() (string, string) {
	if b.pgbouncerProxy != nil {
		return b.pgbouncerProxy.hostHostname, b.pgbouncerProxy.hostPort
	}
	return b.hostHostname, b.hostPort
}

func (b *postgresTester) getContainerHostnameAndPort() (string, string) {
	if b.pgbouncerProxy != nil {
		return b.pgbouncerProxy.containerHostname, b.pgbouncerProxy.containerPort
	}
	return b.containerHostname, b.containerPort
}

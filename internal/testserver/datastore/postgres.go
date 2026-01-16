package datastore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	PostgresTestUser           = "postgres"
	PostgresTestPassword       = "secret"
	PostgresTestPort           = "5432"
	PostgresTestMaxConnections = "3000"
	PgbouncerTestPort          = "6432"
)

type container struct {
	hostHostname      string
	hostPort          string
	containerHostname string
	containerPort     string
}

type postgresTester struct {
	container
	creds                string
	targetMigration      string
	pgbouncerProxy       *container
	pgContainer          testcontainers.Container
	useContainerHostname bool
}

// RunPostgresForTesting returns a RunningEngineForTest for postgres
func RunPostgresForTesting(t testing.TB, bridgeNetworkName string, targetMigration string, pgVersion string, enablePgbouncer bool) RunningEngineForTest {
	return RunPostgresForTestingWithCommitTimestamps(t, bridgeNetworkName, targetMigration, true, pgVersion, enablePgbouncer)
}

func RunPostgresForTestingWithCommitTimestamps(t testing.TB, bridgeNetworkName string, targetMigration string, withCommitTimestamps bool, pgVersion string, enablePgbouncer bool) RunningEngineForTest {
	ctx := context.Background()

	bridgeSupplied := bridgeNetworkName != ""
	var bridgeNetwork *testcontainers.DockerNetwork
	if enablePgbouncer && !bridgeSupplied {
		// We will need a network bridge if we're running pgbouncer
		bridgeNetworkName, bridgeNetwork = createNetworkBridge(t)
	}

	postgresContainerHostname := "postgres-" + uuid.New().String()

	cmd := []string{"-N", PostgresTestMaxConnections}
	if withCommitTimestamps {
		cmd = append(cmd, "-c", "track_commit_timestamp=1")
	}

	req := testcontainers.ContainerRequest{
		Name:         postgresContainerHostname,
		Image:        "mirror.gcr.io/library/postgres:" + pgVersion,
		ExposedPorts: []string{PostgresTestPort + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     PostgresTestUser,
			"POSTGRES_PASSWORD": PostgresTestPassword,
			// use md5 auth to align postgres and pgbouncer auth methods
			"POSTGRES_HOST_AUTH_METHOD": "md5",
			"POSTGRES_INITDB_ARGS":      "--auth=md5",
		},
		Cmd: cmd,
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(dockerBootTimeout),
	}

	if bridgeNetworkName != "" {
		req.Networks = []string{bridgeNetworkName}
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	mappedPort, err := postgres.MappedPort(ctx, PostgresTestPort)
	require.NoError(t, err)

	builder := &postgresTester{
		container: container{
			hostHostname:      "localhost",
			hostPort:          mappedPort.Port(),
			containerHostname: postgresContainerHostname,
			containerPort:     PostgresTestPort,
		},
		creds:                PostgresTestUser + ":" + PostgresTestPassword,
		targetMigration:      targetMigration,
		useContainerHostname: bridgeSupplied,
		pgContainer:          postgres,
	}

	t.Cleanup(func() {
		require.NoError(t, postgres.Terminate(ctx))
		if bridgeNetwork != nil {
			_ = bridgeNetwork.Remove(ctx)
		}
	})

	if enablePgbouncer {
		// if we are running with pgbouncer enabled then set it up
		builder.runPgbouncerForTesting(t, bridgeNetworkName)
	}

	return builder
}

func (b *postgresTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	ctx := context.Background()
	conn := b.initializeHostConnection(t)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	row := conn.QueryRow(ctx, "SELECT datname FROM pg_catalog.pg_database WHERE datname = $1", newDBName)
	var dbName string
	err = row.Scan(&dbName)
	require.NoError(t, err)
	require.Equal(t, newDBName, dbName)

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

		migrationDriver, err := pgmigrations.NewAlembicPostgresDriver(context.Background(), connectStr, datastore.NoCredentialsProvider, false)
		if err == nil {
			ctx := context.WithValue(context.Background(), migrate.BackfillBatchSize, uint64(1000))
			require.NoError(t, pgmigrations.DatabaseMigrations.Run(ctx, migrationDriver, b.targetMigration, migrate.LiveRun))
			return initFunc("postgres", connectStr)
		}

		if i == retryCount {
			require.NoError(t, err, "got error when trying to create migration driver")
		} else {
			t.Logf("failed to create migration driver: %v, retrying... %d", err, i)
		}

		time.Sleep(time.Duration(i) * timeBetweenRetries)
	}

	require.Fail(t, "failed to create datastore for testing")
	return nil
}

func createNetworkBridge(t testing.TB) (string, *testcontainers.DockerNetwork) {
	ctx := context.Background()
	bridgeNetworkName := "bridge-" + uuid.New().String()

	net, err := network.New(ctx, network.WithDriver("bridge"), network.WithLabels(map[string]string{
		"name": bridgeNetworkName,
	}))
	require.NoError(t, err)

	return net.Name, net
}

func (b *postgresTester) runPgbouncerForTesting(t testing.TB, bridgeNetworkName string) {
	ctx := context.Background()
	uniqueID := uuid.New().String()
	pgbouncerContainerHostname := "pgbouncer-" + uniqueID

	req := testcontainers.ContainerRequest{
		Name:         pgbouncerContainerHostname,
		Image:        "mirror.gcr.io/edoburu/pgbouncer:latest",
		ExposedPorts: []string{PgbouncerTestPort + "/tcp"},
		Env: map[string]string{
			"DB_USER":          PostgresTestUser,
			"DB_PASSWORD":      PostgresTestPassword,
			"DB_HOST":          b.containerHostname,
			"DB_PORT":          b.containerPort,
			"LISTEN_PORT":      PgbouncerTestPort,
			"DB_NAME":          "*",   // Needed to make pgbouncer okay with the randomly named databases generated by the test suite
			"AUTH_TYPE":        "md5", // use the same auth type as postgres
			"MAX_CLIENT_CONN":  PostgresTestMaxConnections,
		},
		WaitingFor: wait.ForListeningPort(PgbouncerTestPort + "/tcp").
			WithStartupTimeout(dockerBootTimeout),
	}

	if bridgeNetworkName != "" {
		req.Networks = []string{bridgeNetworkName}
	}

	pgbouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pgbouncer.Terminate(ctx))
	})

	mappedPort, err := pgbouncer.MappedPort(ctx, PgbouncerTestPort)
	require.NoError(t, err)

	b.pgbouncerProxy = &container{
		hostHostname:      "localhost",
		hostPort:          mappedPort.Port(),
		containerHostname: pgbouncerContainerHostname,
		containerPort:     PgbouncerTestPort,
	}
}

func (b *postgresTester) initializeHostConnection(t testing.TB) (conn *pgx.Conn) {
	hostname, port := b.getHostHostnameAndPort()
	uri := fmt.Sprintf("postgresql://%s@%s:%s/?sslmode=disable", b.creds, hostname, port)

	// Retry connection
	maxRetries := 10
	var err error
	for i := 0; i < maxRetries; i++ {
		ctx, cancelConnect := context.WithTimeout(context.Background(), dockerBootTimeout)
		conn, err = pgx.Connect(ctx, uri)
		cancelConnect()
		if err == nil {
			break
		}

		if i == maxRetries-1 {
			require.NoError(t, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

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

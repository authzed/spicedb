package datastore

import (
	"bytes"
	"context"
	_ "embed"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"

	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	// NOTE: this is used in this file but also duplicated in postgres.conf.
	PostgresTestMaxConnections = "3000"
	PgTestPass                 = "testpass"
	PgTestUser                 = "testuser"
)

type postgresTester struct {
	targetMigration string
	pgbouncerProxy  *postgres.PostgresContainer
	pgContainer     *postgres.PostgresContainer
}

// RunPostgresForTesting returns a RunningEngineForTest for postgres
func RunPostgresForTesting(t testing.TB, targetMigration string, pgVersion string, enablePgbouncer bool, opts ...testcontainers.ContainerCustomizer) RunningEngineForTest {
	return RunPostgresForTestingWithCommitTimestamps(t, targetMigration, true, pgVersion, enablePgbouncer, opts...)
}

func RunPostgresForTestingWithCommitTimestamps(t testing.TB, targetMigration string, withCommitTimestamps bool, pgVersion string, enablePgbouncer bool, opts ...testcontainers.ContainerCustomizer) RunningEngineForTest {
	t.Helper()

	builder := &postgresTester{
		targetMigration: targetMigration,
	}

	if enablePgbouncer {
		// if we are running with pgbouncer enabled then set it up
		builder.runPgbouncerForTesting(t, pgVersion, withCommitTimestamps, opts...)
	} else {
		builder.runPostgresForTesting(t, pgVersion, withCommitTimestamps, opts...)
	}

	return builder
}

func (b *postgresTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	ctx := t.Context()
	conn := b.initializeHostConnection(t)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	row := conn.QueryRow(ctx, "SELECT datname FROM pg_catalog.pg_database WHERE datname = $1", newDBName)
	var dbName string
	err = row.Scan(&dbName)
	require.NoError(t, err)
	require.Equal(t, newDBName, dbName)

	connURI, err := b.pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	if b.pgbouncerProxy != nil {
		connURI, err = b.pgbouncerProxy.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
	}

	// ConnectionString always references the container's default database;
	// point it at the database we just created instead.
	u, err := url.Parse(connURI)
	require.NoError(t, err)
	u.Path = "/" + newDBName

	return u.String()
}

func (b *postgresTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	t.Helper()
	ctx := context.WithValue(t.Context(), migrate.BackfillBatchSize, uint64(1000))

	var uri string

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		connectStr := b.NewDatabase(t)
		migrationDriver, err := pgmigrations.NewAlembicPostgresDriver(ctx, connectStr, datastore.NoCredentialsProvider, false)
		if !assert.NoError(collect, err) {
			return
		}
		defer func() {
			migrationDriver.Close(ctx)
		}()
		if !assert.NoError(collect, pgmigrations.DatabaseMigrations.Run(ctx, migrationDriver, b.targetMigration, migrate.LiveRun)) {
			return
		}
		uri = connectStr
	}, 5*time.Second, 500*time.Millisecond)

	return initFunc("postgres", uri)
}

// runPgbouncerForTesting stands up the network, the postgres container, and the pgbouncer container
// for a test run.
func (b *postgresTester) runPgbouncerForTesting(t testing.TB, pgVersion string, withCommitTimestamps bool, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx := t.Context()

	// set up the network for pgbouncer
	// NOTE: this does not conflict with additional networks supplied by opts
	testNetwork, err := network.New(ctx)
	testcontainers.CleanupNetwork(t, testNetwork)
	require.NoError(t, err)

	// set up the pg container
	configBytes := postgresConf
	if withCommitTimestamps {
		configBytes = postgresWithTimestampsConf
	}

	postgresOptions := make([]testcontainers.ContainerCustomizer, 0, len(opts)+4)
	postgresOptions = append(postgresOptions, 
		testcontainers.WithEnv(map[string]string{
			// use md5 auth to align postgres and pgbouncer auth methods
			"POSTGRES_HOST_AUTH_METHOD": "md5",
			"POSTGRES_INITDB_ARGS":      "--auth=md5",
		}),
		// contains the config for commit timestamps and max conns
		postgresConfOption(configBytes),
		network.WithNetwork([]string{"postgres"}, testNetwork),
		postgres.BasicWaitStrategies(),
	)
	postgresOptions = append(postgresOptions, opts...)

	image := "mirror.gcr.io/library/postgres:" + pgVersion
	pgContainer, err := postgres.Run(ctx, image,
		postgresOptions...
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, pgContainer)
	b.pgContainer = pgContainer

	pgConnURI, err := pgContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// set up the bouncer container
	// TODO: this needs to be the "external" thing, and it needs to be
	// talking to postgres on the internal network.
	// There also might need to be a separation between bouncer options and postgres options,
	// which would be an annoying pain.
	bouncerContainer, err := postgres.Run(ctx, "mirror.gcr.io/edoburu/pgbouncer:latest",
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithEnv(map[string]string{
			"DATABASE_URL":    pgConnURI,
			"DB_NAME":         "*", // Needed to make pgbouncer okay with the randomly named databases generated by the test suite
			"DB_PASSWORD":     PgTestPass,
			"DB_USER":         PgTestUser,
			"AUTH_TYPE":       "md5", // use the same auth type as postgres
			"MAX_CLIENT_CONN": PostgresTestMaxConnections,
		}),
		// TODO: these may need to change based on logs. see what this container actually logs.
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, bouncerContainer)

	require.NoError(t, err)
	b.pgbouncerProxy = bouncerContainer
}

//go:embed config/postgres.conf
var postgresConf []byte

//go:embed config/postgres-with-timestamps.conf
var postgresWithTimestampsConf []byte

// postgresConfOption is basically postgres.WithConfigFile but using the `Reader`
// interface on ContainerFile instead of `HostFilePath`, which is difficult to use
// when this code is invoked from different places.
func postgresConfOption(confBytes []byte) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		file := testcontainers.ContainerFile{
			Reader:            bytes.NewBuffer(confBytes),
			ContainerFilePath: "/etc/postgresql.conf",
			FileMode:          0o644,
		}
		if err := testcontainers.WithFiles(file)(req); err != nil {
			return err
		}

		return testcontainers.WithCmdArgs("-c", "config_file=/etc/postgresql.conf")(req)
	}
}

func (b *postgresTester) runPostgresForTesting(t testing.TB, pgVersion string, withCommitTimestamps bool, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx := t.Context()
	logger := log.TestLogger(t)
	configBytes := postgresConf
	if withCommitTimestamps {
		configBytes = postgresWithTimestampsConf
	}

	options := make([]testcontainers.ContainerCustomizer, 0, len(opts)+5)
	options = append(options, 
		testcontainers.WithLogger(logger),
		// contains the config for commit timestamps and max conns
		postgresConfOption(configBytes),
		postgres.WithUsername(PgTestUser),
		postgres.WithPassword(PgTestPass),
		postgres.BasicWaitStrategies(),
	)
	options = append(options, opts...)

	image := "mirror.gcr.io/library/postgres:" + pgVersion
	container, err := postgres.Run(ctx, image,
		options...
	)
	testcontainers.CleanupContainer(t, container)
	b.pgContainer = container
	require.NoError(t, err)
}

func (b *postgresTester) initializeHostConnection(t testing.TB) (conn *pgx.Conn) {
	t.Helper()
	ctx := t.Context()

	uri, err := b.pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	if b.pgbouncerProxy != nil {
		uri, err = b.pgbouncerProxy.ConnectionString(ctx, "sslmode=disable")
		require.NoError(t, err)
	}

	conn, err = pgx.Connect(ctx, uri)
	require.NoError(t, err)

	return conn
}

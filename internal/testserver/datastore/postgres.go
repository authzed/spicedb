package datastore

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

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
	t.Helper()
	uri, err := b.newDatabase(t.Context())
	require.NoError(t, err)
	return uri
}

// newDatabase creates a new database on the running postgres instance and
// returns its connection URI, reporting failures as errors so it is safe to
// call from retry loops and non-test goroutines.
func (b *postgresTester) newDatabase(ctx context.Context) (string, error) {
	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		return "", err
	}

	newDBName := "db" + uniquePortion

	connURI, err := b.hostConnectionString(ctx)
	if err != nil {
		return "", err
	}

	conn, err := pgx.Connect(ctx, connURI)
	if err != nil {
		return "", err
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "CREATE DATABASE "+newDBName); err != nil {
		return "", fmt.Errorf("creating database %s: %w", newDBName, err)
	}

	// ConnectionString always references the container's default database;
	// point it at the database we just created instead.
	u, err := url.Parse(connURI)
	if err != nil {
		return "", err
	}
	u.Path = "/" + newDBName

	return u.String(), nil
}

// hostConnectionString returns the URI for connecting to the datastore from
// the host, routed through pgbouncer when it is enabled.
func (b *postgresTester) hostConnectionString(ctx context.Context) (string, error) {
	if b.pgbouncerProxy != nil {
		return b.pgbouncerProxy.ConnectionString(ctx, "sslmode=disable")
	}
	return b.pgContainer.ConnectionString(ctx, "sslmode=disable")
}

func (b *postgresTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	t.Helper()
	ctx := context.WithValue(t.Context(), migrate.BackfillBatchSize, uint64(1000))

	var uri string

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		connectStr, err := b.newDatabase(ctx)
		if !assert.NoError(collect, err) {
			return
		}
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

	ds := initFunc("postgres", uri)

	// The generic datastore test suites do not close the datastores they
	// create. Close them when the owning test finishes; otherwise their
	// connections accumulate for the lifetime of the container and can
	// exhaust pgbouncer's max_client_conn.
	t.Cleanup(func() {
		if ds != nil {
			_ = ds.Close()
		}
	})

	return ds
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
		postgres.WithUsername(PgTestUser),
		postgres.WithPassword(PgTestPass),
		network.WithNetwork([]string{"postgres"}, testNetwork),
		postgres.BasicWaitStrategies(),
	)
	postgresOptions = append(postgresOptions, opts...)

	image := "mirror.gcr.io/library/postgres:" + pgVersion
	pgContainer, err := postgres.Run(ctx, image,
		postgresOptions...,
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, pgContainer)
	b.pgContainer = pgContainer

	// set up the bouncer container
	// NOTE: this is a bit of a bodge; a pgbouncer container is not the same as a postgres container,
	// so we have to undo some of what the postgres container wrapper is doing.
	bouncerContainer, err := postgres.Run(ctx, "mirror.gcr.io/edoburu/pgbouncer:latest",
		network.WithNetwork([]string{"pgbouncer"}, testNetwork),
		testcontainers.WithLogger(log.TestLogger(t)),
		postgres.WithUsername(PgTestUser),
		postgres.WithPassword(PgTestPass),
		testcontainers.WithEnv(map[string]string{
			"DB_NAME":             "*", // Needed to make pgbouncer okay with the randomly named databases generated by the test suite
			"DB_HOST":             "postgres",
			"DB_PORT":             "5432",
			"DB_USER":             PgTestUser,
			"DB_PASSWORD":         PgTestPass,
			"AUTH_TYPE":           "md5", // use the same auth type as postgres
			"MAX_CLIENT_CONN":     PostgresTestMaxConnections,
			"SERVER_IDLE_TIMEOUT": "10", // close idle conns after 10s
			"AUTODB_IDLE_TIMEOUT": "60", // reap unused wildcard-db pools after 60s instead of 60min
		}),
		// pgbouncer needs one fd per client conn plus one per server conn;
		// docker's default soft limit of 1024 starves it partway through a
		// suite run (it warns "max expected fd use: 3012" at startup).
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Ulimits = []*container.Ulimit{{Name: "nofile", Soft: 16384, Hard: 16384}}
		}),
		// NOTE: this is the original command from the pgbouncer container, which
		// the postgres testcontainers wrapper overwrites.
		testcontainers.WithCmd(
			"/usr/bin/pgbouncer",
			"/etc/pgbouncer/pgbouncer.ini",
		),
		// This is what pgbouncer logs out
		testcontainers.WithWaitStrategy(wait.ForLog("LOG process up")),
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
		options...,
	)
	testcontainers.CleanupContainer(t, container)
	b.pgContainer = container
	require.NoError(t, err)
}

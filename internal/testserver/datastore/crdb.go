package datastore

import (
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

// crdbTester is safe for concurrent use by tests.
type crdbTester struct {
	// endpoint is the host:port of the cockroach container.
	endpoint string
}

var _ RunningEngineForTest = (*crdbTester)(nil)

// RunCRDBForTesting returns a RunningEngineForTest for CRDB
func RunCRDBForTesting(t testing.TB, crdbVersion string, opts ...testcontainers.ContainerCustomizer) *crdbTester {
	ctx := t.Context()

	options := make([]testcontainers.ContainerCustomizer, 0, len(opts)+1)
	options = append(options, cockroachdb.WithInsecure())
	options = append(options, opts...)

	container, err := cockroachdb.Run(ctx,
		"mirror.gcr.io/cockroachdb/cockroach:v"+crdbVersion,
		options...,
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	code, _, err := container.Exec(ctx, []string{
		"cockroach", "sql",
		"--insecure",
		"-e", "SET CLUSTER SETTING kv.rangefeed.enabled = true;",
	})
	require.NoError(t, err)
	require.Equal(t, 0, code)

	// NOTE: we pass this around because we can't use the conn string in most cases;
	// the one returned by testcontainers depends on implicit pgx registration,
	// which we don't want to necessarily use.
	host, err := container.Host(ctx)
	require.NoError(t, err)
	mappedPort, err := container.MappedPort(ctx, "26257/tcp")
	require.NoError(t, err)

	return &crdbTester{
		endpoint: net.JoinHostPort(host, mappedPort.Port()),
	}
}

// dsn returns an (insecure) connection string for the given database. The
// module's ConnectionString returns a pgx-registered config name rather than a
// URL, so the DSN is built directly for the datastore.
func (r *crdbTester) dsn(dbName string) string {
	return fmt.Sprintf("postgres://root@%s/%s?sslmode=disable", r.endpoint, dbName)
}

// NewDatabase creates a new, empty logical database and returns its connection string.
func (r *crdbTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)
	newDBName := "db" + uniquePortion

	ctx := t.Context()
	conn, err := pgx.Connect(ctx, r.dsn("defaultdb"))
	require.NoError(t, err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	return r.dsn(newDBName)
}

// NewDatastore creates a database and runs migrations on it.
func (r *crdbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	connectStr := r.NewDatabase(t)

	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(t.Context(), migrationDriver, migrate.Head, migrate.LiveRun))
	t.Cleanup(func() {
		_ = migrationDriver.Close(t.Context())
	})

	return initFunc("cockroachdb", connectStr)
}

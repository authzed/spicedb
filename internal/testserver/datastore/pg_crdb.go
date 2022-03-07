package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type crdbPGDSBuilder struct {
	conn  *pgx.Conn
	creds string
	port  string
}

// NewCRDBPGBuilder returns a TestDatastoreBuilder for the postgres driver
// backed by a CRDB instance
func NewCRDBPGBuilder(t testing.TB) TestDatastoreBuilder {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(crdbContainer)
	require.NoError(t, err)

	builder := &crdbPGDSBuilder{
		creds: "root:fake",
	}
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	builder.port = resource.GetPort(fmt.Sprintf("%d/tcp", 26257))
	uri := fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", builder.creds, builder.port)
	require.NoError(t, pool.Retry(func() error {
		var err error
		builder.conn, err = pgx.Connect(context.Background(), uri)
		if err != nil {
			return err
		}
		return nil
	}))

	return builder
}

func (b *crdbPGDSBuilder) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	_, err = b.conn.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		b.creds,
		b.port,
		newDBName,
	)
	migrationDriver, err := pgmigrations.NewAlembicPostgresDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, pgmigrations.DatabaseMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun))

	return initFunc("postgres", connectStr)
}

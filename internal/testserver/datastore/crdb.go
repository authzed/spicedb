package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type crdbDSBuilder struct {
	conn  *pgx.Conn
	creds string
	port  string
}

var crdbContainer = &dockertest.RunOptions{
	Repository: "cockroachdb/cockroach",
	Tag:        "v21.1.3",
	Cmd:        []string{"start-single-node", "--insecure", "--max-offset=50ms"},
}

// NewCRDBBuilder returns a TestDatastoreBuilder for CRDB
func NewCRDBBuilder(t testing.TB) TestDatastoreBuilder {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(crdbContainer)
	require.NoError(t, err)

	builder := &crdbDSBuilder{
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

func (b *crdbDSBuilder) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
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
	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun))

	return initFunc("cockroachdb", connectStr)
}

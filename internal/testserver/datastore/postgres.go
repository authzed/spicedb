//go:build docker
// +build docker

package datastore

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	pgmigrations "github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type postgresTester struct {
	conn            *pgx.Conn
	mutex           sync.Mutex
	hostname        string
	port            string
	creds           string
	targetMigration string
}

// RunPostgresForTesting returns a RunningEngineForTest for postgres
func RunPostgresForTesting(t testing.TB, bridgeNetworkName string, targetMigration string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	name := fmt.Sprintf("postgres-%s", uuid.New().String())
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         name,
		Repository:   "postgres",
		Tag:          "13.8",
		Env:          []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=defaultdb"},
		ExposedPorts: []string{"5432/tcp"},
		NetworkID:    bridgeNetworkName,
		Cmd:          []string{"-c", "track_commit_timestamp=1"},
	})
	require.NoError(t, err)

	builder := &postgresTester{
		hostname:        "localhost",
		creds:           "postgres:secret",
		targetMigration: targetMigration,
		mutex:           sync.Mutex{},
	}
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort(fmt.Sprintf("%d/tcp", 5432))
	if bridgeNetworkName != "" {
		builder.hostname = name
		builder.port = "5432"
	} else {
		builder.port = port
	}

	uri := fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", builder.creds, port)
	require.NoError(t, pool.Retry(func() error {
		var err error
		ctx, cancelConnect := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancelConnect()
		builder.conn, err = pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		return nil
	}))
	return builder
}

func (b *postgresTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	b.mutex.Lock()
	defer b.mutex.Unlock()
	_, err = b.conn.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	require.NoError(t, err)

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		b.creds,
		b.hostname,
		b.port,
		newDBName,
	)
}

func (b *postgresTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	connectStr := b.NewDatabase(t)

	migrationDriver, err := pgmigrations.NewAlembicPostgresDriver(connectStr)
	require.NoError(t, err)
	ctx := context.WithValue(context.Background(), migrate.BackfillBatchSize, uint64(1000))
	require.NoError(t, pgmigrations.DatabaseMigrations.Run(ctx, migrationDriver, b.targetMigration, migrate.LiveRun))

	return initFunc("postgres", connectStr)
}

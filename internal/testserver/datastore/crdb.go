//go:build docker
// +build docker

package datastore

import (
	"context"
	"fmt"
	"testing"

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

type crdbTester struct {
	conn     *pgx.Conn
	hostname string
	creds    string
	port     string
}

// RunCRDBForTesting returns a RunningEngineForTest for CRDB
func RunCRDBForTesting(t testing.TB, bridgeNetworkName string, crdbVersion string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	name := fmt.Sprintf("crds-%s", uuid.New().String())
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       name,
		Repository: "mirror.gcr.io/cockroachdb/cockroach",
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
		require.NoError(t, builder.conn.Close(context.Background()))
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
		ctx, cancelConnect := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancelConnect()
		builder.conn, err = pgx.Connect(ctx, uri)
		if err != nil {
			return err
		}
		ctx, cancelRangeFeeds := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancelRangeFeeds()
		_, err = builder.conn.Exec(ctx, enableRangefeeds)
		return err
	}))

	return builder
}

func (r *crdbTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + uniquePortion

	_, err = r.conn.Exec(context.Background(), "CREATE DATABASE "+newDBName)
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

func (r *crdbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	connectStr := r.NewDatabase(t)

	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun))

	return initFunc("cockroachdb", connectStr)
}

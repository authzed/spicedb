//go:build docker
// +build docker

package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/mysql/version"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	mysqlPort    = 3306
	defaultCreds = "root:secret"
	testDBPrefix = "spicedb_test_"
)

type mysqlTester struct {
	db       *sql.DB
	hostname string
	creds    string
	port     string
	options  MySQLTesterOptions
}

// MySQLTesterOptions allows tweaking the behaviour of the builder for the MySQL datastore
type MySQLTesterOptions struct {
	Prefix                 string
	MigrateForNewDatastore bool
	UseV8                  bool
}

// RunMySQLForTesting returns a RunningEngineForTest for the mysql driver
// backed by a MySQL instance with RunningEngineForTest options - no prefix is added, and datastore migration is run.
func RunMySQLForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest {
	return RunMySQLForTestingWithOptions(t, MySQLTesterOptions{Prefix: "", MigrateForNewDatastore: true}, bridgeNetworkName)
}

// RunMySQLForTestingWithOptions returns a RunningEngineForTest for the mysql driver
// backed by a MySQL instance, while allowing options to be forwarded
func RunMySQLForTestingWithOptions(t testing.TB, options MySQLTesterOptions, bridgeNetworkName string) RunningEngineForTest {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	containerImageTag := version.MinimumSupportedMySQLVersion

	name := fmt.Sprintf("mysql-%s", uuid.New().String())
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       name,
		Repository: "mirror.gcr.io/library/mysql",
		Tag:        containerImageTag,
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
		// increase max connections (default 151) to accommodate tests using the same docker container
		Cmd:       []string{"--max-connections=500"},
		NetworkID: bridgeNetworkName,
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)

	builder := &mysqlTester{
		creds:   defaultCreds,
		options: options,
	}
	t.Cleanup(func() {
		require.NoError(t, builder.db.Close())
		require.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort(fmt.Sprintf("%d/tcp", mysqlPort))
	if bridgeNetworkName != "" {
		builder.hostname = name
		builder.port = fmt.Sprintf("%d", mysqlPort)
	} else {
		builder.port = port
	}

	dsn := fmt.Sprintf("%s@(localhost:%s)/mysql?parseTime=true", builder.creds, port)
	require.NoError(t, pool.Retry(func() error {
		var err error
		builder.db, err = sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		ctx, cancelPing := context.WithTimeout(context.Background(), dockerBootTimeout)
		defer cancelPing()
		err = builder.db.PingContext(ctx)
		if err != nil {
			return err
		}
		return nil
	}))

	return builder
}

func (mb *mysqlTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err, "Could not generate unique portion of db name: %s", err)
	dbName := testDBPrefix + uniquePortion
	tx, err := mb.db.Begin()
	require.NoError(t, err, "Could being transaction: %s", err)
	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err, "failed to create database %s: %s", dbName, err)
	err = tx.Commit()
	require.NoError(t, err, "failed to commit: %s", err)
	return fmt.Sprintf("%s@(%s:%s)/%s?parseTime=true", mb.creds, mb.hostname, mb.port, dbName)
}

func (mb *mysqlTester) runMigrate(t testing.TB, dsn string) {
	driver, err := migrations.NewMySQLDriverFromDSN(dsn, mb.options.Prefix, datastore.NoCredentialsProvider)
	require.NoError(t, err, "failed to create migration driver: %s", err)
	err = migrations.Manager.Run(context.Background(), driver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err, "failed to run migration: %s", err)
}

func (mb *mysqlTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	dsn := mb.NewDatabase(t)
	if mb.options.MigrateForNewDatastore {
		mb.runMigrate(t, dsn)
	}
	return initFunc("mysql", dsn)
}

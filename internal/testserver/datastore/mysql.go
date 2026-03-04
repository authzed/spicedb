package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/mysql/version"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	testDBPrefix = "spicedb_test_"
	// This is the db name used for the dsn that's used to generate initial connections.
	initialDB = "testdb"
)

type mysqlTester struct {
	// db is a connection used to create databases
	db *sql.DB
	// container is a reference to the mysql container instance
	container *mysql.MySQLContainer
	options   MySQLTesterOptions
}

// MySQLTesterOptions allows tweaking the behaviour of the builder for the MySQL datastore
type MySQLTesterOptions struct {
	Prefix                 string
	MigrateForNewDatastore bool
}

// RunMySQLForTesting returns a RunningEngineForTest for the mysql driver
// backed by a MySQL instance with RunningEngineForTest options - no prefix is added, and datastore migration is run.
func RunMySQLForTesting(t testing.TB) RunningEngineForTest {
	return RunMySQLForTestingWithOptions(t, MySQLTesterOptions{Prefix: "", MigrateForNewDatastore: true})
}

// RunMySQLForTestingWithOptions returns a RunningEngineForTest for the mysql driver
// backed by a MySQL instance, while allowing options to be forwarded
func RunMySQLForTestingWithOptions(t testing.TB, options MySQLTesterOptions) RunningEngineForTest {
	ctx := t.Context()

	image := "mirror.gcr.io/library/mysql:" + version.MinimumSupportedMySQLVersion
	container, err := mysql.Run(ctx,
		image,
		mysql.WithConfigFile("./config/mysql.cnf"),
		mysql.WithDatabase(initialDB),
	)
	require.NoError(t, err)

	builder := &mysqlTester{
		options:   options,
		container: container,
	}

	dsn, err := container.ConnectionString(ctx, "parseTime=true")
	require.NoError(t, err)
	builder.db, err = sql.Open("mysql", dsn)
	require.NoError(t, err)

	return builder
}

func (mb *mysqlTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err, "Could not generate unique portion of db name: %s", err)

	dbName := testDBPrefix + uniquePortion

	_, err = mb.db.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err, "failed to create database %s: %s", dbName, err)

	dsn, err := mb.container.ConnectionString(t.Context(), "parseTime=true")
	require.NoError(t, err)

	strings.Replace(dsn, initialDB, dbName, 1)

	return dsn
}

func (mb *mysqlTester) runMigrate(t testing.TB, dsn string) error {
	driver, err := migrations.NewMySQLDriverFromDSN(dsn, mb.options.Prefix, datastore.NoCredentialsProvider)
	if err != nil {
		return fmt.Errorf("failed to create mysql driver: %w", err)
	}
	defer driver.Close(t.Context())

	err = migrations.Manager.Run(context.Background(), driver, migrate.Head, migrate.LiveRun)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	return nil
}

func (mb *mysqlTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	var dsn string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		dsn = mb.NewDatabase(t)
		if mb.options.MigrateForNewDatastore {
			err := mb.runMigrate(t, dsn)
			assert.NoError(collect, err)
		}
	}, 5*time.Second, 500*time.Millisecond)
	return initFunc("mysql", dsn)
}

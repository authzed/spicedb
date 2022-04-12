package datastore

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

var mysqlContainerRunOpts = &dockertest.RunOptions{
	Repository: "mysql",
	Tag:        "5",
	Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
	// increase max connections (default 151) to accommodate tests using the same docker container
	Cmd: []string{"--max-connections=500"},
}

const (
	mysqlPort    = 3306
	defaultCreds = "root:secret"
	testDBPrefix = "spicedb_test_"
)

type mysqlBuilder struct {
	db      *sql.DB
	creds   string
	port    string
	options MySQLBuilderOptions
}

type MySQLBuilderOptions struct {
	Prefix  string
	Migrate bool
}

// NewMySQLBuilder returns a TestDatastoreBuilder for the mysql driver
// backed by a MySQL instance
func NewMySQLBuilder(t testing.TB) TestDatastoreBuilder {
	return NewMySQLBuilderWithOptions(t, MySQLBuilderOptions{Prefix: "", Migrate: true})
}

func NewMySQLBuilderWithOptions(t testing.TB, options MySQLBuilderOptions) TestDatastoreBuilder {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(mysqlContainerRunOpts)
	require.NoError(t, err)

	builder := &mysqlBuilder{
		creds:   defaultCreds,
		options: options,
	}
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	builder.port = resource.GetPort(fmt.Sprintf("%d/tcp", mysqlPort))
	dsn := fmt.Sprintf("%s@(localhost:%s)/mysql?parseTime=true", builder.creds, builder.port)
	require.NoError(t, pool.Retry(func() error {
		var err error
		builder.db, err = sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		err = builder.db.Ping()
		if err != nil {
			return err
		}
		return nil
	}))

	return builder
}

func (mb *mysqlBuilder) setupDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err, "Could not generate unique portion of db name: %s", err)
	dbName := testDBPrefix + uniquePortion
	tx, err := mb.db.Begin()
	require.NoError(t, err, "Could being transaction: %s", err)
	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err, "failed to create database %s: %s", dbName, err)
	err = tx.Commit()
	require.NoError(t, err, "failed to commit: %s", err)
	return dbName
}

func (mb *mysqlBuilder) runMigrate(t testing.TB, dsn string) {
	driver, err := migrations.NewMysqlDriverFromDSN(dsn, mb.options.Prefix)
	require.NoError(t, err, "failed to create migration driver: %s", err)
	err = migrations.Manager.Run(driver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err, "failed to run migration: %s", err)
	err = migrations.Manager.Run(driver, migrate.Head, migrate.LiveRun)
	require.NoError(t, err, "failed to run migration: %s", err)
}

func (mb *mysqlBuilder) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	dbName := mb.setupDatabase(t)
	dsn := fmt.Sprintf("%s@(localhost:%s)/%s?parseTime=true", mb.creds, mb.port, dbName)
	if mb.options.Migrate {
		mb.runMigrate(t, dsn)
	}
	return initFunc("mysql", dsn)
}

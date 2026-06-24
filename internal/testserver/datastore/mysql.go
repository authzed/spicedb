package datastore

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
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
	// We connect as root since the tests create and migrate arbitrary databases,
	// which the default non-root user is not granted privileges over.
	mysqlRootUser     = "root"
	mysqlRootPassword = "secret"
)

type mysqlTester struct {
	// db is a connection used to create databases
	db *sql.DB
	// endpoint is the host:port of the mysql container
	endpoint string
	options  MySQLTesterOptions
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
		// TODO
		mysql.WithConfigFile(configFilePath("mysql.cnf")),
		mysql.WithDatabase(initialDB),
		// Sets MYSQL_ROOT_PASSWORD so we can connect as root below.
		mysql.WithPassword(mysqlRootPassword),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	endpoint, err := container.PortEndpoint(ctx, "3306/tcp", "")
	require.NoError(t, err)

	builder := &mysqlTester{
		options:  options,
		endpoint: endpoint,
	}

	builder.db, err = sql.Open("mysql", builder.dsn(initialDB))
	require.NoError(t, err)

	return builder
}

// dsn returns a root connection string for the given database, including the
// parseTime=true parameter required by the MySQL datastore.
func (mb *mysqlTester) dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", mysqlRootUser, mysqlRootPassword, mb.endpoint, dbName)
}

func (mb *mysqlTester) NewDatabase(t testing.TB) string {
	uniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err, "Could not generate unique portion of db name: %s", err)

	dbName := testDBPrefix + uniquePortion

	_, err = mb.db.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err, "failed to create database %s: %s", dbName, err)

	return mb.dsn(dbName)
}

func (mb *mysqlTester) runMigrate(t testing.TB, dsn string) error {
	driver, err := migrations.NewMySQLDriverFromDSN(dsn, mb.options.Prefix, datastore.NoCredentialsProvider)
	if err != nil {
		return fmt.Errorf("failed to create mysql driver: %w", err)
	}
	defer driver.Close(t.Context())

	err = migrations.Manager.Run(t.Context(), driver, migrate.Head, migrate.LiveRun)
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

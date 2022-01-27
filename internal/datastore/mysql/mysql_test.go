//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

var testDBName = "spicedb_test"

type sqlTest struct {
	db         *sql.DB
	port       string
	dbName     string
	connectStr string
	cleanup    func()
}

var mysqlContainer = &dockertest.RunOptions{
	Repository: "mysql",
	Tag:        "5.6",
	Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
}

func TestMysqlMigration(t *testing.T) {
	req := require.New(t)
	creds := "root:secret"
	sqlTester := sqlTestSetup(req, mysqlContainer, creds, 3306)
	defer sqlTester.cleanup()

	_, err := sqlTester.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlTester.dbName))
	req.NoError(err, "unable to create database")

	runTestMigrations(req, sqlTester)
}

func sqlTestSetup(req *require.Assertions, containerOpts *dockertest.RunOptions, creds string, portNum uint16) *sqlTest {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(containerOpts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var db *sql.DB
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	if err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("mysql", fmt.Sprintf("%s@(localhost:%s)/mysql", creds, port))
		if err != nil {
			return fmt.Errorf("couldn't connect to docker: %w", err)
		}

		// `Select 1` succeeding signals that the Database is ready
		_, err = db.Exec("Select 1")
		if err != nil {
			return fmt.Errorf("couldn't validate docker/mysql readiness: %w", err)
		}

		return nil
	}); err != nil {
		req.NoError(err, fmt.Errorf("mysql database error: %w", err))
	}

	uniquePortion, err := secrets.TokenHex(4)
	req.NoError(err)
	dbName := testDBName + uniquePortion
	connectStr := fmt.Sprintf("%s@(localhost:%s)/%s", creds, port, dbName)

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{db: db, port: port, dbName: dbName, connectStr: connectStr, cleanup: cleanup}
}

// Test that a migration can run successfully.
// Pull out into a separate method, so it can be re-used in future tests as part of db setup.
func runTestMigrations(req *require.Assertions, sqlTester *sqlTest) {
	migrationDriver, err := migrations.NewMysqlDriver(sqlTester.connectStr)
	req.NoError(err, "unable to initialize migration engine")

	version, err := migrationDriver.Version()
	req.NoError(err)
	req.Equal("", version)

	err = migrations.DatabaseMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err, "unable to migrate database")

	version, err = migrationDriver.Version()
	req.NoError(err)
	req.Equal("initial", version)
}

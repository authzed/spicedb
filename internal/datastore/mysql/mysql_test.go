//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/alecthomas/units"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	migrations "github.com/authzed/spicedb/internal/datastore/mysql/migrations"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

var testdbName = "spicedb_test"

type sqlTest struct {
	db                        *sql.DB
	port                      string
	creds                     string
	splitAtEstimatedQuerySize units.Base2Bytes
	cleanup                   func()
}

var mysqlContainer = &dockertest.RunOptions{
	Repository: "mysql",
	Tag:        "5.6",
	Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
}

func TestMysqlMigration(t *testing.T) {
	req := require.New(t)
	creds := "root:secret"
	tester := newTester(mysqlContainer, creds, 3306)
	defer tester.cleanup()

	uniquePortion, err := secrets.TokenHex(4)
	req.NoError(err)
	dbName := testdbName + uniquePortion
	_, err = tester.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	req.NoError(err, "unable to create database")

	connectStr := fmt.Sprintf("%s@(localhost:%s)/%s", creds, tester.port, dbName)

	migrationDriver, err := migrations.NewMysqlDriver(connectStr)
	req.NoError(err, "unable to initialize migration engine")

	err = migrations.DatabaseMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err, "unable to migrate database")
}

func newTester(containerOpts *dockertest.RunOptions, creds string, portNum uint16) *sqlTest {
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
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	if err = pool.Retry(func() error {
		var err error
		_, err = db.Exec("Select 1")
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not validate mysql/docker connection readiness: %s", err)
	}

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{db: db, port: port, cleanup: cleanup, creds: creds, splitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize}
}

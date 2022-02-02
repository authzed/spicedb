//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

var (
	testDBPrefix = "spicedb_test"
	creds        = "root:secret"

	once              sync.Once
	containerResource *dockertest.Resource
	containerCleanup  func()
)

type sqlTest struct {
	connectStr string
}

var mysqlContainer = &dockertest.RunOptions{
	Repository: "mysql",
	Tag:        "latest",
	Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
}

func createMigrationDriver(connectStr string) (*migrations.MysqlDriver, error) {
	migrationDriver, err := migrations.NewMysqlDriver(connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	return migrationDriver, nil
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase(mysqlContainer, creds, 3306)

	migrationDriver, err := createMigrationDriver(connectStr)
	req.NoError(err)

	version, err := migrationDriver.Version()
	req.NoError(err)
	req.Equal("", version)

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	req.NoError(err)

	version, err = migrationDriver.Version()
	req.NoError(err)

	headVersion, err := migrations.Manager.HeadRevision()
	req.NoError(err)
	req.Equal(headVersion, version)
}

func setupDatabase(containerOpts *dockertest.RunOptions, creds string, portNum uint16) string {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// only bring up the container once
	once.Do(func() {
		containerResource, err = pool.RunWithOptions(containerOpts)
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}

		containerCleanup = func() {
			// When you're done, kill and remove the container
			if err := pool.Purge(containerResource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}
	})

	var db *sql.DB
	port := containerResource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	connectStr := fmt.Sprintf("%s@(localhost:%s)/mysql", creds, port)
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		log.Fatalf("couldn't open DB: %s", err)
	}
	defer db.Close() // we do not want this connection to stay open

	err = pool.Retry(func() error {
		var err error
		err = db.Ping()
		if err != nil {
			return fmt.Errorf("couldn't validate docker/mysql readiness: %w", err)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("mysql database error: %v", err)
	}

	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		log.Fatalf("Could not generate unique portion of db name: %s", err)
	}
	dbName := testDBPrefix + uniquePortion

	tx, err := db.Begin()
	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	if err != nil {
		log.Fatalf("failed to create database: %s: %s", dbName, err)
	}
	tx.Commit()

	connectStr = fmt.Sprintf("%s@(localhost:%s)/%s", creds, port, dbName)
	return connectStr
}

func TestMain(m *testing.M) {
	exitStatus := m.Run()
	// clean up container
	if containerResource != nil && containerCleanup != nil {
		containerCleanup()
	}
	os.Exit(exitStatus)
}

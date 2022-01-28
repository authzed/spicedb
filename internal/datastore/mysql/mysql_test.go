//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

var (
	testDBName = "spicedb_test"
	creds      = "root:secret"
)

type sqlTest struct {
	db                        *sql.DB
	port                      string
	dbName                    string
	creds                     string
	connectStr                string
	splitAtEstimatedQuerySize units.Base2Bytes
	cleanup                   func()
}

var mysqlContainer = &dockertest.RunOptions{
	Repository: "mysql",
	Tag:        "5.6",
	Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
}

func (st sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	_, err := createDbAndRunMigrations(st.db, st.dbName, st.connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to create database and run migrations: %w", err)
	}

	return NewMysqlDatastore(
		st.connectStr,
		// RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
		// GCWindow(gcWindow),
		// GCInterval(0*time.Second), // Disable auto GC
		// WatchBufferLength(watchBufferLength),
		// SplitAtEstimatedQuerySize(st.splitAtEstimatedQuerySize),
	)
}

func createDbAndRunMigrations(db *sql.DB, dbName string, connectStr string) (string, error) {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		return "", fmt.Errorf("unable to create database: %w", err)
	}

	migrationDriver, err := migrations.NewMysqlDriver(connectStr)
	if err != nil {
		return "", fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	version, err := migrationDriver.Version()
	if err != nil {
		return "", fmt.Errorf("unable to get starting migration version: %w", err)
	}
	if version == "" {
		err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
		if err != nil {
			return "", fmt.Errorf("unable to migrate database: %w", err)
		}
	}

	return migrationDriver.Version()
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	sqlTester := newTester(mysqlContainer, creds, 3306)
	defer sqlTester.cleanup()

	version, err := createDbAndRunMigrations(sqlTester.db, sqlTester.dbName, sqlTester.connectStr)
	req.NoError(err)
	req.Equal("initial", version)
}

// func TestMySqlDatastore(t *testing.T) {
// 	sqlTester := newTester(mysqlContainer, creds, 3306)
// 	defer sqlTester.cleanup()

// 	test.All(t, sqlTester)
// }

// func TestMySqlDatastoreWithSplit(t *testing.T) {
// 	// Set the split at a VERY small size, to ensure any WithUsersets queries are split.
// 	sqlTester := newTester(mysqlContainer, creds, 3306)
// 	sqlTester.splitAtEstimatedQuerySize = 1 // bytes
// 	defer sqlTester.cleanup()

// 	test.All(t, sqlTester)
// }

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
	db, err = sql.Open("mysql", fmt.Sprintf("%s@(localhost:%s)/mysql", creds, port))
	if err != nil {
		log.Fatalf("couldn't open DB: %s", err)
	}

	if err = pool.Retry(func() error {
		var err error
		err = db.Ping()
		if err != nil {
			return fmt.Errorf("couldn't validate docker/mysql readiness: %w", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("mysql database error: %v", err)
	}

	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		log.Fatalf("Could not generate unique portion of db name: %s", err)
	}

	dbName := testDBName + uniquePortion
	connectStr := fmt.Sprintf("%s@(localhost:%s)/%s", creds, port, dbName)

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{
		db:                        db,
		port:                      port,
		dbName:                    dbName,
		creds:                     creds,
		connectStr:                connectStr,
		cleanup:                   cleanup,
		splitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,
	}
}

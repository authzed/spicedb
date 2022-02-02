//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

const (
	mysqlPort    = 3306
	testDBPrefix = "spicedb_test"
	creds        = "root:secret"
)

var containerPort string

type sqlTest struct{}

func newTester() *sqlTest {
	return &sqlTest{}
}

func (st *sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	connectStr := setupDatabase()

	migrationDriver, err := createMigrationDriver(connectStr)
	if err != nil {
		panic(err)
	}

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		panic(err)
	}

	return NewMysqlDatastore(connectStr)
}

func createMigrationDriver(connectStr string) (*migrations.MysqlDriver, error) {
	migrationDriver, err := migrations.NewMysqlDriver(connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	return migrationDriver, nil
}

func TestMysqlDatastore(t *testing.T) {
	tester := newTester()

	// TODO: switch this to call test.All() once we added the remaining test support:
	// - TestRevisionFuzzing
	// - TestInvalidReads
	// - TestWatch
	// - TestWatchCancel
	t.Run("TestSimple", func(t *testing.T) { test.SimpleTest(t, tester) })
	t.Run("TestWritePreconditions", func(t *testing.T) { test.WritePreconditionsTest(t, tester) })
	t.Run("TestDeletePreconditions", func(t *testing.T) { test.DeletePreconditionsTest(t, tester) })
	t.Run("TestDeleteRelationships", func(t *testing.T) { test.DeleteRelationshipsTest(t, tester) })
	t.Run("TestNamespaceWrite", func(t *testing.T) { test.NamespaceWriteTest(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { test.NamespaceDeleteTest(t, tester) })
	t.Run("TestEmptyNamespaceDelete", func(t *testing.T) { test.EmptyNamespaceDeleteTest(t, tester) })
	t.Run("TestUsersets", func(t *testing.T) { test.UsersetsTest(t, tester) })
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	connectStr := setupDatabase()

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

func setupDatabase() string {
	var db *sql.DB
	connectStr := fmt.Sprintf("%s@(localhost:%s)/mysql", creds, containerPort)
	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		log.Fatalf("couldn't open DB: %s", err)
	}
	defer func() {
		err := db.Close() // we do not want this connection to stay open
		if err != nil {
			log.Fatalf("failed to close db: %s", err)
		}
	}()

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
	log.Printf("database created for test: %s", dbName)

	err = tx.Commit()
	if err != nil {
		log.Fatalf("failed to commit: %s", err)
	}

	return fmt.Sprintf("%s@(localhost:%s)/%s", creds, containerPort, dbName)
}

func TestMain(m *testing.M) {
	mysqlContainerRunOpts := &dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "latest",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		fmt.Printf("could not connect to docker: %s\n", err)
		os.Exit(1)
	}

	// only bring up the container once
	containerResource, err := pool.RunWithOptions(mysqlContainerRunOpts)
	if err != nil {
		fmt.Printf("could not start resource: %s\n", err)
		os.Exit(1)
	}

	containerCleanup := func() {
		// When you're done, kill and remove the container
		if err := pool.Purge(containerResource); err != nil {
			fmt.Printf("could not purge resource: %s\n", err)
			os.Exit(1)
		}
	}
	defer containerCleanup()

	containerPort = containerResource.GetPort(fmt.Sprintf("%d/tcp", mysqlPort))
	connectStr := fmt.Sprintf("%s@(localhost:%s)/mysql", creds, containerPort)

	db, err := sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("failed to open db: %s\n", err)
		os.Exit(1)
	}

	defer func() {
		err := db.Close() // we do not want this connection to stay open
		if err != nil {
			fmt.Printf("failed to close db: %s\n", err)
			os.Exit(1)
		}
	}()

	err = pool.Retry(func() error {
		var err error
		err = db.Ping()
		if err != nil {
			return fmt.Errorf("couldn't validate docker/mysql readiness: %w", err)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("mysql database error: %s\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

//go:build ci
// +build ci

package mysql

import (
	"database/sql"
	"fmt"
	"log"
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

var (
	testDBPrefix = "spicedb_test"
	creds        = "root:secret"
)

type sqlTest struct {
	connectStr string
	cleanup    func()
}

func (st *sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	sqlTester := newTester(mysqlContainer, creds, 3306)

	migrationDriver, err := createMigrationDriver(sqlTester.connectStr)
	if err != nil {
		panic(err)
	}

	err = migrations.Manager.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		panic(err)
	}

	return NewMysqlDatastore(sqlTester.connectStr)
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

func TestMysqlDatastore(t *testing.T) {
	tester := newTester(mysqlContainer, creds, 3306)
	defer tester.cleanup()

	t.Run("TestSimple", func(t *testing.T) { test.SimpleTest(t, tester) })
	//t.Run("TestRevisionFuzzing", func(t *testing.T) { RevisionFuzzingTest(t, tester) })
	t.Run("TestWritePreconditions", func(t *testing.T) { test.WritePreconditionsTest(t, tester) })
	t.Run("TestDeletePreconditions", func(t *testing.T) { test.DeletePreconditionsTest(t, tester) })
	t.Run("TestDeleteRelationships", func(t *testing.T) { test.DeleteRelationshipsTest(t, tester) })
	t.Run("TestInvalidReads", func(t *testing.T) { test.InvalidReadsTest(t, tester) })
	t.Run("TestNamespaceWrite", func(t *testing.T) { test.NamespaceWriteTest(t, tester) })
	t.Run("TestNamespaceDelete", func(t *testing.T) { test.NamespaceDeleteTest(t, tester) })
	t.Run("TestEmptyNamespaceDelete", func(t *testing.T) { test.EmptyNamespaceDeleteTest(t, tester) })
	//t.Run("TestWatch", func(t *testing.T) { WatchTest(t, tester) })
	//t.Run("TestWatchCancel", func(t *testing.T) { WatchCancelTest(t, tester) })
	t.Run("TestUsersets", func(t *testing.T) { test.UsersetsTest(t, tester) })
}

func TestMySQLMigrations(t *testing.T) {
	req := require.New(t)

	sqlTester := newTester(mysqlContainer, creds, 3306)
	defer sqlTester.cleanup()

	migrationDriver, err := createMigrationDriver(sqlTester.connectStr)
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

func newTester(containerOpts *dockertest.RunOptions, creds string, portNum uint16) *sqlTest {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		log.Fatalf("Could not generate unique portion of db name: %s", err)
	}
	dbName := testDBPrefix + uniquePortion

	// Setup the containter environment. From the mysql docker image docs:
	// MYSQL_DATABASE
	// This variable is optional and allows you to specify the name of a database to be created on image startup.
	// If a user/password was supplied (see below) then that user will be granted superuser access (corresponding to GRANT ALL) to this database.
	containerOpts.Env = append(containerOpts.Env, fmt.Sprintf("MYSQL_DATABASE=%s", dbName))

	resource, err := pool.RunWithOptions(containerOpts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	log.Print("container started")

	var db *sql.DB
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	connectStr := fmt.Sprintf("%s@(localhost:%s)/%s", creds, port, dbName)
	log.Printf("connecting over %s", connectStr)
	db, err = sql.Open("mysql", connectStr)
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

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{
		connectStr: connectStr,
		cleanup:    cleanup,
	}
}

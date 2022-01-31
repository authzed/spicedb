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

var (
	testDBPrefix = "spicedb_test"
	creds        = "root:secret"
)

type sqlTest struct {
	connectStr string
	cleanup    func()
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

	var db *sql.DB
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	connectStr := fmt.Sprintf("%s@(localhost:%s)/%s", creds, port, dbName)
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

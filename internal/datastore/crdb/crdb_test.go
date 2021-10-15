//go:build ci
// +build ci

package crdb

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/ory/dockertest/v3"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type sqlTest struct {
	conn    *pgx.Conn
	port    string
	creds   string
	cleanup func()
}

var crdbContainer = &dockertest.RunOptions{
	Repository: "cockroachdb/cockroach",
	Tag:        "v21.1.3",
	Cmd:        []string{"start-single-node", "--insecure", "--max-offset=50ms"},
}

func (st sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		return nil, err
	}

	newDBName := "db" + uniquePortion

	_, err = st.conn.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	if err != nil {
		return nil, fmt.Errorf("unable to create database: %w", err)
	}

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		st.creds,
		st.port,
		newDBName,
	)

	migrationDriver, err := migrations.NewCRDBDriver(connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	err = migrations.CRDBMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		return nil, fmt.Errorf("unable to migrate database: %w", err)
	}

	return NewCRDBDatastore(
		connectStr,
		GCWindow(gcWindow),
		RevisionQuantization(revisionFuzzingTimedelta),
		WatchBufferLength(watchBufferLength),
	)
}

func TestCRDBDatastore(t *testing.T) {
	tester := newTester(crdbContainer, "root:fake", 26257)
	defer tester.cleanup()

	test.All(t, tester)
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

	var conn *pgx.Conn
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	if err = pool.Retry(func() error {
		var err error
		conn, err = pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", creds, port))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	cleanup := func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	return &sqlTest{conn: conn, port: port, cleanup: cleanup, creds: creds}
}

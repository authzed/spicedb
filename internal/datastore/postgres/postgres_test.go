//go:build ci
// +build ci

package postgres

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/migrations"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

type sqlTest struct {
	dbpool                    *pgxpool.Pool
	port                      string
	creds                     string
	splitAtEstimatedQuerySize units.Base2Bytes
	cleanup                   func()
}

var postgresContainer = &dockertest.RunOptions{
	Repository: "postgres",
	Tag:        "9.6",
	Env:        []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=defaultdb"},
}

func (st sqlTest) New(revisionFuzzingTimedelta, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
	uniquePortion, err := secrets.TokenHex(4)
	if err != nil {
		return nil, err
	}

	newDBName := "db" + uniquePortion

	_, err = st.dbpool.Exec(context.Background(), "CREATE DATABASE "+newDBName)
	if err != nil {
		return nil, fmt.Errorf("unable to create database: %w", err)
	}

	connectStr := fmt.Sprintf(
		"postgres://%s@localhost:%s/%s?sslmode=disable",
		st.creds,
		st.port,
		newDBName,
	)

	migrationDriver, err := migrations.NewAlembicPostgresDriver(connectStr)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize migration engine: %w", err)
	}

	err = migrations.DatabaseMigrations.Run(migrationDriver, migrate.Head, migrate.LiveRun)
	if err != nil {
		return nil, fmt.Errorf("unable to migrate database: %w", err)
	}

	return NewPostgresDatastore(
		connectStr,
		RevisionFuzzingTimedelta(revisionFuzzingTimedelta),
		GCWindow(gcWindow),
		WatchBufferLength(watchBufferLength),
		SplitAtEstimatedQuerySize(st.splitAtEstimatedQuerySize),
	)
}

func TestPostgresDatastore(t *testing.T) {
	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	test.TestAll(t, tester)
}

func TestPostgresDatastoreWithSplit(t *testing.T) {
	// Set the split at a VERY small size, to ensure any WithUsersets queries are split.
	tester := newTester(postgresContainer, "postgres:secret", 5432)
	tester.splitAtEstimatedQuerySize = 1 // bytes
	defer tester.cleanup()

	test.TestAll(t, tester)
}

func BenchmarkPostgresQuery(b *testing.B) {
	req := require.New(b)

	tester := newTester(postgresContainer, "postgres:secret", 5432)
	defer tester.cleanup()

	ds, err := tester.New(0, 24*time.Hour, 1)
	req.NoError(err)

	_, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.QueryTuples(testfixtures.DocumentNS.Name, revision).Execute(context.Background())
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ObjectAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
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

	var dbpool *pgxpool.Pool
	port := resource.GetPort(fmt.Sprintf("%d/tcp", portNum))
	if err = pool.Retry(func() error {
		var err error
		dbpool, err = pgxpool.Connect(context.Background(), fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", creds, port))
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

	return &sqlTest{dbpool: dbpool, port: port, cleanup: cleanup, creds: creds, splitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize}
}

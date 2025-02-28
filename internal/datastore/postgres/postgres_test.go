//go:build ci && docker && postgres
// +build ci,docker,postgres

package postgres

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/postgres/version"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func postgresTestVersion() string {
	ver := os.Getenv("POSTGRES_TEST_VERSION")
	if ver != "" {
		return ver
	}

	return version.LatestTestedPostgresVersion
}

var postgresConfig = postgresTestConfig{"head", "", postgresTestVersion(), false}

func TestPostgresDatastore(t *testing.T) {
	testPostgresDatastore(t, postgresConfig)
}

func TestPostgresDatastoreWithoutCommitTimestamps(t *testing.T) {
	testPostgresDatastoreWithoutCommitTimestamps(t, postgresConfig)
}

func TestPostgresDatastoreGC(t *testing.T) {
	config := postgresConfig
	pgbouncerStr := ""
	if config.pgbouncer {
		pgbouncerStr = "pgbouncer-"
	}
	t.Run(fmt.Sprintf("%spostgres-gc-%s-%s-%s", pgbouncerStr, config.pgVersion, config.targetMigration, config.migrationPhase), func(t *testing.T) {
		t.Parallel()

		b := testdatastore.RunPostgresForTesting(t, "", config.targetMigration, config.pgVersion, config.pgbouncer)

		t.Run("GarbageCollection", createDatastoreTest(
			b,
			GarbageCollectionTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
		))

		t.Run("GarbageCollectionByTime", createDatastoreTest(
			b,
			GarbageCollectionByTimeTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
		))

		t.Run("ChunkedGarbageCollection", createDatastoreTest(
			b,
			ChunkedGarbageCollectionTest,
			RevisionQuantization(0),
			GCWindow(1*time.Millisecond),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(1),
			MigrationPhase(config.migrationPhase),
			WithRevisionHeartbeat(false),
		))
	})
}

func TestDefaultQueryExecMode(t *testing.T) {
	parsedConfig, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/dbname")
	require.NoError(t, err)
	require.Equal(t, parsedConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeCacheStatement)

	pgConfig := DefaultQueryExecMode(parsedConfig)
	require.Equal(t, pgConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeExec)
}

func TestDefaultQueryExecModeOverridden(t *testing.T) {
	parsedConfig, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/dbname?default_query_exec_mode=cache_statement")
	require.NoError(t, err)

	pgConfig := DefaultQueryExecMode(parsedConfig)
	require.Equal(t, pgConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeCacheStatement)
}

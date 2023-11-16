//go:build ci && docker && postgres
// +build ci,docker,postgres

package postgres

import (
	"fmt"
	"testing"
	"time"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
)

func TestPostgresDatastore(t *testing.T) {
	t.Parallel()

	testPostgresDatastore(t, postgresConfigs)
}

func TestPostgresDatastoreWithoutCommitTimestamps(t *testing.T) {
	t.Parallel()

	testPostgresDatastoreWithoutCommitTimestamps(t, postgresConfigs)
}

func TestPostgresDatastoreGC(t *testing.T) {
	for _, config := range postgresConfigs {
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
			))
		})
	}
}

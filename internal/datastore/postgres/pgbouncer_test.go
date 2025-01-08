//go:build ci && docker && pgbouncer
// +build ci,docker,pgbouncer

package postgres

import (
	"os"
	"testing"

	"github.com/authzed/spicedb/internal/datastore/postgres/version"
)

func pgbouncerTestVersion() string {
	ver := os.Getenv("POSTGRES_TEST_VERSION")
	if ver != "" {
		return ver
	}

	return version.LatestTestedPostgresVersion
}

var pgbouncerConfig = postgresTestConfig{"head", "", pgbouncerTestVersion(), true}

func TestPostgresWithPgBouncerDatastore(t *testing.T) {
	testPostgresDatastore(t, pgbouncerConfig)
}

func TestPostgresDatastoreWithPgBouncerWithoutCommitTimestamps(t *testing.T) {
	testPostgresDatastoreWithoutCommitTimestamps(t, pgbouncerConfig)
}

//go:build ci && docker && pgbouncer
// +build ci,docker,pgbouncer

package postgres

import (
	"testing"

	pgversion "github.com/authzed/spicedb/internal/datastore/postgres/version"

	"github.com/samber/lo"
)

var pgbouncerConfigs = lo.Map(
	[]string{pgversion.MinimumSupportedPostgresVersion, "14", "15", "16"},
	func(postgresVersion string, _ int) postgresConfig {
		return postgresConfig{"head", "", postgresVersion, true}
	},
)

func TestPostgresWithPgBouncerDatastore(t *testing.T) {
	testPostgresDatastore(t, pgbouncerConfigs)
}

func TestPostgresDatastoreWithPgBouncerWithoutCommitTimestamps(t *testing.T) {
	testPostgresDatastoreWithoutCommitTimestamps(t, pgbouncerConfigs)
}

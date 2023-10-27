//go:build ci && docker && postgres
// +build ci,docker,postgres

package postgres

import (
	"testing"
)

func TestPostgresDatastore(t *testing.T) {
	t.Parallel()

	testPostgresDatastore(t, postgresConfigs)
}

func TestPostgresDatastoreWithoutCommitTimestamps(t *testing.T) {
	t.Parallel()

	testPostgresDatastoreWithoutCommitTimestamps(t, postgresConfigs)
}

//go:build ci && docker && postgres
// +build ci,docker,postgres

package postgres

import (
	"testing"
)

func TestPostgresDatastore(t *testing.T) {
	testPostgresDatastore(t, postgresConfigs)
}

func TestPostgresDatastoreWithoutCommitTimestamps(t *testing.T) {
	testPostgresDatastoreWithoutCommitTimestamps(t, postgresConfigs)
}

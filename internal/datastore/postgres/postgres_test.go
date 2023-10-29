//go:build ci && docker && postgres
// +build ci,docker,postgres

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPostgresDatastore(t *testing.T) {
	t.Parallel()

	testPostgresDatastore(t, postgresConfigs)
}

func TestPostgresDatastoreWithoutCommitTimestamps(t *testing.T) {
	t.Parallel()

	testPostgresDatastoreWithoutCommitTimestamps(t, postgresConfigs)
}

func TestDefaultQueryExecMode(t *testing.T) {
	parsedConfig, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/dbname")
	require.NoError(t, err)
	require.Equal(t, parsedConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeCacheStatement)

	pgConfig := DefaultQueryExecMode(parsedConfig)
	require.Equal(t, pgConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeCacheDescribe)
}

func TestDefaultQueryExecModeOverridden(t *testing.T) {
	parsedConfig, err := pgxpool.ParseConfig("postgres://username:password@localhost:5432/dbname?default_query_exec_mode=cache_statement")
	require.NoError(t, err)

	pgConfig := DefaultQueryExecMode(parsedConfig)
	require.Equal(t, pgConfig.ConnConfig.DefaultQueryExecMode, pgx.QueryExecModeCacheStatement)
}

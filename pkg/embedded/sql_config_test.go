package embedded_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/embedded"
)

func TestSQLConfigurationValidation(t *testing.T) {
	ctx := context.Background()

	_, err := embedded.NewPostgresPermissions(ctx, nil, embedded.PostgresConfig{GCWindow: -time.Second})
	require.ErrorContains(t, err, "GC durations must not be negative")
	_, err = embedded.NewCRDBPermissions(ctx, nil, embedded.CRDBConfig{GCWindow: -time.Second})
	require.ErrorContains(t, err, "GC durations must not be negative")
	_, err = embedded.NewMySQLPermissions(ctx, nil, embedded.MySQLConfig{GCInterval: -time.Second})
	require.ErrorContains(t, err, "GC durations must not be negative")

	ds, err := memdb.NewMemdbDatastore(0, time.Second, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	_, err = embedded.NewPermissions(embedded.Config{Datastore: ds, MaxUpdatesPerWrite: -1})
	require.ErrorContains(t, err, "limits must not be negative")
	_, err = embedded.NewPermissions(embedded.Config{Datastore: ds, MaxRelationshipContextSize: -1})
	require.ErrorContains(t, err, "limits must not be negative")
	_, err = embedded.NewPostgresPermissions(ctx, nil, embedded.PostgresConfig{Permissions: embedded.Config{Datastore: ds}})
	require.ErrorContains(t, err, "Datastore conflicts with supplied pool")
	_, err = embedded.NewCRDBPermissions(ctx, nil, embedded.CRDBConfig{Permissions: embedded.Config{Datastore: ds}})
	require.ErrorContains(t, err, "Datastore conflicts with supplied pool")
	_, err = embedded.NewMySQLPermissions(ctx, nil, embedded.MySQLConfig{Permissions: embedded.Config{Datastore: ds}})
	require.ErrorContains(t, err, "Datastore conflicts with supplied pool")

	err = (embedded.PostgresConfig{}).MigrateIfNeeded(ctx, (*pgxpool.Pool)(nil))
	require.Error(t, err)
	err = (embedded.CRDBConfig{}).MigrateIfNeeded(ctx, (*pgxpool.Pool)(nil))
	require.Error(t, err)
	err = (embedded.MySQLConfig{}).MigrateIfNeeded(ctx, (*sql.DB)(nil))
	require.Error(t, err)

	// Exercise the callback guards without creating a datastore or SQL transaction.
	_, err = (*embedded.PostgresPermissions)(nil).WithPostgresTransaction(ctx, nil, nil)
	require.ErrorContains(t, err, "callback is required")
	_, err = (*embedded.CRDBPermissions)(nil).WithCRDBTransaction(ctx, nil, nil)
	require.ErrorContains(t, err, "callback is required")
	_, err = (*embedded.MySQLPermissions)(nil).WithMySQLTransaction(ctx, nil, nil)
	require.ErrorContains(t, err, "callback is required")

	// Nonzero GC options are accepted and flow as far as the pool validation.
	_, err = embedded.NewPostgresPermissions(ctx, nil, embedded.PostgresConfig{GCWindow: time.Second})
	require.Error(t, err)
	_, err = embedded.NewCRDBPermissions(ctx, nil, embedded.CRDBConfig{GCWindow: time.Second})
	require.Error(t, err)
	_, err = embedded.NewMySQLPermissions(ctx, nil, embedded.MySQLConfig{GCWindow: time.Second, GCInterval: time.Second})
	require.Error(t, err)
}

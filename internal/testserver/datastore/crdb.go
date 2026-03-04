package datastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

// crdbTester is safe for concurrent use by tests.
type crdbTester struct {
	databaseConnectStr string
}

var _ RunningEngineForTest = (*crdbTester)(nil)

// RunCRDBForTesting returns a RunningEngineForTest for CRDB
func RunCRDBForTesting(t testing.TB, crdbVersion string) *crdbTester {
	ctx := t.Context()

	container, err := cockroachdb.Run(ctx, "mirror.gcr.io/cockroachdb/cockroach:v"+crdbVersion,
		cockroachdb.WithInsecure(),
	)
	require.NoError(t, err)

	// enable changefeeds
	code, _, err := container.Exec(ctx, []string{
		"cockroach", "sql",
		"--insecure",
		"-e", "SET CLUSTER SETTING kv.rangefeed.enabled = true;",
	})
	require.NoError(t, err)
	require.Equal(t, 0, code)

	connUri, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	return &crdbTester{
		databaseConnectStr: connUri,
	}
}

func (r *crdbTester) NewDatabase(t testing.TB) string {
	// nothing to do; database was already created
	return r.databaseConnectStr
}

// NewDatastore creates a database and runs migrations on it.
func (r *crdbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	connectStr := r.databaseConnectStr

	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun))
	defer func() {
		migrationDriver.Close(context.Background())
	}()

	return initFunc("cockroachdb", connectStr)
}

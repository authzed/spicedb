package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/secrets"
)

// crdbTester is safe for concurrent use by tests.
type crdbTester struct {
	databaseConnectStr string
}

var _ RunningEngineForTest = (*crdbTester)(nil)

// RunCRDBForTesting returns a RunningEngineForTest for CRDB
func RunCRDBForTesting(t testing.TB, bridgeNetworkName string, crdbVersion string) *crdbTester {
	ctx := context.Background()
	name := "crds-" + uuid.New().String()

	containerReq := testcontainers.ContainerRequest{
		Name: name,
		Cmd:  []string{"--insecure", "--max-offset=50ms"},
	}

	if bridgeNetworkName != "" {
		containerReq.Networks = []string{bridgeNetworkName}
	}

	opts := []testcontainers.ContainerCustomizer{
		cockroachdb.WithInsecure(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: containerReq,
		}),
	}

	container, err := cockroachdb.Run(ctx, "mirror.gcr.io/cockroachdb/cockroach:v"+crdbVersion, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	// enable changefeeds
	code, _, _ := container.Exec(ctx, []string{
		"cockroach", "sql",
		"--insecure",
		"-e", "SET CLUSTER SETTING kv.rangefeed.enabled = true;",
	})
	require.Equal(t, 0, code)

	// create DB
	dbNameUniquePortion, err := secrets.TokenHex(4)
	require.NoError(t, err)

	newDBName := "db" + dbNameUniquePortion
	code, _, _ = container.Exec(ctx, []string{
		"cockroach", "sql",
		"--insecure",
		"-e", "CREATE DATABASE " + newDBName + ";",
	})
	require.Equal(t, 0, code)

	hostname := "localhost"
	creds := "root:fake"
	port := "26257"

	if bridgeNetworkName != "" {
		hostname = name
	} else {
		mappedPort, err := container.MappedPort(ctx, "26257")
		require.NoError(t, err)
		port = mappedPort.Port()
	}

	return &crdbTester{
		databaseConnectStr: fmt.Sprintf(
			"postgres://%s@%s:%s/%s?sslmode=disable",
			creds,
			hostname,
			port,
			newDBName,
		),
	}
}

func (r *crdbTester) NewDatabase(t testing.TB) string {
	// nothing to do; database was already created
	return r.databaseConnectStr
}

// NewDatastore creates a database and runs migrations on it.
func (r *crdbTester) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	t.Log("MAINE")
	t.Log(r.databaseConnectStr)
	connectStr := r.databaseConnectStr

	migrationDriver, err := crdbmigrations.NewCRDBDriver(connectStr)
	require.NoError(t, err)
	require.NoError(t, crdbmigrations.CRDBMigrations.Run(context.Background(), migrationDriver, migrate.Head, migrate.LiveRun))

	return initFunc("cockroachdb", connectStr)
}

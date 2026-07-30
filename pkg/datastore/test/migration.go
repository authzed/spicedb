package test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/grpchelpers"
	log "github.com/authzed/spicedb/internal/logging"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
	"github.com/authzed/spicedb/pkg/genutil"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
)

var migrationTestConfigs = map[string]func(t *testing.T) string{}

// RegisterMigrationTestConfig registers a config provider for a datastore
// engine defined outside this repository, enabling MigrationTest for it. The
// provider returns the URI of an empty database that has had no migrations
// applied
// The engine must be migratable, registered in datastore.BuilderForEngine, and its datastore must implement
// datastore.EngineIdentifiable.
func RegisterMigrationTestConfig(engineKey string, provider func(t *testing.T) string) {
	migrationTestConfigs[engineKey] = provider
}

// MigrationTest verifies every migration of the datastore engine, in order,
// against an empty, unmigrated database:
//  1. Each migration is applied one at a time, verifying that it applies
//     cleanly and that the recorded version advances to it.
//  2. Once the datastore reaches the engine's verifiable migration (the
//     earliest one the current code can operate against), SpiceDB's schema
//     service is served against it and a schema is written and read back
//     through the WriteSchema and ReadSchema APIs.
//  3. Before each remaining migration a new schema is written; after the
//     migration that schema must still be readable, verifying that each
//     migration preserves the data written through the API before it.
func MigrationTest(t *testing.T, tester DatastoreTester) {
	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 16)
	require.NoError(t, err)

	identifiable := datastore.UnwrapAs[datastore.EngineIdentifiable](ds)
	if identifiable == nil {
		t.Skip("datastore does not implement datastore.EngineIdentifiable")
		return
	}

	engineKey := identifiable.EngineName()
	require.NoError(t, ds.Close())
	t.Logf("running migration test for engine %q", engineKey)

	var datastoreURI string
	if provider, ok := migrationTestConfigs[engineKey]; ok {
		t.Logf("creating an empty test database via the registered migration test config")
		datastoreURI = provider(t)
	} else {
		if !slices.Contains(migration.Engines(), engineKey) {
			t.Skipf("engine %q is not migratable; register it via migration.RegisterMigratableEngine and RegisterMigrationTestConfig", engineKey)
		}
		t.Logf("creating an empty test database via testdatastore")
		datastoreURI = testdatastore.RunDatastoreEngine(t, engineKey).NewDatabase(t)
	}
	t.Logf("test database URI: %s", datastoreURI)

	migrationNames, err := migration.MigrationNames(engineKey)
	require.NoError(t, err)
	verifiableMigrationName, err := migration.VerifiableMigrationName(engineKey)
	require.NoError(t, err)
	firstVerifiable := slices.Index(migrationNames, verifiableMigrationName)
	require.GreaterOrEqualf(t, firstVerifiable, 0, "verifiable migration %q is not a known migration of engine %q", verifiableMigrationName, engineKey)
	t.Logf("engine %q has %d migrations: %q; the earliest the current code can operate against is %q", engineKey, len(migrationNames), migrationNames, verifiableMigrationName)

	migrationCfg := &migration.Config{
		DatastoreEngine: engineKey,
		DatastoreURI:    datastoreURI,
		Timeout:         5 * time.Minute,
		BatchSize:       1000,
	}

	var schemaClient v1.SchemaServiceClient
	step := 0
	for i, migrationName := range migrationNames {
		t.Logf("applying migration %d/%d: %q", i+1, len(migrationNames), migrationName)
		require.NoErrorf(t, migration.Run(t.Context(), migrationCfg, migrationName), "failed to apply migration %q", migrationName)
		version, err := migration.Version(t.Context(), migrationCfg)
		require.NoError(t, err)
		require.Equalf(t, migrationName, version, "datastore version did not advance to %q", migrationName)

		switch {
		case i < firstVerifiable:
			continue
		case i == firstVerifiable:
			t.Logf("reached verifiable migration %q; standing up a SpiceDB server", migrationName)
			schemaClient = startMigrationTestServer(t, engineKey, datastoreURI, migrationNames[i:])
		default:
			// The schema written before this migration must still be readable.
			t.Logf("verifying that the schema written before migration %q is still readable", migrationName)
			requireStepSchema(t, schemaClient, step)
		}

		step++
		writeStepSchema(t, schemaClient, step)
		requireStepSchema(t, schemaClient, step)
	}
}

// startMigrationTestServer opens the given database at the migrations applied
// so far, serves SpiceDB's schema service against it over an in-process
// connection, and returns a client for that service.
//
// The datastore is built through the engine's own registered builder rather
// than through pkg/cmd/datastore.NewDatastore, and only the schema service is
// served rather than the whole command server. Both keep this package free of
// pkg/cmd/datastore and pkg/cmd/server: an engine's tests drive this suite from
// the engine's own package, so a dependency on either would be an import cycle
// once pkg/cmd/datastore links the engines.
func startMigrationTestServer(t *testing.T, engineKey, datastoreURI string, allowedMigrations []string) v1.SchemaServiceClient {
	builder, ok := dsconfig.BuilderForEngine[engineKey]
	require.Truef(t, ok, "no datastore builder is registered for engine %q", engineKey)

	dsCfg := dsconfig.DefaultDatastoreConfig()
	dsCfg.Engine = engineKey
	dsCfg.URI = datastoreURI
	dsCfg.RevisionQuantization = 0
	dsCfg.RequestHedgingEnabled = false
	dsCfg.AllowedMigrations = allowedMigrations

	ds, err := builder(t.Context(), *dsCfg)
	require.NoError(t, err)

	dl := datalayer.NewDataLayer(ds)
	validator, err := genutil.NewProtoValidator()
	require.NoError(t, err)

	listener := bufconn.Listen(humanize.MiByte)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			datalayer.UnaryServerInterceptor(dl),
			consistency.UnaryServerInterceptor("migrationtest", consistency.TreatMismatchingTokensAsError),
		),
		grpc.ChainStreamInterceptor(
			datalayer.StreamServerInterceptor(dl),
			consistency.StreamServerInterceptor("migrationtest", consistency.TreatMismatchingTokensAsError),
		),
	)
	v1.RegisterSchemaServiceServer(srv, v1svc.NewSchemaServer(v1svc.SchemaServerConfig{
		CaveatTypeSet:       caveattypes.Default.TypeSet,
		ExpiringRelsEnabled: true,
	}, validator))

	go func() {
		if err := srv.Serve(listener); err != nil {
			log.Err(err).Msg("migration test schema service stopped serving")
		}
	}()

	conn, err := grpchelpers.NewBufferedClient(listener)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		srv.Stop()
		require.NoError(t, listener.Close())
		require.NoError(t, ds.Close())
	})
	return v1.NewSchemaServiceClient(conn)
}

// stepSchemaText returns a schema unique to the given step, so that a read
// after each migration can be attributed to the write that preceded it.
func stepSchemaText(step int) string {
	return fmt.Sprintf(`caveat is_public_step%d(public bool) {
	public
}

definition user {}

definition document_step%d {
	relation viewer: user with is_public_step%d
	permission view = viewer
}`, step, step, step)
}

// writeStepSchema writes the schema for the given step, replacing any previously written schema.
func writeStepSchema(t *testing.T, schemaClient v1.SchemaServiceClient, step int) {
	t.Helper()

	t.Logf("writing schema for step %d", step)
	_, err := schemaClient.WriteSchema(t.Context(), &v1.WriteSchemaRequest{
		Schema: stepSchemaText(step),
	})
	require.NoErrorf(t, err, "failed to write schema for step %d", step)
}

// requireStepSchema verifies that the schema written for the given step is readable.
func requireStepSchema(t *testing.T, schemaClient v1.SchemaServiceClient, step int) {
	t.Helper()

	resp, err := schemaClient.ReadSchema(t.Context(), &v1.ReadSchemaRequest{})
	require.NoErrorf(t, err, "failed to read schema at step %d", step)
	require.Containsf(t, resp.SchemaText, fmt.Sprintf("document_step%d", step), "read schema does not contain the definitions written for step %d", step)
	t.Logf("schema for step %d is readable", step)
}

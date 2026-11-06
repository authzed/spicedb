//go:build integration

package cmd

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	dscfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestSchemaMigrationPhases(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "cockroachdb", "spanner"} {
		t.Run(engine, func(t *testing.T) {
			running := testdatastore.RunDatastoreEngine(t, engine)
			pool := *dscfg.DefaultReadConnPool()
			pool.MinOpenConns = 0
			pool.MaxOpenConns = 2
			previous := "populate-schema-tables"
			if engine == "mysql" {
				previous = "identifiers_case_sensitive"
			}
			t.Run("empty installation", func(t *testing.T) {
				uri := running.NewDatabase(t)
				cfg := &migration.Config{DatastoreEngine: engine, DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
				require.NoError(t, migration.Run(t.Context(), cfg, migrate.Head))
				ds, err := dscfg.NewDatastore(t.Context(), dscfg.WithEngine(engine), dscfg.WithURI(uri), dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool), dscfg.WithEnableDatastoreMetrics(false))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, ds.Close()) })
				head, err := ds.HeadRevision(t.Context())
				require.NoError(t, err)
				_, err = ds.SnapshotReader(head.Revision).ReadStoredSchema(t.Context())
				require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
			})

			for _, tc := range []struct {
				name, latest  string
				mode          datalayer.SchemaMode
				migrationMode string
				want          string
			}{
				{"stale unified copy", "caveat after(flag bool) { flag } definition current {}", datalayer.SchemaModeReadLegacyWriteLegacy, "", "current"},
				{"empty legacy schema", "", datalayer.SchemaModeReadLegacyWriteLegacy, "", ""},
				{"unified only", "caveat after(flag bool) { flag } definition current {}", datalayer.SchemaModeReadNewWriteNew, "read-new-write-new", "current"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					uri := running.NewDatabase(t)
					cfg := &migration.Config{DatastoreEngine: engine, DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
					require.NoError(t, migration.Run(ctx, cfg, previous))
					ds, err := dscfg.NewDatastore(ctx,
						dscfg.WithEngine(engine), dscfg.WithURI(uri),
						dscfg.WithMigrationPhase("schema-write-both-read-old"),
						dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool), dscfg.WithEnableDatastoreMetrics(false), dscfg.WithRevisionQuantization(0),
					)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, ds.Close()) })
					ready, err := ds.ReadyState(ctx)
					require.NoError(t, err)
					require.True(t, ready.IsReady, ready.Message)

					promotion, err := dscfg.NewDatastore(ctx,
						dscfg.WithEngine(engine), dscfg.WithURI(uri),
						dscfg.WithMigrationPhase("schema-write-both-read-new"),
						dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool), dscfg.WithEnableDatastoreMetrics(false),
					)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, promotion.Close()) })
					ready, err = promotion.ReadyState(ctx)
					require.NoError(t, err)
					require.False(t, ready.IsReady, "unified reads must wait for the refresh migration")

					legacyRevision := writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteLegacy, "definition legacy_history {}")
					oldRevision := writeMigrationSchema(t, ds, datalayer.SchemaModeReadNewWriteBoth, "caveat before(flag bool) { flag } definition stale {}")
					before, err := ds.SnapshotReader(oldRevision).ReadStoredSchema(ctx)
					require.NoError(t, err)
					staleHash := datalayer.SchemaHash(before.Get().GetV1().GetSchemaHash())
					legacyUpdateRevision := writeMigrationSchema(t, ds, tc.mode, tc.latest)
					cfg.SchemaMode = tc.migrationMode
					require.NoError(t, migration.Run(ctx, cfg, migrate.Head))
					ready, err = promotion.ReadyState(ctx)
					require.NoError(t, err)
					require.True(t, ready.IsReady, ready.Message)
					require.NoError(t, migration.Run(ctx, cfg, migrate.Head))
					head, err := ds.HeadRevision(ctx)
					require.NoError(t, err)
					stored, err := ds.SnapshotReader(head.Revision).ReadStoredSchema(ctx)
					require.NoError(t, err)
					definitions := stored.Get().GetV1().GetNamespaceDefinitions()
					if tc.want == "" {
						require.Empty(t, stored.Get().GetV1().GetCaveatDefinitions())
					} else {
						require.Len(t, stored.Get().GetV1().GetCaveatDefinitions(), 1)
						require.Contains(t, stored.Get().GetV1().GetCaveatDefinitions(), "after")
					}
					if tc.want == "" {
						require.Empty(t, definitions)
					} else {
						require.Len(t, definitions, 1)
						require.Contains(t, definitions, tc.want)
					}
					historical, err := ds.SnapshotReader(oldRevision).ReadStoredSchema(ctx)
					require.NoError(t, err)
					require.Contains(t, historical.Get().GetV1().GetNamespaceDefinitions(), "stale")
					require.Contains(t, historical.Get().GetV1().GetCaveatDefinitions(), "before")
					if tc.mode != datalayer.SchemaModeReadNewWriteNew {
						newDL := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteBoth))
						for _, hash := range []datalayer.SchemaHash{datalayer.NoSchemaHashInLegacyZedToken, datalayer.NoSchemaHashForLegacyCursor} {
							reader, err := newDL.SnapshotReader(legacyRevision, hash).ReadSchema(ctx)
							require.NoError(t, err)
							_, found, err := reader.LookupTypeDefByName(ctx, "legacy_history")
							require.NoError(t, err)
							require.True(t, found, "pre-migration tokens retain their legacy snapshot")
						}
						// Optimized revisions can still expose a stale unified hash from before
						// refresh. The durable boundary must select legacy storage for them.
						reader, err := newDL.SnapshotReader(legacyUpdateRevision, staleHash).ReadSchema(ctx)
						require.NoError(t, err)
						definitions, err := reader.ListAllTypeDefinitions(ctx)
						require.NoError(t, err)
						if tc.want == "" {
							require.Empty(t, definitions)
						} else {
							require.Len(t, definitions, 1)
							require.Equal(t, tc.want, definitions[0].Definition.Name)
						}
					}
					if tc.mode == datalayer.SchemaModeReadNewWriteNew {
						// A persisted unified-only marker also preserves unhashed tokens if
						// a library later uses the normal dual-write reader.
						newDL := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteBoth))
						reader, err := newDL.SnapshotReader(legacyUpdateRevision, datalayer.NoSchemaHashInLegacyZedToken).ReadSchema(ctx)
						require.NoError(t, err)
						_, found, err := reader.LookupTypeDefByName(ctx, "current")
						require.NoError(t, err)
						require.True(t, found)
						legacy, err := ds.SnapshotReader(head.Revision).LegacyListAllNamespaces(ctx)
						require.NoError(t, err)
						require.Len(t, legacy, 1)
						require.Equal(t, "stale", legacy[0].Definition.Name)
					}
				})
			}
		})
	}
}

func writeMigrationSchema(t *testing.T, ds datastore.Datastore, mode datalayer.SchemaMode, text string) datastore.Revision {
	t.Helper()
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("schema"), SchemaString: text}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	definitions := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, definition := range compiled.CaveatDefinitions {
		definitions = append(definitions, definition)
	}
	for _, definition := range compiled.ObjectDefinitions {
		definitions = append(definitions, definition)
	}
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(mode))
	revision, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
		_, err := tx.WriteSchema(ctx, definitions, text, nil)
		return err
	})
	require.NoError(t, err)
	return revision
}

// A schema update already in flight must commit before the refresh takes its
// legacy snapshot, otherwise the refresh can replace a newer unified copy.
func TestPostgresSchemaRefreshWaitsForWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	running := testdatastore.RunDatastoreEngine(t, "postgres")
	uri := running.NewDatabase(t)
	cfg := &migration.Config{DatastoreEngine: "postgres", DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
	require.NoError(t, migration.Run(ctx, cfg, "populate-schema-tables"))
	ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine("postgres"), dscfg.WithURI(uri),
		dscfg.WithMigrationPhase("schema-write-both-read-old"), dscfg.WithEnableDatastoreMetrics(false))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	writeMigrationSchema(t, ds, datalayer.SchemaModeReadNewWriteBoth, "definition stale {}")
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("schema"), SchemaString: "definition current {}"}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	writerStarted := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	var started sync.Once
	dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadLegacyWriteBoth))
	go func() {
		_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
			if _, err := tx.WriteSchema(ctx, []datastore.SchemaDefinition{compiled.ObjectDefinitions[0]}, "definition current {}", nil); err != nil {
				return err
			}
			started.Do(func() { close(writerStarted) })
			select {
			case <-releaseWriter:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		writerDone <- err
	}()
	select {
	case <-writerStarted:
	case err := <-writerDone:
		t.Fatalf("schema writer ended before holding its transaction: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	migrationDone := make(chan error, 1)
	go func() { migrationDone <- migration.Run(ctx, cfg, migrate.Head) }()
	select {
	case err := <-migrationDone:
		t.Fatalf("refresh completed before the schema writer committed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseWriter)
	require.NoError(t, <-writerDone)
	require.NoError(t, <-migrationDone)
	head, err := ds.HeadRevision(ctx)
	require.NoError(t, err)
	stored, err := ds.SnapshotReader(head.Revision).ReadStoredSchema(ctx)
	require.NoError(t, err)
	require.Contains(t, stored.Get().GetV1().GetNamespaceDefinitions(), "current")
	require.NotContains(t, stored.Get().GetV1().GetNamespaceDefinitions(), "stale")
}

func TestPostgresSchemaRefreshBeforeFirstUnifiedWrite(t *testing.T) {
	for _, tc := range []struct {
		relaxed bool
		name    string
	}{
		{false, "current"}, {true, "current"}, {false, "stale"}, {true, "stale"},
	} {
		t.Run(fmt.Sprintf("relaxed=%v/name=%s", tc.relaxed, tc.name), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			running := testdatastore.RunDatastoreEngine(t, "postgres")
			uri := running.NewDatabase(t)
			cfg := &migration.Config{DatastoreEngine: "postgres", DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
			require.NoError(t, migration.Run(ctx, cfg, "populate-schema-tables"))
			ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine("postgres"), dscfg.WithURI(uri),
				dscfg.WithMigrationPhase("schema-write-both-read-old"), dscfg.WithRelaxedIsolationLevel(tc.relaxed), dscfg.WithEnableDatastoreMetrics(false))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ds.Close()) })
			writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteLegacy, "definition stale {}")
			compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("schema"), SchemaString: "definition " + tc.name + " {}"}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			writerStarted := make(chan struct{})
			releaseWriter := make(chan struct{})
			writerDone := make(chan error, 1)
			var started sync.Once
			attempts := 0
			dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadLegacyWriteBoth))
			go func() {
				_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
					attempts++
					schema, err := tx.ReadSchema(ctx)
					if err != nil {
						return err
					}
					if _, err := schema.ListAllSchemaDefinitions(ctx); err != nil {
						return err
					}
					started.Do(func() { close(writerStarted) })
					select {
					case <-releaseWriter:
					case <-ctx.Done():
						return ctx.Err()
					}
					_, err = tx.WriteSchema(ctx, []datastore.SchemaDefinition{compiled.ObjectDefinitions[0]}, "definition "+tc.name+" {}", nil)
					return err
				})
				writerDone <- err
			}()
			select {
			case <-writerStarted:
			case err := <-writerDone:
				t.Fatalf("schema writer ended before taking its snapshot: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.NoError(t, migration.Run(ctx, cfg, migrate.Head))
			close(releaseWriter)
			require.NoError(t, <-writerDone)
			require.Greater(t, attempts, 1, "writer must retry after its snapshot misses the refresh")
			head, err := ds.HeadRevision(ctx)
			require.NoError(t, err)
			stored, err := ds.SnapshotReader(head.Revision).ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.Len(t, stored.Get().GetV1().GetNamespaceDefinitions(), 1)
			require.Contains(t, stored.Get().GetV1().GetNamespaceDefinitions(), tc.name)
			conn, err := pgx.Connect(ctx, uri)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close(context.Background())) }()
			var liveVersions int
			require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(*) FROM schema_revision WHERE name = 'current' AND deleted_xid = '9223372036854775807'::xid8`).Scan(&liveVersions))
			require.Equal(t, 1, liveVersions, "refresh and concurrent writers must not create multiple live schema versions")
			require.NoError(t, conn.QueryRow(ctx, `SELECT COUNT(*) FROM schema WHERE name = 'unified_schema' AND deleted_xid = '9223372036854775807'::xid8`).Scan(&liveVersions))
			require.Equal(t, 1, liveVersions, "the small schema must have exactly one live chunk")
		})
	}
}

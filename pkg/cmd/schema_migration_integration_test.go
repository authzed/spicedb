//go:build integration

package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
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
			previous := schemaRefreshPreviousMigration(engine)
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
					// The migration job runs with the preparation phase, as the servers do.
					cfg.SchemaMode = tc.migrationMode
					cfg.MigrationPhase = "schema-write-both-read-old"
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

// schemaRefreshPreviousMigration is the migration the preparation phase runs at,
// immediately before the schema refresh.
func schemaRefreshPreviousMigration(engine string) string {
	switch engine {
	case "mysql":
		return "identifiers_case_sensitive"
	case "spanner":
		return "add-schema-revision-to-change-stream"
	default:
		return "populate-schema-tables"
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

// Every CockroachDB HeadRevision reads the schema_revision hash row, and a read
// that reaches an uncommitted write to it waits for the writer to commit. A
// refresh that finds unified storage already current must not write that row.
func TestCockroachSchemaRefreshLeavesCurrentUnifiedSchemaUntouched(t *testing.T) {
	ctx := t.Context()
	running := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	uri := running.NewDatabase(t)
	cfg := &migration.Config{DatastoreEngine: "cockroachdb", DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
	pool := *dscfg.DefaultReadConnPool()
	pool.MinOpenConns = 0
	pool.MaxOpenConns = 2
	require.NoError(t, migration.Run(ctx, cfg, "populate-schema-tables"))
	ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine("cockroachdb"), dscfg.WithURI(uri),
		dscfg.WithMigrationPhase("schema-write-both-read-old"), dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool), dscfg.WithEnableDatastoreMetrics(false))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteBoth, "definition current {}")

	conn, err := pgx.Connect(ctx, uri)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	const lastWrites = `SELECT
		(SELECT crdb_internal_mvcc_timestamp::STRING FROM schema_revision WHERE name = 'current'),
		(SELECT max(crdb_internal_mvcc_timestamp)::STRING FROM schema WHERE name = 'unified_schema')`
	var hashBefore, schemaBefore string
	require.NoError(t, conn.QueryRow(ctx, lastWrites).Scan(&hashBefore, &schemaBefore))

	cfg.MigrationPhase = "schema-write-both-read-old"
	require.NoError(t, migration.Run(ctx, cfg, migrate.Head))

	var hashAfter, schemaAfter string
	require.NoError(t, conn.QueryRow(ctx, lastWrites).Scan(&hashAfter, &schemaAfter))
	require.Equal(t, hashBefore, hashAfter, "the refresh must not rewrite a current schema hash")
	require.Equal(t, schemaBefore, schemaAfter, "the refresh must not rewrite current schema chunks")

	var marker []byte
	require.NoError(t, conn.QueryRow(ctx, `SELECT hash FROM schema_revision WHERE name = 'unified_schema_migration'`).Scan(&marker))
	require.Regexp(t, `^legacy:\d+\.\d+$`, string(marker))
}

// schemaRefreshStall holds a lock that the refresh migration needs only after it
// has written the unified schema, so the refresh waits with those writes
// uncommitted while the test reads.
type schemaRefreshStall struct {
	release func()
	stalled func(context.Context) (bool, error)
}

func stallSchemaRefresh(ctx context.Context, t *testing.T, engine, uri string) schemaRefreshStall {
	t.Helper()
	stall := heldSchemaRefresh(ctx, t, engine, uri)
	// Release on failure too, so the refresh does not wait out its timeout.
	release := sync.OnceFunc(stall.release)
	t.Cleanup(release)
	stall.release = release
	return stall
}

func heldSchemaRefresh(ctx context.Context, t *testing.T, engine, uri string) schemaRefreshStall {
	t.Helper()
	switch engine {
	case "mysql":
		// MySQL writes the migration version with ALTER TABLE, which commits the
		// refresh first, so stall the refresh's marker insert instead. A committed
		// sentinel row sorts between the hash rows and the marker, so the refresh's
		// locking scans of the hash rows stop at it, and only the marker insert
		// enters the gap held after it.
		db, err := sql.Open("mysql", uri)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		_, err = db.ExecContext(ctx, `INSERT INTO stored_schema_revision (name, hash, created_transaction) VALUES ('test_stall_sentinel', '', 0)`)
		require.NoError(t, err)
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		require.NoError(t, err)
		rows, err := tx.QueryContext(ctx, `SELECT name FROM stored_schema_revision WHERE name > 'test_stall_sentinel' FOR UPDATE`)
		require.NoError(t, err)
		require.False(t, rows.Next(), "nothing sorts after the sentinel")
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		return schemaRefreshStall{
			release: func() { require.NoError(t, tx.Rollback()) },
			stalled: func(ctx context.Context) (bool, error) {
				var waiting int
				err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.processlist
					WHERE info LIKE 'INSERT INTO stored_schema_revision (name, hash, created_transaction)%' AND time >= 1`).Scan(&waiting)
				return waiting > 0, err
			},
		}
	case "postgres":
		// The migration framework updates the version row in the refresh's own
		// transaction, after the refresh has written the unified schema.
		conn, observer := connectPair(ctx, t, uri)
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `SELECT version_num FROM alembic_version FOR UPDATE`)
		require.NoError(t, err)
		return schemaRefreshStall{
			release: func() { require.NoError(t, tx.Rollback(context.Background())) },
			stalled: func(ctx context.Context) (bool, error) {
				var waiting int
				err := observer.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
					WHERE wait_event_type = 'Lock' AND query LIKE 'UPDATE alembic_version%'`).Scan(&waiting)
				return waiting > 0, err
			},
		}
	case "cockroachdb":
		// CockroachDB makes plain reads wait on a locked version row, which would
		// stall the migration before it starts. Instead, hold an uncommitted
		// marker row: the refresh's batch writes the schema and its hash, then
		// waits to insert its own marker.
		conn, observer := connectPair(ctx, t, uri)
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `INSERT INTO schema_revision (name, hash) VALUES ('unified_schema_migration', 'held')`)
		require.NoError(t, err)
		return schemaRefreshStall{
			release: func() { require.NoError(t, tx.Rollback(context.Background())) },
			stalled: func(ctx context.Context) (bool, error) {
				var waiting int
				err := observer.QueryRow(ctx, `SELECT count(*) FROM crdb_internal.cluster_locks
					WHERE table_name = 'schema_revision' AND NOT granted`).Scan(&waiting)
				return waiting > 0, err
			},
		}
	default:
		require.FailNow(t, "no refresh stall for engine", engine)
		return schemaRefreshStall{}
	}
}

func connectPair(ctx context.Context, t *testing.T, uri string) (*pgx.Conn, *pgx.Conn) {
	t.Helper()
	conn, err := pgx.Connect(ctx, uri)
	require.NoError(t, err)
	observer, err := pgx.Connect(ctx, uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
		require.NoError(t, observer.Close(context.Background()))
	})
	return conn, observer
}

// Servers in the preparation phase keep serving while the refresh runs. None of
// their reads may wait on the refresh's uncommitted schema writes or locks.
func TestSchemaRefreshDoesNotBlockPreparationReads(t *testing.T) {
	// Spanner is excluded: its emulator runs one read-write transaction at a
	// time, and serving reads use lock-free read-only transactions.
	for _, engine := range []string{"postgres", "mysql", "cockroachdb"} {
		t.Run(engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			running := testdatastore.RunDatastoreEngine(t, engine)
			uri := running.NewDatabase(t)
			previous := schemaRefreshPreviousMigration(engine)
			cfg := &migration.Config{DatastoreEngine: engine, DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
			require.NoError(t, migration.Run(ctx, cfg, previous))

			pool := *dscfg.DefaultReadConnPool()
			pool.MinOpenConns = 0
			pool.MaxOpenConns = 4
			ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine(engine), dscfg.WithURI(uri),
				dscfg.WithMigrationPhase("schema-write-both-read-old"), dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool),
				dscfg.WithEnableDatastoreMetrics(false), dscfg.WithRevisionQuantization(0), dscfg.WithWriteAcquisitionTimeout(5*time.Second))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ds.Close()) })

			// Leave the unified copy stale so the refresh rewrites the hash row
			// that revision reads consult.
			writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteBoth, "definition stale {}")
			writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteLegacy, "definition current {}")

			stall := stallSchemaRefresh(ctx, t, engine, uri)
			cfg.MigrationPhase = "schema-write-both-read-old"
			migrated := make(chan error, 1)
			go func() { migrated <- migration.Run(ctx, cfg, migrate.Head) }()
			require.Eventually(t, func() bool {
				select {
				case err := <-migrated:
					require.FailNow(t, "the refresh finished without reaching the held lock", "%v", err)
				default:
				}
				stalled, err := stall.stalled(ctx)
				require.NoError(t, err)
				return stalled
			}, 30*time.Second, 50*time.Millisecond, "the refresh must reach the held lock")

			// The refresh's writes are now uncommitted. A read that waits on them
			// does not observe its context deadline, so bound each one directly.
			read := func(name string, f func(context.Context) error) {
				readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- f(readCtx) }()
				select {
				case err := <-done:
					require.NoError(t, err, "%s failed during the refresh", name)
				case <-time.After(5 * time.Second):
					require.FailNow(t, name+" waited on the refresh")
				}
			}
			dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadLegacyWriteBoth))
			var head datastore.Revision
			var hash datalayer.SchemaHash
			read("HeadRevision", func(ctx context.Context) (err error) {
				head, hash, err = dl.HeadRevision(ctx)
				return err
			})
			read("OptimizedRevision", func(ctx context.Context) error {
				_, _, err := dl.OptimizedRevision(ctx)
				return err
			})
			read("CheckRevision", func(ctx context.Context) error { return dl.CheckRevision(ctx, head) })
			read("ReadSchema", func(ctx context.Context) error {
				schema, err := dl.SnapshotReader(head, hash).ReadSchema(ctx)
				if err != nil {
					return err
				}
				definitions, err := schema.ListAllTypeDefinitions(ctx)
				if err != nil {
					return err
				}
				if len(definitions) != 1 || definitions[0].Definition.Name != "current" {
					return fmt.Errorf("unexpected legacy definitions %v", definitions)
				}
				return nil
			})
			read("QueryRelationships", func(ctx context.Context) error {
				it, err := ds.SnapshotReader(head).QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "current"})
				if err != nil {
					return err
				}
				_, err = datastore.IteratorToSlice(it)
				return err
			})

			stall.release()
			require.NoError(t, <-migrated)
			after, err := ds.HeadRevision(ctx)
			require.NoError(t, err)
			stored, err := ds.SnapshotReader(after.Revision).ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.Contains(t, stored.Get().GetV1().GetNamespaceDefinitions(), "current")
		})
	}
}

// The refresh follows the schema mode the installation's servers run: modes that
// read unified storage keep it, and modes that read legacy storage copy legacy
// storage over it. Without a configured mode, it refuses only where copying
// would replace a unified schema that differs from legacy storage.
func TestSchemaRefreshFollowsConfiguredMode(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "cockroachdb", "spanner"} {
		t.Run(engine, func(t *testing.T) {
			running := testdatastore.RunDatastoreEngine(t, engine)
			previous := schemaRefreshPreviousMigration(engine)
			for _, tc := range []struct {
				name, schemaMode, phase string
				unifiedOnly             bool
				wantErr                 bool
				wantDefinition          string
			}{
				{name: "preparation phase", phase: "schema-write-both-read-old", unifiedOnly: true, wantDefinition: "legacy"},
				{name: "legacy mode", schemaMode: "read-legacy-write-both", unifiedOnly: true, wantDefinition: "legacy"},
				{name: "unified only mode", schemaMode: "read-new-write-new", unifiedOnly: true, wantDefinition: "unified_only"},
				{name: "unified only mode in a phase", schemaMode: "read-new-write-new", phase: "schema-write-both-read-old", unifiedOnly: true, wantDefinition: "unified_only"},
				{name: "unified read mode", schemaMode: "read-new-write-both", unifiedOnly: true, wantDefinition: "unified_only"},
				{name: "unconfigured with differing schemas", unifiedOnly: true, wantErr: true, wantDefinition: "unified_only"},
				{name: "unconfigured without unified schema", wantDefinition: "legacy"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := t.Context()
					uri := running.NewDatabase(t)
					cfg := &migration.Config{DatastoreEngine: engine, DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
					require.NoError(t, migration.Run(ctx, cfg, previous))
					pool := *dscfg.DefaultReadConnPool()
					pool.MinOpenConns = 0
					pool.MaxOpenConns = 2
					ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine(engine), dscfg.WithURI(uri),
						dscfg.WithMigrationPhase("schema-write-both-read-old"), dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool),
						dscfg.WithEnableDatastoreMetrics(false), dscfg.WithRevisionQuantization(0), dscfg.WithWriteAcquisitionTimeout(5*time.Second))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, ds.Close()) })

					writeMigrationSchema(t, ds, datalayer.SchemaModeReadLegacyWriteLegacy, "definition legacy {}")
					if tc.unifiedOnly {
						writeMigrationSchema(t, ds, datalayer.SchemaModeReadNewWriteNew, "definition unified_only {}")
					}

					cfg.SchemaMode, cfg.MigrationPhase = tc.schemaMode, tc.phase
					err = migration.Run(ctx, cfg, migrate.Head)
					if tc.wantErr {
						require.ErrorIs(t, err, common.ErrSchemaRefreshModeRequired)
						ready, err := ds.ReadyState(ctx)
						require.NoError(t, err)
						require.True(t, ready.IsReady, "a refused refresh must leave the preparation revision in place")
					} else {
						require.NoError(t, err)
					}

					head, err := ds.HeadRevision(ctx)
					require.NoError(t, err)
					stored, err := ds.SnapshotReader(head.Revision).ReadStoredSchema(ctx)
					require.NoError(t, err)
					definitions := stored.Get().GetV1().GetNamespaceDefinitions()
					require.Len(t, definitions, 1)
					require.Contains(t, definitions, tc.wantDefinition)
				})
			}
		})
	}
}

// A watch resumed across promotion from before the boundary must report each
// schema change once, at the revision it happened, as a watch of legacy storage
// did. The refresh itself changes nothing that watch has not already seen.
func TestSchemaWatchAcrossRefresh(t *testing.T) {
	// MySQL does not support schema watches.
	for _, engine := range []string{"postgres", "cockroachdb", "spanner"} {
		t.Run(engine, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			running := testdatastore.RunDatastoreEngine(t, engine)
			uri := running.NewDatabase(t)
			cfg := &migration.Config{DatastoreEngine: engine, DatastoreURI: uri, Timeout: time.Minute, BatchSize: 1000}
			require.NoError(t, migration.Run(ctx, cfg, schemaRefreshPreviousMigration(engine)))

			pool := *dscfg.DefaultReadConnPool()
			pool.MinOpenConns = 0
			pool.MaxOpenConns = 4
			open := func(phase string) datastore.Datastore {
				ds, err := dscfg.NewDatastore(ctx, dscfg.WithEngine(engine), dscfg.WithURI(uri),
					dscfg.WithMigrationPhase(phase), dscfg.WithReadConnPool(pool), dscfg.WithWriteConnPool(pool),
					dscfg.WithEnableDatastoreMetrics(false), dscfg.WithRevisionQuantization(0), dscfg.WithWriteAcquisitionTimeout(5*time.Second))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, ds.Close()) })
				return ds
			}
			prepared := open("schema-write-both-read-old")

			startRevision := writeMigrationSchema(t, prepared, datalayer.SchemaModeReadLegacyWriteBoth, "definition before {}")
			legacyRevision := writeMigrationSchema(t, prepared, datalayer.SchemaModeReadLegacyWriteLegacy, "definition before {}\ndefinition legacy_only {}")

			cfg.MigrationPhase = "schema-write-both-read-old"
			require.NoError(t, migration.Run(ctx, cfg, migrate.Head))

			promoted := open("schema-write-both-read-new")
			promotedRevision := writeMigrationSchema(t, promoted, datalayer.SchemaModeReadNewWriteBoth, "definition before {}\ndefinition legacy_only {}\ndefinition after {}")

			type schemaEvent struct {
				revision datastore.Revision
				changed  []string
				deleted  []string
			}
			dl := datalayer.NewDataLayer(promoted, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteBoth))
			updates, errs := dl.Watch(ctx, startRevision, datastore.WatchOptions{Content: datastore.WatchSchema})
			var events []schemaEvent
			for !slices.ContainsFunc(events, func(e schemaEvent) bool { return e.revision.Equal(promotedRevision) }) {
				select {
				case update, ok := <-updates:
					require.True(t, ok, "the watch ended early")
					if len(update.ChangedDefinitions)+len(update.DeletedNamespaces)+len(update.DeletedCaveats) == 0 {
						continue
					}
					event := schemaEvent{revision: update.Revision, deleted: append(update.DeletedNamespaces, update.DeletedCaveats...)}
					for _, def := range update.ChangedDefinitions {
						event.changed = append(event.changed, def.GetName())
					}
					events = append(events, event)
				case err := <-errs:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for the promoted schema write", "events so far: %v", events)
				}
			}

			require.Len(t, events, 2, "events: %v", events)
			require.True(t, events[0].revision.Equal(legacyRevision), "the legacy change is reported at its own revision: %v", events)
			require.Equal(t, []string{"legacy_only"}, events[0].changed)
			require.Empty(t, events[0].deleted)
			require.Equal(t, []string{"after"}, events[1].changed)
			require.Empty(t, events[1].deleted)
		})
	}
}

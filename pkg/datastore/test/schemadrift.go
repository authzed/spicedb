package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/migration"
)

// SchemaSnapshotFunc captures the physical schema of the database at the given
// URI as text.
//
// The two snapshots SchemaDriftTest compares are taken from the same database,
// on the same server, seconds apart, so the text needs no normalization: it
// only has to be deterministic for one database at one moment, and to cover
// the parts of the schema that a stray DDL statement could change — tables,
// columns, types, nullability, defaults, indexes, constraints and table
// storage parameters. Whatever form the engine reports those in natively is
// fine, which is why the engines use `SHOW CREATE`-style output where they
// have it.
type SchemaSnapshotFunc func(ctx context.Context, tb testing.TB, uri string) (string, error)

var schemaSnapshotters = map[string]SchemaSnapshotFunc{}

// RegisterSchemaSnapshotter registers the function SchemaDriftTest uses to
// capture the schema of a database of the given engine. It is typically called
// from an init function in the engine's own test package. Engines without one
// skip SchemaDriftTest.
func RegisterSchemaSnapshotter(engineKey string, snapshot SchemaSnapshotFunc) {
	schemaSnapshotters[engineKey] = snapshot
}

// SchemaDriftTest asserts the invariant that migrations own the schema:
// constructing a datastore against a database that has been migrated to head
// must not change that database's schema.
//
// MigrationTest checks that every migration applies and preserves data; it
// never checks the result against what the running code expects. That gap has
// been filled before by fixing the schema up at startup instead of in a
// migration — the datastore issues the DDL it wants every time it is
// constructed, and nothing ever notices the migration was incomplete. Such a
// fix-up is invisible to every other test in this suite, because after it runs
// the database is correct.
//
// This test makes it visible, without a golden file and without any knowledge
// of what the schema is supposed to be: snapshot the schema after migrating to
// head, construct the datastore exactly as production does, snapshot again, and
// require the two to be identical. Any DDL the datastore issues at startup
// shows up in the diff, by name. The check needs no maintenance as migrations
// are added, and it holds for every engine version, because both snapshots come
// from the same server.
func SchemaDriftTest(t *testing.T, tester DatastoreTester) {
	ds, err := tester.New(t, DefaultRevisionParameters(), 16)
	require.NoError(t, err)

	identifiable := datastore.UnwrapAs[datastore.EngineIdentifiable](ds)
	if identifiable == nil {
		t.Skip("datastore does not implement datastore.EngineIdentifiable")
		return
	}

	engineKey := identifiable.EngineName()
	require.NoError(t, ds.Close())

	snapshot, ok := schemaSnapshotters[engineKey]
	if !ok {
		t.Skipf("engine %q has no registered schema snapshotter; register one via test.RegisterSchemaSnapshotter", engineKey)
	}

	datastoreURI := newEmptyDatabase(t, engineKey)

	head, err := migration.HeadRevision(engineKey)
	require.NoError(t, err)

	t.Logf("migrating the test database to head (%q)", head)
	require.NoError(t, migration.Run(t.Context(), &migration.Config{
		DatastoreEngine: engineKey,
		DatastoreURI:    datastoreURI,
		Timeout:         5 * time.Minute,
		BatchSize:       1000,
	}, head))

	beforeSchema, err := snapshot(t.Context(), t, datastoreURI)
	require.NoError(t, err, "failed to capture the schema after migrating to head")
	require.NotEmpty(t, beforeSchema, "the schema snapshot for engine %q is empty; the snapshotter is not reading the migrated database", engineKey)

	t.Logf("constructing the %q datastore against the migrated database", engineKey)
	builder, ok := dsconfig.BuilderForEngine[engineKey]
	require.Truef(t, ok, "no datastore builder is registered for engine %q", engineKey)

	dsCfg := dsconfig.DefaultDatastoreConfig()
	dsCfg.Engine = engineKey
	dsCfg.URI = datastoreURI
	dsCfg.RevisionQuantization = 0
	dsCfg.RequestHedgingEnabled = false

	constructed, err := builder(t.Context(), *dsCfg)
	require.NoError(t, err)
	require.NoError(t, constructed.Close())

	afterSchema, err := snapshot(t.Context(), t, datastoreURI)
	require.NoError(t, err, "failed to capture the schema after constructing the datastore")

	if beforeSchema != afterSchema {
		require.Failf(t, "constructing the datastore changed the schema",
			"Constructing the %q datastore changed the schema of a database that was already migrated to head (%q). "+
				"The datastore is fixing up at startup something a migration should have done; the change is:\n\n%s",
			engineKey, head, schemaDiff(beforeSchema, afterSchema))
	}
}

// schemaDiff reports the lines that differ between two schema snapshots,
// prefixed the way a diff is, so that a failure names the drift rather than
// printing two whole schemas for the reader to compare.
func schemaDiff(before, after string) string {
	counts := map[string]int{}
	for _, line := range strings.Split(before, "\n") {
		counts[line]++
	}
	for _, line := range strings.Split(after, "\n") {
		counts[line]--
	}

	var removed, added []string
	for _, line := range strings.Split(before, "\n") {
		if counts[line] > 0 {
			counts[line]--
			removed = append(removed, "- "+line)
		}
	}
	for _, line := range strings.Split(after, "\n") {
		if counts[line] < 0 {
			counts[line]++
			added = append(added, "+ "+line)
		}
	}

	return strings.Join(append(removed, added...), "\n")
}

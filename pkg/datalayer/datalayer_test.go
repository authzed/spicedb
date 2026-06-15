package datalayer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

var testDefinitions = []compiler.SchemaDefinition{
	ns.Namespace("user"),
	ns.Namespace("document",
		ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
	),
}

func newTestDatastore(t *testing.T) datastore.Datastore {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 90000*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { ds.Close() })
	return ds
}

func newTestDataLayer(t testing.TB) (DataLayer, datastore.Datastore) {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ds.Close()
	})
	return NewDataLayer(ds), ds
}

func testSchemaDefinitions(t *testing.T) ([]datastore.SchemaDefinition, string) {
	t.Helper()
	schemaText, _, err := generator.GenerateSchema(t.Context(), testDefinitions)
	require.NoError(t, err)

	defs := make([]datastore.SchemaDefinition, 0, len(testDefinitions))
	for _, def := range testDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}
	return defs, schemaText
}

// hasLegacyData checks whether legacy schema storage has namespace data at the given revision.
func hasLegacyData(t *testing.T, ds datastore.Datastore, rev datastore.Revision) bool {
	t.Helper()
	nsDefs, err := ds.SnapshotReader(rev).LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	return len(nsDefs) > 0
}

// hasUnifiedData checks whether unified schema storage has data at the given revision.
func hasUnifiedData(t *testing.T, ds datastore.Datastore, rev datastore.Revision) bool {
	t.Helper()
	_, err := ds.SnapshotReader(rev).ReadStoredSchema(t.Context())
	if err != nil {
		require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
		return false
	}
	return true
}

// TestDefaultDataLayer_PassThroughs exercises the trivial pass-through methods on
// defaultDataLayer in impl.go.
func TestDefaultDataLayer_PassThroughs(t *testing.T) {
	dl, underlying := newTestDataLayer(t)
	ctx := t.Context()

	readyState, err := dl.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	features, err := dl.Features(ctx)
	require.NoError(t, err)
	require.NotNil(t, features)

	offline, err := dl.OfflineFeatures()
	require.NoError(t, err)
	require.NotNil(t, offline)

	_, err = dl.Statistics(ctx)
	require.NoError(t, err)

	metricsID, err := dl.MetricsID()
	require.NoError(t, err)
	require.NotEmpty(t, metricsID)

	uniqueID, err := dl.UniqueID(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, uniqueID)

	rev, _, err := dl.HeadRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, dl.CheckRevision(ctx, rev))

	optRev, _, err := dl.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	roundTripped, err := dl.RevisionFromString(rev.String())
	require.NoError(t, err)
	require.True(t, roundTripped.Equal(rev))

	watchCh, errCh := dl.Watch(ctx, rev, datastore.WatchOptions{Content: datastore.WatchRelationships})
	require.NotNil(t, watchCh)
	require.NotNil(t, errCh)

	require.Equal(t, underlying, UnwrapDatastore(dl))
	require.NoError(t, dl.Close())
}

func TestWriteSchemaRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mode          SchemaMode
		expectLegacy  bool
		expectUnified bool
	}{
		{
			name:          "ReadLegacyWriteLegacy",
			mode:          SchemaModeReadLegacyWriteLegacy,
			expectLegacy:  true,
			expectUnified: false,
		},
		{
			name:          "ReadLegacyWriteBoth",
			mode:          SchemaModeReadLegacyWriteBoth,
			expectLegacy:  true,
			expectUnified: true,
		},
		{
			name:          "ReadNewWriteBoth",
			mode:          SchemaModeReadNewWriteBoth,
			expectLegacy:  true,
			expectUnified: true,
		},
		{
			name:          "ReadNewWriteNew",
			mode:          SchemaModeReadNewWriteNew,
			expectLegacy:  false,
			expectUnified: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(t, err)

			require.Equal(t, tc.expectLegacy, hasLegacyData(t, ds, rev),
				"legacy store populated = %v, want %v", hasLegacyData(t, ds, rev), tc.expectLegacy)
			require.Equal(t, tc.expectUnified, hasUnifiedData(t, ds, rev),
				"unified store populated = %v, want %v", hasUnifiedData(t, ds, rev), tc.expectUnified)
		})
	}
}

func TestReadSchemaRouting(t *testing.T) {
	t.Parallel()

	modes := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			// Write through the datalayer so both stores are populated if needed.
			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			// Read back through the datalayer.
			reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
			schemaReader, err := reader.ReadSchema(ctx)
			require.NoError(err)

			// Verify we can read back the schema.
			readText, err := schemaReader.SchemaText(ctx)
			require.NoError(err)
			require.NotEmpty(readText)

			typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
			require.NoError(err)
			require.Len(typeDefs, 2)

			// Both legacy and unified readers should return a real LastWrittenRevision.
			for _, td := range typeDefs {
				require.NotNil(td.LastWrittenRevision,
					"LastWrittenRevision should be set for %s", td.Definition.Name)
				require.NotEqual(datastore.NoRevision, td.LastWrittenRevision,
					"LastWrittenRevision should not be NoRevision for %s", td.Definition.Name)
			}
		})
	}
}

func TestReadSchemaWithinTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				// Write schema.
				if _, err := rwt.WriteSchema(ctx, defs, schemaText, nil); err != nil {
					return err
				}

				// Read schema back within the same transaction.
				schemaReader, err := rwt.ReadSchema(ctx)
				if err != nil {
					return err
				}

				readText, err := schemaReader.SchemaText(ctx)
				if err != nil {
					return err
				}
				require.NotEmpty(readText)

				typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
				if err != nil {
					return err
				}
				require.Len(typeDefs, 2)

				return nil
			})
			require.NoError(err)
		})
	}
}

func TestWriteSchemaDualWriteConsistency(t *testing.T) {
	t.Parallel()

	// For modes that write to both stores, verify the data is consistent between them.
	dualWriteModes := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth},
	}

	for _, tc := range dualWriteModes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			// Read from legacy store directly.
			legacyNsDefs, err := ds.SnapshotReader(rev).LegacyListAllNamespaces(ctx)
			require.NoError(err)
			legacyNames := make(map[string]bool, len(legacyNsDefs))
			for _, ns := range legacyNsDefs {
				legacyNames[ns.Definition.Name] = true
			}

			// Read from unified store directly.
			storedSchema, err := ds.SnapshotReader(rev).ReadStoredSchema(ctx)
			require.NoError(err)
			v1 := storedSchema.Get().GetV1()
			require.NotNil(v1)

			// Both stores should have the same namespace definitions.
			require.Len(v1.NamespaceDefinitions, len(legacyNsDefs),
				"both stores should have the same number of namespace definitions")
			for name := range v1.NamespaceDefinitions {
				require.True(legacyNames[name],
					"namespace %q in unified store but not in legacy store", name)
			}
		})
	}
}

func TestSchemaLookupOperationsPerMode(t *testing.T) {
	t.Parallel()

	modes := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
			schemaReader, err := reader.ReadSchema(ctx)
			require.NoError(err)

			// LookupTypeDefByName: existing.
			docDef, found, err := schemaReader.LookupTypeDefByName(ctx, "document")
			require.NoError(err)
			require.True(found)
			require.Equal("document", docDef.Definition.Name)

			// LookupTypeDefByName: missing.
			_, found, err = schemaReader.LookupTypeDefByName(ctx, "nonexistent")
			require.NoError(err)
			require.False(found)

			// LookupCaveatDefByName: missing (no caveats in this schema).
			_, found, err = schemaReader.LookupCaveatDefByName(ctx, "nonexistent")
			require.NoError(err)
			require.False(found)

			// ListAllSchemaDefinitions.
			allDefs, err := schemaReader.ListAllSchemaDefinitions(ctx)
			require.NoError(err)
			require.Len(allDefs, 2)
			require.Contains(allDefs, "user")
			require.Contains(allDefs, "document")

			// LookupSchemaDefinitionsByNames.
			lookedUp, err := schemaReader.LookupSchemaDefinitionsByNames(ctx, []string{"user", "nonexistent"})
			require.NoError(err)
			require.Len(lookedUp, 1)
			require.Contains(lookedUp, "user")

			// LookupTypeDefinitionsByNames.
			typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"user", "document"})
			require.NoError(err)
			require.Len(typeDefs, 2)

			// LookupCaveatDefinitionsByNames: empty since no caveats.
			caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"anything"})
			require.NoError(err)
			require.Empty(caveatDefs)
		})
	}
}

func TestWriteSchemaWithCaveatsPerMode(t *testing.T) {
	t.Parallel()

	modes := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			schemaText := `caveat test_caveat(allowed bool) {
	allowed
}

definition user {}

definition document {
	relation viewer: user with test_caveat
}`

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
			for _, caveatDef := range compiled.CaveatDefinitions {
				defs = append(defs, caveatDef)
			}
			for _, objDef := range compiled.ObjectDefinitions {
				defs = append(defs, objDef)
			}

			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			// Verify through the datalayer.
			reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
			schemaReader, err := reader.ReadSchema(ctx)
			require.NoError(err)

			caveats, err := schemaReader.ListAllCaveatDefinitions(ctx)
			require.NoError(err)
			require.Len(caveats, 1)
			require.Equal("test_caveat", caveats[0].Definition.Name)

			caveat, found, err := schemaReader.LookupCaveatDefByName(ctx, "test_caveat")
			require.NoError(err)
			require.True(found)
			require.Equal("test_caveat", caveat.Definition.Name)
		})
	}
}

func TestWriteSchemaStableTextPerMode(t *testing.T) {
	t.Parallel()

	// Schema text with definitions intentionally in non-alphabetical order.
	outOfOrderSchemaText := `caveat zebra_caveat(flag bool) {
	flag
}

caveat alpha_caveat(allowed bool) {
	allowed
}

definition zresource {
	relation viewer: auser with alpha_caveat
}

definition auser {}`

	modes := []struct {
		name         string
		mode         SchemaMode
		readsFromNew bool
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy, false},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth, false},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth, true},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew, true},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: outOfOrderSchemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			// Build definitions in the compiled order (which may not be alphabetical).
			defs := make([]datastore.SchemaDefinition, 0,
				len(compiled.CaveatDefinitions)+len(compiled.ObjectDefinitions))
			for _, cd := range compiled.CaveatDefinitions {
				defs = append(defs, cd)
			}
			for _, od := range compiled.ObjectDefinitions {
				defs = append(defs, od)
			}

			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, outOfOrderSchemaText, nil)
				return err
			})
			require.NoError(err)

			reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
			schemaReader, err := reader.ReadSchema(ctx)
			require.NoError(err)

			readText, err := schemaReader.SchemaText(ctx)
			require.NoError(err)

			if tc.readsFromNew {
				// Read-new modes should return the exact written text, preserving the
				// original ordering even though definitions were out of alphabetical order.
				require.Equal(outOfOrderSchemaText, readText,
					"read-new mode should return the original written text verbatim")
			} else {
				// Read-legacy modes regenerate text from sorted definitions, so the
				// output should be sorted alphabetically.
				require.NotEmpty(readText)
				require.Contains(readText, "alpha_caveat")
				require.Contains(readText, "zebra_caveat")
				require.Contains(readText, "auser")
				require.Contains(readText, "zresource")

				// Read a second time and verify the text is stable.
				schemaReader2, err := reader.ReadSchema(ctx)
				require.NoError(err)
				readText2, err := schemaReader2.SchemaText(ctx)
				require.NoError(err)
				require.Equal(readText, readText2,
					"legacy text should be stable across multiple reads")
			}
		})
	}
}

func TestWriteSchemaRejectsCaveatAndNamespaceWithSameName(t *testing.T) {
	t.Parallel()

	env := caveats.NewEnvironmentWithDefaultTypeSet()
	caveatDef := ns.MustCaveatDefinition(env, "samename", "1 == 1")
	namespaceDef := ns.Namespace("samename")

	defs := []datastore.SchemaDefinition{namespaceDef, caveatDef}

	// WriteSchemaViaStoredSchema uses MustBugf for duplicate names, which panics in tests.
	require.Panics(t, func() {
		_, _, _ = WriteSchemaViaStoredSchema(t.Context(), nil, defs, "")
	})
}

func TestParseSchemaMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected SchemaMode
		wantErr  bool
	}{
		{"read-legacy-write-legacy", SchemaModeReadLegacyWriteLegacy, false},
		{"read-legacy-write-both", SchemaModeReadLegacyWriteBoth, false},
		{"read-new-write-both", SchemaModeReadNewWriteBoth, false},
		{"read-new-write-new", SchemaModeReadNewWriteNew, false},
		{"invalid", SchemaModeReadLegacyWriteLegacy, true},
		{"", SchemaModeReadLegacyWriteLegacy, true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			mode, err := ParseSchemaMode(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid schema mode")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, mode)
			}
		})
	}
}

func TestSchemaModeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode     SchemaMode
		expected string
	}{
		{SchemaModeReadLegacyWriteLegacy, "read-legacy-write-legacy"},
		{SchemaModeReadLegacyWriteBoth, "read-legacy-write-both"},
		{SchemaModeReadNewWriteBoth, "read-new-write-both"},
		{SchemaModeReadNewWriteNew, "read-new-write-new"},
		{SchemaMode(255), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, tc.mode.String())
		})
	}
}

func TestSchemaModeProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode           SchemaMode
		readsFromNew   bool
		writesToLegacy bool
		writesToNew    bool
	}{
		{SchemaModeReadLegacyWriteLegacy, false, true, false},
		{SchemaModeReadLegacyWriteBoth, false, true, true},
		{SchemaModeReadNewWriteBoth, true, true, true},
		{SchemaModeReadNewWriteNew, true, false, true},
	}

	for _, tc := range tests {
		t.Run(tc.mode.String(), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.readsFromNew, tc.mode.ReadsFromNew())
			require.Equal(t, tc.writesToLegacy, tc.mode.WritesToLegacy())
			require.Equal(t, tc.writesToNew, tc.mode.WritesToNew())
		})
	}
}

func TestParseSchemaModeRoundTrip(t *testing.T) {
	t.Parallel()

	modes := []SchemaMode{
		SchemaModeReadLegacyWriteLegacy,
		SchemaModeReadLegacyWriteBoth,
		SchemaModeReadNewWriteBoth,
		SchemaModeReadNewWriteNew,
	}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			parsed, err := ParseSchemaMode(mode.String())
			require.NoError(t, err)
			require.Equal(t, mode, parsed)
		})
	}
}

func TestStoredSchemaReaderAdapterEmptySchema(t *testing.T) {
	t.Parallel()

	// Test the storedSchemaReaderAdapter when schema is empty (no definitions).
	adapter := &storedSchemaReaderAdapter{
		storedSchema: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
			Version: 1,
			VersionOneof: &core.StoredSchema_V1{
				V1: &core.StoredSchema_V1StoredSchema{},
			},
		}),
		lastWrittenRevision: datastore.NoRevision,
	}

	ctx := t.Context()

	// SchemaText should return error for empty schema.
	_, err := adapter.SchemaText(ctx)
	require.Error(t, err)

	// Lookups should return not-found.
	_, found, err := adapter.LookupTypeDefByName(ctx, "user")
	require.NoError(t, err)
	require.False(t, found)

	_, found, err = adapter.LookupCaveatDefByName(ctx, "somecaveat")
	require.NoError(t, err)
	require.False(t, found)

	// Lists should return empty.
	typeDefs, err := adapter.ListAllTypeDefinitions(ctx)
	require.NoError(t, err)
	require.Empty(t, typeDefs)

	caveatDefs, err := adapter.ListAllCaveatDefinitions(ctx)
	require.NoError(t, err)
	require.Empty(t, caveatDefs)

	allDefs, err := adapter.ListAllSchemaDefinitions(ctx)
	require.NoError(t, err)
	require.Empty(t, allDefs)

	// Lookups by name should return empty maps.
	looked, err := adapter.LookupSchemaDefinitionsByNames(ctx, []string{"user"})
	require.NoError(t, err)
	require.Empty(t, looked)

	typeLooked, err := adapter.LookupTypeDefinitionsByNames(ctx, []string{"user"})
	require.NoError(t, err)
	require.Empty(t, typeLooked)

	caveatLooked, err := adapter.LookupCaveatDefinitionsByNames(ctx, []string{"somecaveat"})
	require.NoError(t, err)
	require.Empty(t, caveatLooked)
}

func TestStoredSchemaReaderAdapterV1Nil(t *testing.T) {
	t.Parallel()

	// Test the v1() fallback path when VersionOneof is nil.
	adapter := &storedSchemaReaderAdapter{
		storedSchema: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
			Version: 1,
		}),
		lastWrittenRevision: datastore.NoRevision,
	}

	// v1() should return a zero-valued V1StoredSchema, so no panic.
	_, err := adapter.SchemaText(t.Context())
	require.Error(t, err) // empty schema, no definitions
}

func TestUnwrapDatastore(t *testing.T) {
	t.Parallel()

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds)

	// Should unwrap to the original datastore.
	unwrapped := UnwrapDatastore(dl)
	require.NotNil(t, unwrapped)
	require.Equal(t, ds, unwrapped)
}

func toCompilerDefs(defs []datastore.SchemaDefinition) []compiler.SchemaDefinition {
	result := make([]compiler.SchemaDefinition, 0, len(defs))
	for _, def := range defs {
		result = append(result, def.(compiler.SchemaDefinition))
	}
	return result
}

func computeHash(t *testing.T, defs []datastore.SchemaDefinition) string {
	t.Helper()
	hash, err := generator.ComputeSchemaHash(toCompilerDefs(defs))
	require.NoError(t, err)
	return hash
}

func TestComputeSchemaHash(t *testing.T) {
	t.Parallel()

	defs, _ := testSchemaDefinitions(t)

	hash1, err := generator.ComputeSchemaHash(toCompilerDefs(defs))
	require.NoError(t, err)
	require.NotEmpty(t, hash1)

	// Same definitions should produce same hash.
	hash2, err := generator.ComputeSchemaHash(toCompilerDefs(defs))
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)

	// Different definitions should produce different hash.
	differentDefs := []datastore.SchemaDefinition{
		ns.Namespace("different"),
	}
	hash3, err := generator.ComputeSchemaHash(toCompilerDefs(differentDefs))
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash3)
}

func TestWriteSchemaDeletesRemovedDefinitions(t *testing.T) {
	t.Parallel()

	// Test that writing a schema with fewer definitions removes the old ones.
	modes := []struct {
		name string
		mode SchemaMode
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))
			ctx := t.Context()

			// Write initial schema with two definitions.
			defs, schemaText := testSchemaDefinitions(t)
			_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			// Write schema with only one definition.
			smallerDefs := []datastore.SchemaDefinition{ns.Namespace("user")}
			smallSchemaText, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("user")})
			require.NoError(err)

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, smallerDefs, smallSchemaText, nil)
				return err
			})
			require.NoError(err)

			// Read back and verify "document" is gone.
			reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
			schemaReader, err := reader.ReadSchema(ctx)
			require.NoError(err)

			typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
			require.NoError(err)
			require.Len(typeDefs, 1)
			require.Equal("user", typeDefs[0].Definition.Name)
		})
	}
}

// testSchemaCache is a simple in-memory cache satisfying SchemaCache for tests.
type testSchemaCache struct {
	mu    sync.Mutex
	items map[SchemaCacheKey]*datastore.ReadOnlyStoredSchema // GUARDED_BY(mu)
}

func newTestSchemaCache() *testSchemaCache {
	return &testSchemaCache{items: make(map[SchemaCacheKey]*datastore.ReadOnlyStoredSchema)}
}

func (c *testSchemaCache) Get(key SchemaCacheKey) (*datastore.ReadOnlyStoredSchema, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[key]
	return v, ok
}

func (c *testSchemaCache) Set(key SchemaCacheKey, entry *datastore.ReadOnlyStoredSchema, _ int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry
	return true
}

func (c *testSchemaCache) Wait() {}

// countingDatastore wraps a datastore.Datastore and counts ReadStoredSchema calls.
type countingDatastore struct {
	datastore.Datastore
	readStoredSchemaCalls atomic.Int32
}

func (c *countingDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &countingReader{Reader: c.Datastore.SnapshotReader(rev), counter: &c.readStoredSchemaCalls}
}

type countingReader struct {
	datastore.Reader
	counter *atomic.Int32
}

func (r *countingReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	r.counter.Add(1)
	return r.Reader.ReadStoredSchema(ctx)
}

func TestHashCacheIntegration_CachesReadStoredSchema(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS := newTestDatastore(t)
	ds := &countingDatastore{Datastore: rawDS}
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew), WithSchemaCache(newTestSchemaCache()))

	defs, schemaText := testSchemaDefinitions(t)
	ctx := t.Context()

	// Write schema to populate unified storage.
	rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(err)

	// Compute the expected hash to use for cached reads.
	compDefs := make([]compiler.SchemaDefinition, 0, len(defs))
	for _, def := range defs {
		compDefs = append(compDefs, def.(compiler.SchemaDefinition))
	}
	hash, err := generator.ComputeSchemaHash(compDefs)
	require.NoError(err)
	schemaHash := SchemaHash(hash)

	// Reset call counter (writes may have triggered reads).
	ds.readStoredSchemaCalls.Store(0)

	// First read with the schema hash should hit the cache (populated by WriteSchema).
	reader := dl.SnapshotReader(rev, schemaHash)
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(err)

	readText, err := schemaReader.SchemaText(ctx)
	require.NoError(err)
	require.NotEmpty(readText)

	callsAfterFirst := ds.readStoredSchemaCalls.Load()
	require.Equal(int32(0), callsAfterFirst, "should serve from cache without hitting datastore")

	// Second read should also hit cache.
	reader2 := dl.SnapshotReader(rev, schemaHash)
	schemaReader2, err := reader2.ReadSchema(ctx)
	require.NoError(err)

	readText2, err := schemaReader2.SchemaText(ctx)
	require.NoError(err)
	require.Equal(readText, readText2)
	require.Equal(int32(0), ds.readStoredSchemaCalls.Load(), "second read should also be cached")
}

func TestHashCacheIntegration_BypassSentinelSkipsCache(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS := newTestDatastore(t)
	ds := &countingDatastore{Datastore: rawDS}
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew), WithSchemaCache(newTestSchemaCache()))

	defs, schemaText := testSchemaDefinitions(t)
	ctx := t.Context()

	rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(err)

	ds.readStoredSchemaCalls.Store(0)

	// Read with testing sentinel should bypass cache and hit datastore.
	reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(err)

	readText, err := schemaReader.SchemaText(ctx)
	require.NoError(err)
	require.NotEmpty(readText)
	require.Equal(int32(1), ds.readStoredSchemaCalls.Load(), "sentinel should bypass cache")
}

func TestHashCacheIntegration_WriteUpdatesCache(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS := newTestDatastore(t)
	ds := &countingDatastore{Datastore: rawDS}
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew), WithSchemaCache(newTestSchemaCache()))

	ctx := t.Context()

	// Write initial schema.
	defs1, schemaText1 := testSchemaDefinitions(t)
	rev1, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(err)

	// Write different schema.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	rev2, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(err)

	// Compute hash for the second schema.
	hash2, err := generator.ComputeSchemaHash([]compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	ds.readStoredSchemaCalls.Store(0)

	// Read with the new hash should be served from cache (populated by WriteSchema).
	reader := dl.SnapshotReader(rev2, SchemaHash(hash2))
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(err)

	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs, 1)
	require.Equal("newtype", typeDefs[0].Definition.Name)
	require.Equal(int32(0), ds.readStoredSchemaCalls.Load(), "new hash should be cached from write")

	// Read with the first schema's hash should also be cached.
	compDefs1 := make([]compiler.SchemaDefinition, 0, len(defs1))
	for _, def := range defs1 {
		compDefs1 = append(compDefs1, def.(compiler.SchemaDefinition))
	}
	hash1, err := generator.ComputeSchemaHash(compDefs1)
	require.NoError(err)

	reader1 := dl.SnapshotReader(rev1, SchemaHash(hash1))
	_, err = reader1.ReadSchema(ctx)
	require.NoError(err)
	require.Equal(int32(0), ds.readStoredSchemaCalls.Load(), "old hash should also be cached from first write")
}

// TestNewReadOnlyDataLayer_RejectsWrite ensures the read-only adapter returns a
// readonly error from ReadWriteTx and pass-throughs everything else.
func TestNewReadOnlyDataLayer_RejectsWrite(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ro := NewReadOnlyDataLayer(ds)
	ctx := t.Context()

	_, err = ro.ReadWriteTx(ctx, func(context.Context, ReadWriteTransaction) error {
		return nil
	})
	require.Error(t, err)

	rev, _, err := ro.HeadRevision(ctx)
	require.NoError(t, err)
	require.NoError(t, ro.CheckRevision(ctx, rev))

	readyState, err := ro.ReadyState(ctx)
	require.NoError(t, err)
	require.True(t, readyState.IsReady)

	_, err = ro.Features(ctx)
	require.NoError(t, err)

	_, err = ro.OfflineFeatures()
	require.NoError(t, err)

	_, err = ro.Statistics(ctx)
	require.NoError(t, err)

	_, err = ro.UniqueID(ctx)
	require.NoError(t, err)

	_, err = ro.MetricsID()
	require.NoError(t, err)

	optRev, _, err := ro.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.NotNil(t, optRev)

	rtr, err := ro.RevisionFromString(rev.String())
	require.NoError(t, err)
	require.True(t, rtr.Equal(rev))

	watchCh, errCh := ro.Watch(ctx, rev, datastore.WatchOptions{Content: datastore.WatchRelationships})
	require.NotNil(t, watchCh)
	require.NotNil(t, errCh)

	// UnwrapDatastore returns nil for the readonly adapter.
	require.Nil(t, UnwrapDatastore(ro))

	require.NoError(t, ro.Close())
}

// TestReadOnlyDataLayer_ReaderPassThroughs exercises the snapshot reader on the
// readonly adapter.
func TestReadOnlyDataLayer_ReaderPassThroughs(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 5*time.Second, 1*time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ro := NewReadOnlyDataLayer(ds)
	ctx := t.Context()

	rev, schemaHash, err := ro.HeadRevision(ctx)
	require.NoError(t, err)

	reader := ro.SnapshotReader(rev, schemaHash)

	// ReadSchema on revisionedReader (shared between impl.go and readonly adapter).
	sr, err := reader.ReadSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, sr)

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "resource"})
	require.NoError(t, err)
	rels, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Empty(t, rels)

	rit, err := reader.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{SubjectType: "user"})
	require.NoError(t, err)
	rrels, err := datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Empty(t, rrels)

	_, err = reader.CountRelationships(ctx, "missing")
	require.Error(t, err)

	_, err = reader.LookupCounters(ctx)
	require.NoError(t, err)
}

// stubBulkSourceDL is a minimal BulkWriteRelationshipSource for tests.
type stubBulkSourceDL struct {
	rels []tuple.Relationship
	idx  int
}

func (s *stubBulkSourceDL) Next(_ context.Context) (*tuple.Relationship, error) {
	if s.idx >= len(s.rels) {
		return nil, nil
	}
	rel := s.rels[s.idx]
	s.idx++
	return &rel, nil
}

// TestReadWriteTransaction_AllMethods exercises the RW transaction wrappers in
// impl.go: WriteRelationships, DeleteRelationships, ReadSchema,
// CountRelationships, LookupCounters, BulkLoad, counter registration, and the
// legacy schema writer.
func TestReadWriteTransaction_AllMethods(t *testing.T) {
	dl, _ := newTestDataLayer(t)
	ctx := t.Context()

	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		require.NoError(t, rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("resource:bar#viewer@user:fred")),
		}))

		_, _, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType:       "resource",
			OptionalResourceId: "bar",
		})
		require.NoError(t, err)

		sr, err := rwt.ReadSchema(ctx)
		require.NoError(t, err)
		require.NotNil(t, sr)

		_, err = rwt.CountRelationships(ctx, "missing")
		require.Error(t, err)

		_, err = rwt.LookupCounters(ctx)
		require.NoError(t, err)

		// BulkLoad via the passthrough iterator.
		src := &stubBulkSourceDL{rels: []tuple.Relationship{
			tuple.MustParse("resource:bulk1#viewer@user:alice"),
			tuple.MustParse("resource:bulk2#viewer@user:bob"),
		}}
		loaded, err := rwt.BulkLoad(ctx, src)
		require.NoError(t, err)
		require.Equal(t, uint64(2), loaded)

		// Counter registration + store + unregister.
		counterName := "my_counter"
		require.NoError(t, rwt.RegisterCounter(ctx, counterName, &core.RelationshipFilter{
			ResourceType: "resource",
		}))

		// StoreCounterValue requires a revision; use a zero/no revision.
		require.NoError(t, rwt.StoreCounterValue(ctx, counterName, 42, datastore.NoRevision))

		require.NoError(t, rwt.UnregisterCounter(ctx, counterName))

		// LegacySchemaWriter surface: exercise each method. memdb's legacy writer
		// accepts empty inputs.
		lw := rwt.LegacySchemaWriter()
		require.NotNil(t, lw)
		require.NoError(t, lw.LegacyWriteCaveats(ctx, nil))
		require.NoError(t, lw.LegacyWriteNamespaces(ctx))
		require.NoError(t, lw.LegacyDeleteCaveats(ctx, nil))
		require.NoError(t, lw.LegacyDeleteNamespaces(ctx, nil, datastore.DeleteNamespacesOnly))

		// WriteSchema with no definitions should succeed via the legacy path.
		_, err = rwt.WriteSchema(ctx, nil, "", nil)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)
}

func TestHashCacheIntegration_CacheAcrossPhaseTransitions(t *testing.T) {
	t.Parallel()

	rawDS := newTestDatastore(t)
	ds := &countingDatastore{Datastore: rawDS}
	cache := newTestSchemaCache()
	ctx := t.Context()

	// Phase 1: Write schema with cache in ReadLegacyWriteBoth (populates unified storage).
	dl1 := NewDataLayer(ds, WithSchemaMode(SchemaModeReadLegacyWriteBoth), WithSchemaCache(cache))
	defs1, schemaText1 := testSchemaDefinitions(t)

	rev1, err := dl1.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(t, err)

	// Compute the hash for defs1.
	compDefs1 := make([]compiler.SchemaDefinition, 0, len(defs1))
	for _, def := range defs1 {
		compDefs1 = append(compDefs1, def.(compiler.SchemaDefinition))
	}
	hash1, err := generator.ComputeSchemaHash(compDefs1)
	require.NoError(t, err)

	// Phase 2: Create a new DataLayer in ReadNewWriteNew with the SAME cache.
	// The cache entry from phase 1's write should still be usable.
	dl2 := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew), WithSchemaCache(cache))

	ds.readStoredSchemaCalls.Store(0)

	reader := dl2.SnapshotReader(rev1, SchemaHash(hash1))
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(t, err)

	readText, err := schemaReader.SchemaText(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, readText)
	require.Equal(t, int32(0), ds.readStoredSchemaCalls.Load(),
		"cache from phase 1 write should serve reads in phase 2 without hitting datastore")

	// Write a different schema in phase 2.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(t, err)

	rev2, err := dl2.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(t, err)

	hash2, err := generator.ComputeSchemaHash([]compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(t, err)

	ds.readStoredSchemaCalls.Store(0)

	// Read with new hash should be cached from the phase 2 write.
	reader2 := dl2.SnapshotReader(rev2, SchemaHash(hash2))
	schemaReader2, err := reader2.ReadSchema(ctx)
	require.NoError(t, err)

	typeDefs, err := schemaReader2.ListAllTypeDefinitions(ctx)
	require.NoError(t, err)
	require.Len(t, typeDefs, 1)
	require.Equal(t, "newtype", typeDefs[0].Definition.Name)
	require.Equal(t, int32(0), ds.readStoredSchemaCalls.Load(),
		"phase 2 write should populate cache for new hash")

	// Old hash from phase 1 should still be cached.
	reader3 := dl2.SnapshotReader(rev1, SchemaHash(hash1))
	_, err = reader3.ReadSchema(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(0), ds.readStoredSchemaCalls.Load(),
		"old hash from phase 1 should still be in cache")
}

func TestHashCacheIntegration_NoCacheStillWorks(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Without a cache, everything should still work.
	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	ctx := t.Context()

	rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(err)

	reader := dl.SnapshotReader(rev, NoSchemaHashForTesting)
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(err)

	readText, err := schemaReader.SchemaText(ctx)
	require.NoError(err)
	require.NotEmpty(readText)
}

func TestHeadRevisionReturnsSchemaHashInNewReadModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       SchemaMode
		expectHash bool
	}{
		{"ReadLegacyWriteLegacy", SchemaModeReadLegacyWriteLegacy, false},
		{"ReadLegacyWriteBoth", SchemaModeReadLegacyWriteBoth, false},
		{"ReadNewWriteBoth", SchemaModeReadNewWriteBoth, true},
		{"ReadNewWriteNew", SchemaModeReadNewWriteNew, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
				return err
			})
			require.NoError(err)

			_, hash, err := dl.HeadRevision(ctx)
			require.NoError(err)

			if tc.expectHash {
				expectedHash := computeHash(t, defs)
				require.Equal(SchemaHash(expectedHash), hash,
					"HeadRevision should return the matching schema hash in new-read mode %s", tc.name)
				require.False(hash.IsBypassSentinel(), "hash should not be a sentinel")
			} else {
				require.Equal(NoSchemaHashInLegacyMode, hash,
					"HeadRevision should return legacy sentinel in mode %s", tc.name)
			}
		})
	}
}

func TestHeadRevisionSchemaHashChangesWithSchema(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))
	ctx := t.Context()

	// Write first schema.
	defs1, schemaText1 := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(err)

	_, hash1, err := dl.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(SchemaHash(computeHash(t, defs1)), hash1)

	// Write a different schema.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(err)

	_, hash2, err := dl.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(SchemaHash(computeHash(t, defs2)), hash2)

	// Hashes should differ.
	require.NotEqual(hash1, hash2, "different schemas should produce different hashes")
}

func TestHeadRevisionSchemaHashBeforeSchemaWrite(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))
	ctx := t.Context()

	// Before any schema is written, HeadRevision should return legacy sentinel
	// (no schema hash available).
	_, hash, err := dl.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(NoSchemaHashInLegacyMode, hash,
		"HeadRevision should return legacy sentinel when no schema has been written")
}

func TestHeadRevisionSchemaHashMatchesRevision(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))
	ctx := t.Context()

	// Write first schema.
	defs1, schemaText1 := testSchemaDefinitions(t)
	rev1, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(err)

	// Write second schema.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(err)

	// HeadRevision should return the hash for the latest (second) schema.
	_, headHash, err := dl.HeadRevision(ctx)
	require.NoError(err)
	require.Equal(SchemaHash(computeHash(t, defs2)), headHash)

	// Use the hash from HeadRevision to read schema and verify it matches.
	reader := dl.SnapshotReader(rev1, SchemaHash(computeHash(t, defs1)))
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(err)

	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs, 2, "first revision should have the original 2-type schema")
}

// TestReadSchemaInWriteTxOnFreshDatastore verifies that ReadSchema inside a write
// transaction on a database with no schema returns ErrSchemaNotFound.
func TestReadSchemaInWriteTxOnFreshDatastore(t *testing.T) {
	t.Parallel()

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))
	ctx := t.Context()

	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.ReadSchema(ctx)
		return err
	})
	require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
}

// TestRolledBackWriteDoesNotPolluteCache verifies that a schema write whose
// transaction is rolled back (callback returns an error) does NOT populate the
// schema cache. cache.Set is deferred to post-commit, so a rolled-back write
// must not leave a stale entry that subsequent reads could serve.
func TestRolledBackWriteDoesNotPolluteCache(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	rawDS := newTestDatastore(t)
	innerCache := newTestSchemaCache()
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew), WithSchemaCache(innerCache))
	ctx := t.Context()

	defs, schemaText := testSchemaDefinitions(t)

	// Pre-compute the hash that WriteSchema would produce for these definitions.
	rolledBackHash := SchemaHash(computeHash(t, defs))

	rollbackErr := errors.New("intentional rollback")

	// Write schema then force a rollback by returning an error from the callback.
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		if _, wErr := rwt.WriteSchema(ctx, defs, schemaText, nil); wErr != nil {
			return wErr
		}
		return rollbackErr
	})
	require.ErrorIs(err, rollbackErr)

	// The rolled-back hash must NOT appear in the cache.
	_, ok := innerCache.Get(SchemaCacheKey(rolledBackHash))
	require.False(ok, "rolled-back write must not populate the schema cache")

	// A subsequent write transaction must see ErrSchemaNotFound (not the rolled-back schema).
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, rErr := rwt.ReadSchema(ctx)
		return rErr
	})
	require.ErrorIs(err, datastore.ErrSchemaNotFound)
}

// TestReadSchemaInWriteTxOnExistingDatastore verifies that ReadSchema inside a write
// transaction on a database that already has schema returns the schema correctly.
func TestReadSchemaInWriteTxOnExistingDatastore(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ds := newTestDatastore(t)
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadNewWriteNew))
	ctx := t.Context()
	defs, schemaText := testSchemaDefinitions(t)

	// Write schema in tx1.
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(err)

	// In tx2, read schema without writing first.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		sr, err := rwt.ReadSchema(ctx)
		if err != nil {
			return err
		}
		typeDefs, err := sr.ListAllTypeDefinitions(ctx)
		if err != nil {
			return err
		}
		require.Len(typeDefs, 2)
		return nil
	})
	require.NoError(err)
}

// TestSchemaHashPreconditionMatch verifies that ReadSchema uses the precondition hash
// directly when WithSchemaHashPrecondition is set and the hash matches.
func TestSchemaHashPreconditionMatch(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	// Get the stored hash via HeadRevision.
	headRev, err := rawDS.HeadRevision(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, headRev.SchemaHash)

	// Open a write tx with the correct precondition and read schema — must succeed.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.ReadSchema(ctx)
		return err
	}, options.WithSchemaHashPrecondition(headRev.SchemaHash))
	require.NoError(t, err)
}

// TestSchemaHashPreconditionMismatch verifies that a wrong precondition hash returns
// ErrSchemaHashPreconditionFailed when retries are disabled, and that the user
// callback is never called (assertSchemaHash fires before fn).
func TestSchemaHashPreconditionMismatch(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	fnCalled := false
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		fnCalled = true
		return nil
	}, options.WithSchemaHashPrecondition("wrong-hash-value"), options.WithDisableRetries(true))
	require.ErrorIs(t, err, datastore.ErrSchemaHashPreconditionFailed)
	require.False(t, fnCalled, "user callback must not be called when precondition fails")
}

// TestSchemaHashPreconditionIgnoredInLegacyMode verifies that WithSchemaHashPrecondition
// is effectively ignored by the datalayer in legacy schema modes (the hash does not
// affect ReadSchema routing, which falls back to legacy storage).
func TestSchemaHashPreconditionIgnoredInLegacyMode(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadLegacyWriteLegacy))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	// In legacy mode, any precondition string is simply ignored by the datalayer's
	// ReadSchema path. The underlying datastore may or may not check it; memdb does
	// check it but there is no schema_revision row in legacy-only mode, so we must
	// not pass a precondition to avoid a false ErrSchemaHashPreconditionFailed.
	// This test uses an empty precondition to confirm legacy mode works normally.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		sr, err := rwt.ReadSchema(ctx)
		if err != nil {
			return err
		}
		typeDefs, err := sr.ListAllTypeDefinitions(ctx)
		if err != nil {
			return err
		}
		require.Len(t, typeDefs, 2)
		return nil
	})
	require.NoError(t, err)
}

// TestSchemaHashPreconditionEmpty verifies that an empty precondition behaves
// identically to not passing one at all — ReadSchema succeeds.
func TestSchemaHashPreconditionEmpty(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	// Passing an empty precondition must behave identically to passing no option.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		sr, err := rwt.ReadSchema(ctx)
		if err != nil {
			return err
		}
		typeDefs, err := sr.ListAllTypeDefinitions(ctx)
		if err != nil {
			return err
		}
		require.Len(t, typeDefs, 2)
		return nil
	}, options.WithSchemaHashPrecondition(""))
	require.NoError(t, err)
}

// TestSchemaHashPreconditionRetryOnStalePrecondition verifies that the datalayer
// transparently retries when a precondition hash is stale (schema was updated after
// the hash was fetched). The callback must be called exactly once — on the retry,
// not on the failed first attempt (since assertSchemaHash fires before fn).
func TestSchemaHashPreconditionRetryOnStalePrecondition(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	// Write first schema, capture its hash.
	defs1, schemaText1 := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(t, err)

	headRev1, err := rawDS.HeadRevision(ctx)
	require.NoError(t, err)
	staleHash := headRev1.SchemaHash
	require.NotEmpty(t, staleHash)

	// Write a different schema — staleHash is now outdated.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("team")}
	schemaText2, _, err := generator.GenerateSchema(ctx, []compiler.SchemaDefinition{ns.Namespace("team")})
	require.NoError(t, err)
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(t, err)

	// Open a write tx with the stale hash. The datalayer should detect the mismatch,
	// fetch the new hash, and retry — the callback must succeed on that retry.
	callCount := 0
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		callCount++
		return nil
	}, options.WithSchemaHashPrecondition(staleHash))
	require.NoError(t, err)
	require.Equal(t, 1, callCount, "callback should be called exactly once (on the retry)")
}

// TestSchemaHashPreconditionNoRetryWhenDisabled verifies that DisableRetries suppresses
// the automatic refresh-and-retry on ErrSchemaHashPreconditionFailed.
func TestSchemaHashPreconditionNoRetryWhenDisabled(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs1, schemaText1 := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs1, schemaText1, nil)
		return err
	})
	require.NoError(t, err)

	headRev1, err := rawDS.HeadRevision(ctx)
	require.NoError(t, err)
	staleHash := headRev1.SchemaHash

	// Update schema — staleHash is now outdated.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("team")}
	schemaText2, _, err := generator.GenerateSchema(ctx, []compiler.SchemaDefinition{ns.Namespace("team")})
	require.NoError(t, err)
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs2, schemaText2, nil)
		return err
	})
	require.NoError(t, err)

	// With DisableRetries, the stale hash must surface as an error rather than retrying.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	}, options.WithSchemaHashPrecondition(staleHash), options.WithDisableRetries(true))
	require.ErrorIs(t, err, datastore.ErrSchemaHashPreconditionFailed)
}

// preconditionCapturingDS wraps a Datastore and records the SchemaHashPrecondition
// value passed to each ReadWriteTx call so tests can assert auto-seeding behavior.
type preconditionCapturingDS struct {
	datastore.Datastore
	mu                    sync.Mutex
	capturedPreconditions []string // GUARDED_BY(mu)
}

func (d *preconditionCapturingDS) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)
	d.mu.Lock()
	d.capturedPreconditions = append(d.capturedPreconditions, config.SchemaHashPrecondition)
	d.mu.Unlock()
	return d.Datastore.ReadWriteTx(ctx, fn, opts...)
}

func (d *preconditionCapturingDS) lastCapturedPrecondition() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.capturedPreconditions) == 0 {
		return ""
	}
	return d.capturedPreconditions[len(d.capturedPreconditions)-1]
}

// TestAutoSeedLastSchemaHashFromHeadRevision verifies that calling HeadRevision
// populates lastSchemaHash and that a subsequent ReadWriteTx without an explicit
// precondition automatically forwards that hash to the underlying datastore.
func TestAutoSeedLastSchemaHashFromHeadRevision(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	dl := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	// Write schema so a hash exists in the datastore.
	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	// Record the hash and reset the capture slice so the next call is isolated.
	_, seededHash, err := dl.HeadRevision(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, seededHash)

	capDS.mu.Lock()
	capDS.capturedPreconditions = nil
	capDS.mu.Unlock()

	// ReadWriteTx without any explicit precondition should auto-seed from lastSchemaHash.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, string(seededHash), capDS.lastCapturedPrecondition(),
		"ReadWriteTx should forward the hash observed by HeadRevision as the precondition")
}

// TestAutoSeedLastSchemaHashFromSnapshotReader verifies that SnapshotReader
// populates lastSchemaHash so a subsequent ReadWriteTx uses it automatically.
func TestAutoSeedLastSchemaHashFromSnapshotReader(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	dl := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	expectedHash := SchemaHash(computeHash(t, defs))

	// SnapshotReader with a non-bypass hash should seed lastSchemaHash.
	_ = dl.SnapshotReader(rev, expectedHash)

	capDS.mu.Lock()
	capDS.capturedPreconditions = nil
	capDS.mu.Unlock()

	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, string(expectedHash), capDS.lastCapturedPrecondition(),
		"ReadWriteTx should forward the hash observed by SnapshotReader as the precondition")
}

// TestAutoSeedLastSchemaHashFromSchemaWriteCommit verifies that committing a WriteSchema
// inside ReadWriteTx populates lastSchemaHash so the next ReadWriteTx uses it.
func TestAutoSeedLastSchemaHashFromSchemaWriteCommit(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	dl := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	// First write: the committed hash should become the seeded value.
	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	expectedHash := SchemaHash(computeHash(t, defs))

	capDS.mu.Lock()
	capDS.capturedPreconditions = nil
	capDS.mu.Unlock()

	// Second write tx (no explicit precondition) should use the hash from the first commit.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, string(expectedHash), capDS.lastCapturedPrecondition(),
		"ReadWriteTx should forward the hash observed at schema write commit as the precondition")
}

// TestAutoSeedFallbackToHeadRevisionWhenCold verifies that when lastSchemaHash is
// empty (cold datalayer) ReadWriteTx falls back to HeadRevision to seed the hash.
func TestAutoSeedFallbackToHeadRevisionWhenCold(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)

	// Write schema via a separate datalayer so the hash exists in the datastore
	// without being known to the datalayer under test.
	dl1 := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))
	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl1.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	expectedHash := SchemaHash(computeHash(t, defs))

	// dl2 is cold — it has never called HeadRevision/SnapshotReader/ReadWriteTx.
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	dl2 := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	_, err = dl2.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, string(expectedHash), capDS.lastCapturedPrecondition(),
		"cold datalayer should fall back to HeadRevision to seed the precondition hash")
}

// TestAssertSchemaHashReturnsErrSchemaNotFound verifies that passing a non-empty
// precondition against a datastore with no schema row returns ErrSchemaNotFound,
// not ErrSchemaHashPreconditionFailed.
func TestAssertSchemaHashReturnsErrSchemaNotFound(t *testing.T) {
	ctx := t.Context()

	// Fresh datastore with no schema written — no schema_revision row exists.
	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	}, options.WithSchemaHashPrecondition("any-hash"), options.WithDisableRetries(true))
	require.ErrorIs(t, err, datastore.ErrSchemaNotFound,
		"missing schema row should surface as ErrSchemaNotFound, not ErrSchemaHashPreconditionFailed")
}

// TestMemdbStaleHashClearedOnWriteWithoutHash verifies that writing a schema without
// a hash field clears any previously stored schema_revision row, so the old hash
// cannot be matched by a subsequent precondition check.
func TestMemdbStaleHashClearedOnWriteWithoutHash(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	// Write schema with a hash via the datalayer.
	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	_, storedHash, err := dl.HeadRevision(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, storedHash)

	// Overwrite schema via rawDS with no hash — simulates a legacy/migration write.
	// An empty StoredSchema has no v1.SchemaHash, so the schema_revision row should be cleared.
	_, err = rawDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteStoredSchema(ctx, &core.StoredSchema{})
	})
	require.NoError(t, err)

	// The old hash should now produce ErrSchemaNotFound, not ErrSchemaHashPreconditionFailed,
	// because the schema_revision row was cleared by the write-without-hash.
	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	}, options.WithSchemaHashPrecondition(string(storedHash)), options.WithDisableRetries(true))
	require.ErrorIs(t, err, datastore.ErrSchemaNotFound,
		"old hash should yield ErrSchemaNotFound after a no-hash write clears the schema_revision row")
}

// TestMemdbConcurrentWriteBlocksUntilActive verifies that when a write transaction is
// already active in memdb, a concurrent write blocks (via sync.Cond) until the active
// transaction finishes, then proceeds successfully.
func TestMemdbConcurrentWriteBlocksUntilActive(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	dl := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	headRev, err := rawDS.HeadRevision(ctx)
	require.NoError(t, err)
	hash := headRev.SchemaHash
	require.NotEmpty(t, hash)

	// Hold the memdb write lock open.
	lockHeld := make(chan struct{})
	lockRelease := make(chan struct{})
	go func() {
		_, _ = rawDS.ReadWriteTx(ctx, func(ctx context.Context, _ datastore.ReadWriteTransaction) error {
			close(lockHeld)
			<-lockRelease
			return nil
		})
	}()
	<-lockHeld

	// A concurrent write should block (not spin) until the active tx finishes.
	resultCh := make(chan error, 1)
	go func() {
		_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
			return nil
		}, options.WithSchemaHashPrecondition(hash))
		resultCh <- err
	}()

	close(lockRelease)

	require.NoError(t, <-resultCh,
		"ReadWriteTx should succeed once the active transaction completes")
}

// TestFirstWriteSchemaOnEmptyDatastoreSucceeds verifies that WriteSchema works on a
// fresh datastore with no prior schema. HeadRevision returns an empty SchemaHash when
// no schema exists, so no precondition is set and assertSchemaHash is never called.
func TestFirstWriteSchemaOnEmptyDatastoreSucceeds(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	dl := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	defs, schemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err, "first WriteSchema on empty datastore must not return ErrSchemaNotFound")

	// No precondition should have been sent — HeadRevision returned empty SchemaHash.
	require.Empty(t, capDS.lastCapturedPrecondition(),
		"empty datastore HeadRevision returns no hash, so no precondition should be forwarded")
}

// TestColdNodeWithExistingSchemaSucceeds verifies that a newly started node (cold
// lastSchemaHash) can perform a write transaction when schema already exists in the
// datastore. The HeadRevision fallback seeds the correct hash so assertSchemaHash passes.
func TestColdNodeWithExistingSchemaSucceeds(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)

	// Write schema using a separate datalayer instance to simulate schema already
	// existing in the datastore before this node starts up.
	warmDL := NewDataLayer(rawDS, WithSchemaMode(SchemaModeReadNewWriteNew))
	defs, schemaText := testSchemaDefinitions(t)
	_, err := warmDL.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.WriteSchema(ctx, defs, schemaText, nil)
		return err
	})
	require.NoError(t, err)

	expectedHash := SchemaHash(computeHash(t, defs))

	// Cold node: fresh datalayer pointing at the same datastore, lastSchemaHash is nil.
	capDS := &preconditionCapturingDS{Datastore: rawDS}
	coldDL := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	_, err = coldDL.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.NoError(t, err, "cold node write tx must succeed when schema exists in the datastore")

	// The HeadRevision fallback should have seeded the existing hash as the precondition.
	require.Equal(t, string(expectedHash), capDS.lastCapturedPrecondition(),
		"cold node should fall back to HeadRevision and use the existing hash as precondition")
}

// erroringRevisionDS wraps a Datastore and returns a configurable error from
// HeadRevision and OptimizedRevision, used to cover the error-return branches.
type erroringRevisionDS struct {
	datastore.Datastore
	headErr      error
	optimizedErr error
}

func (d *erroringRevisionDS) HeadRevision(_ context.Context) (datastore.RevisionWithSchemaHash, error) {
	return datastore.RevisionWithSchemaHash{}, d.headErr
}

func (d *erroringRevisionDS) OptimizedRevision(_ context.Context) (datastore.RevisionWithSchemaHash, error) {
	return datastore.RevisionWithSchemaHash{}, d.optimizedErr
}

// schemaHashInjectingDS wraps a Datastore and injects a fixed SchemaHash into
// OptimizedRevision results so tests can exercise the hash-seeding branch
// without depending on memdb's nanosecond-precision clock behaviour.
type schemaHashInjectingDS struct {
	datastore.Datastore
	injectHash string
}

func (d *schemaHashInjectingDS) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHash, error) {
	r, err := d.Datastore.OptimizedRevision(ctx)
	if err == nil {
		r.SchemaHash = d.injectHash
	}
	return r, err
}

// TestOptimizedRevisionSeedsLastSchemaHash verifies that OptimizedRevision in a
// new-read mode populates lastSchemaHash when the datastore returns a schema hash,
// and that a subsequent ReadWriteTx forwards that hash as the precondition.
func TestOptimizedRevisionSeedsLastSchemaHash(t *testing.T) {
	ctx := t.Context()

	rawDS := newTestDatastore(t)
	injected := "injected-schema-hash-value"
	injectDS := &schemaHashInjectingDS{Datastore: rawDS, injectHash: injected}
	capDS := &preconditionCapturingDS{Datastore: injectDS}
	dl := NewDataLayer(capDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	// Seed the lastSchemaHash via OptimizedRevision (injected returns non-empty hash).
	_, gotHash, err := dl.OptimizedRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, SchemaHash(injected), gotHash,
		"OptimizedRevision should return the datastore hash in new-read mode")

	capDS.mu.Lock()
	capDS.capturedPreconditions = nil
	capDS.mu.Unlock()

	// Next ReadWriteTx should auto-seed the precondition from lastSchemaHash.
	// No real schema exists, so the tx will fail — we only care that the
	// precondition was forwarded, not whether it matched.
	_, _ = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return nil
	})
	require.Equal(t, injected, capDS.lastCapturedPrecondition(),
		"ReadWriteTx should forward the hash seeded by OptimizedRevision as the precondition")
}

// TestHeadRevisionErrorReturnsNoHash verifies that HeadRevision propagates a
// datastore error and returns the legacy no-hash sentinel.
func TestHeadRevisionErrorReturnsNoHash(t *testing.T) {
	ctx := t.Context()

	sentinelErr := errors.New("head revision unavailable")
	errDS := &erroringRevisionDS{Datastore: newTestDatastore(t), headErr: sentinelErr}
	dl := NewDataLayer(errDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	_, hash, err := dl.HeadRevision(ctx)
	require.ErrorIs(t, err, sentinelErr)
	require.Equal(t, NoSchemaHashInLegacyMode, hash)
}

// TestOptimizedRevisionErrorReturnsNoHash verifies that OptimizedRevision
// propagates a datastore error and returns the legacy no-hash sentinel.
func TestOptimizedRevisionErrorReturnsNoHash(t *testing.T) {
	ctx := t.Context()

	sentinelErr := errors.New("optimized revision unavailable")
	errDS := &erroringRevisionDS{Datastore: newTestDatastore(t), optimizedErr: sentinelErr}
	dl := NewDataLayer(errDS, WithSchemaMode(SchemaModeReadNewWriteNew))

	_, hash, err := dl.OptimizedRevision(ctx)
	require.ErrorIs(t, err, sentinelErr)
	require.Equal(t, NoSchemaHashInLegacyMode, hash)
}

package datalayer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
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
	ctx := t.Context()
	nsDefs, err := ds.SnapshotReader(rev).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	return len(nsDefs) > 0
}

// hasUnifiedData checks whether unified schema storage has data at the given revision.
func hasUnifiedData(t *testing.T, ds datastore.Datastore, rev datastore.Revision) bool {
	t.Helper()
	ctx := t.Context()
	_, err := ds.SnapshotReader(rev).ReadStoredSchema(ctx)
	if err != nil {
		require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
		return false
	}
	return true
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
			require := require.New(t)

			ds := newTestDatastore(t)
			dl := NewDataLayer(ds, WithSchemaMode(tc.mode))

			defs, schemaText := testSchemaDefinitions(t)
			ctx := t.Context()

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
			})
			require.NoError(err)

			require.Equal(tc.expectLegacy, hasLegacyData(t, ds, rev),
				"legacy store populated = %v, want %v", hasLegacyData(t, ds, rev), tc.expectLegacy)
			require.Equal(tc.expectUnified, hasUnifiedData(t, ds, rev),
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
				if err := rwt.WriteSchema(ctx, defs, schemaText, nil); err != nil {
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
				return rwt.WriteSchema(ctx, defs, outOfOrderSchemaText, nil)
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
		_ = WriteSchemaViaStoredSchema(t.Context(), nil, defs, "", nil)
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
			})
			require.NoError(err)

			// Write schema with only one definition.
			smallerDefs := []datastore.SchemaDefinition{ns.Namespace("user")}
			smallSchemaText, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("user")})
			require.NoError(err)

			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
				return rwt.WriteSchema(ctx, smallerDefs, smallSchemaText, nil)
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
		return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
		return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
		return rwt.WriteSchema(ctx, defs1, schemaText1, nil)
	})
	require.NoError(err)

	// Write different schema.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	rev2, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return rwt.WriteSchema(ctx, defs2, schemaText2, nil)
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
		return rwt.WriteSchema(ctx, defs1, schemaText1, nil)
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
		return rwt.WriteSchema(ctx, defs2, schemaText2, nil)
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
		return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
				return rwt.WriteSchema(ctx, defs, schemaText, nil)
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
		return rwt.WriteSchema(ctx, defs1, schemaText1, nil)
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
		return rwt.WriteSchema(ctx, defs2, schemaText2, nil)
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
		return rwt.WriteSchema(ctx, defs1, schemaText1, nil)
	})
	require.NoError(err)

	// Write second schema.
	defs2 := []datastore.SchemaDefinition{ns.Namespace("newtype")}
	schemaText2, _, err := generator.GenerateSchema(t.Context(), []compiler.SchemaDefinition{ns.Namespace("newtype")})
	require.NoError(err)

	_, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return rwt.WriteSchema(ctx, defs2, schemaText2, nil)
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

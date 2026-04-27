package test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
)

// toSchemaDefinitions converts compiler.SchemaDefinition slice to datastore.SchemaDefinition slice.
func toSchemaDefinitions(defs []compiler.SchemaDefinition) []datastore.SchemaDefinition {
	result := make([]datastore.SchemaDefinition, len(defs))
	for i, def := range defs {
		result[i] = def.(datastore.SchemaDefinition)
	}
	return result
}

// writeSchema is a test helper that writes a schema via WriteSchemaViaStoredSchema.
func writeSchema(ctx context.Context, t *testing.T, ds datastore.Datastore,
	definitions []compiler.SchemaDefinition, schemaText string,
) datastore.Revision {
	t.Helper()
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return datalayer.WriteSchemaViaStoredSchema(ctx, rwt, toSchemaDefinitions(definitions), schemaText, nil)
	})
	require.NoError(t, err)
	return rev
}

// StoredSchemaNotFoundTest tests that reading a stored schema when none exists returns ErrSchemaNotFound.
func StoredSchemaNotFoundTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	startRevisionResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	startRevision := startRevisionResult.Revision

	_, err = ds.SnapshotReader(startRevision).ReadStoredSchema(ctx)
	require.ErrorIs(err, datastore.ErrSchemaNotFound)
}

// StoredSchemaWriteReadTest tests basic write and read of a stored schema,
// including both namespace and caveat definitions.
func StoredSchemaWriteReadTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	schemaString := `caveat is_allowed(allowed bool) {
	allowed
}

definition user {}

definition document {
	relation viewer: user with is_allowed
}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	allDefs := make([]compiler.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, caveatDef := range compiled.CaveatDefinitions {
		allDefs = append(allDefs, caveatDef)
	}
	for _, objDef := range compiled.ObjectDefinitions {
		allDefs = append(allDefs, objDef)
	}

	writtenRev := writeSchema(ctx, t, ds, allDefs, schemaString)

	// Read it back at the written revision.
	readSchema, err := ds.SnapshotReader(writtenRev).ReadStoredSchema(ctx)
	require.NoError(err)

	// Verify the stored schema contents.
	require.EqualValues(1, readSchema.Get().Version)
	v1 := readSchema.Get().GetV1()
	require.NotNil(v1)
	require.Equal(schemaString, v1.SchemaText)
	require.NotEmpty(v1.SchemaHash)
	require.Len(v1.NamespaceDefinitions, 2)
	require.Contains(v1.NamespaceDefinitions, "user")
	require.Contains(v1.NamespaceDefinitions, "document")
	require.Len(v1.CaveatDefinitions, 1)
	require.Contains(v1.CaveatDefinitions, "is_allowed")

	// Verify the namespace definitions are faithfully round-tripped.
	for _, objDef := range compiled.ObjectDefinitions {
		readDef, ok := v1.NamespaceDefinitions[objDef.Name]
		require.True(ok)
		testutil.RequireProtoEqual(t, objDef, readDef, "namespace %s should round-trip", objDef.Name)
	}

	// Verify the caveat definition round-trips.
	testutil.RequireProtoEqual(t, compiled.CaveatDefinitions[0], v1.CaveatDefinitions["is_allowed"],
		"caveat definition should round-trip")
}

// StoredSchemaRevisionTest writes three schema versions, then reads all of them
// back (including older revisions after newer ones have been written) to verify
// that each revision returns the correct content.
func StoredSchemaRevisionTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	// Write first schema: just "user".
	firstDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
	}
	firstText, _, err := generator.GenerateSchema(ctx, firstDefs)
	require.NoError(err)
	firstRev := writeSchema(ctx, t, ds, firstDefs, firstText)

	// Write second schema: "user" + "document" with viewer.
	secondDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
		),
	}
	secondText, _, err := generator.GenerateSchema(ctx, secondDefs)
	require.NoError(err)
	secondRev := writeSchema(ctx, t, ds, secondDefs, secondText)
	require.True(secondRev.GreaterThan(firstRev))

	// Write third schema: "user" + "document" with viewer+editor.
	thirdDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "...")),
		),
	}
	thirdText, _, err := generator.GenerateSchema(ctx, thirdDefs)
	require.NoError(err)
	thirdRev := writeSchema(ctx, t, ds, thirdDefs, thirdText)
	require.True(thirdRev.GreaterThan(secondRev))

	// Now read ALL revisions back, starting with the oldest, to verify that
	// reading older revisions after newer writes still returns the old content.
	readFirst, err := ds.SnapshotReader(firstRev).ReadStoredSchema(ctx)
	require.NoError(err)
	require.NotEmpty(readFirst.Get().GetV1().SchemaHash)
	require.Len(readFirst.Get().GetV1().NamespaceDefinitions, 1)
	require.Contains(readFirst.Get().GetV1().NamespaceDefinitions, "user")

	readSecond, err := ds.SnapshotReader(secondRev).ReadStoredSchema(ctx)
	require.NoError(err)
	require.NotEmpty(readSecond.Get().GetV1().SchemaHash)
	require.Len(readSecond.Get().GetV1().NamespaceDefinitions, 2)
	require.Contains(readSecond.Get().GetV1().NamespaceDefinitions, "user")
	require.Contains(readSecond.Get().GetV1().NamespaceDefinitions, "document")
	require.Len(readSecond.Get().GetV1().NamespaceDefinitions["document"].Relation, 1,
		"document should have only viewer at second revision")

	readThird, err := ds.SnapshotReader(thirdRev).ReadStoredSchema(ctx)
	require.NoError(err)
	require.NotEmpty(readThird.Get().GetV1().SchemaHash)
	require.Len(readThird.Get().GetV1().NamespaceDefinitions, 2)
	require.Len(readThird.Get().GetV1().NamespaceDefinitions["document"].Relation, 2,
		"document should have viewer and editor at third revision")

	// Verify all three revisions have different hashes (different schemas).
	require.NotEqual(readFirst.Get().GetV1().SchemaHash, readSecond.Get().GetV1().SchemaHash)
	require.NotEqual(readSecond.Get().GetV1().SchemaHash, readThird.Get().GetV1().SchemaHash)
}

// StoredSchemaUpdateTest tests that overwriting a stored schema replaces it completely.
func StoredSchemaUpdateTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	// Write initial schema with 2 relations.
	initialDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "...")),
		),
	}
	initialText, _, err := generator.GenerateSchema(ctx, initialDefs)
	require.NoError(err)
	firstRev := writeSchema(ctx, t, ds, initialDefs, initialText)

	// Update schema: add an "owner" relation.
	updatedDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("owner", nil, ns.AllowedRelation("user", "...")),
		),
	}
	updatedText, _, err := generator.GenerateSchema(ctx, updatedDefs)
	require.NoError(err)
	secondRev := writeSchema(ctx, t, ds, updatedDefs, updatedText)
	require.True(secondRev.GreaterThan(firstRev))

	// At first revision: 2 relations.
	readFirst, err := ds.SnapshotReader(firstRev).ReadStoredSchema(ctx)
	require.NoError(err)
	docFirst := readFirst.Get().GetV1().NamespaceDefinitions["document"]
	require.Len(docFirst.Relation, 2)

	// At second revision: 3 relations.
	readSecond, err := ds.SnapshotReader(secondRev).ReadStoredSchema(ctx)
	require.NoError(err)
	docSecond := readSecond.Get().GetV1().NamespaceDefinitions["document"]
	require.Len(docSecond.Relation, 3)

	ownerFound := false
	for _, rel := range docSecond.Relation {
		if rel.Name == "owner" {
			ownerFound = true
			break
		}
	}
	require.True(ownerFound, "owner relation should exist in updated schema")
}

// StoredSchemaMultipleRevisionsTest writes multiple schema versions and verifies each revision
// can still be read independently.
func StoredSchemaMultipleRevisionsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	const numVersions = 5

	type versionData struct {
		revision   datastore.Revision
		schemaText string
		numDefs    int
	}
	versions := make([]versionData, 0, numVersions)

	for i := range numVersions {
		defs := []compiler.SchemaDefinition{
			ns.Namespace("user"),
			ns.Namespace(fmt.Sprintf("resource_%d", i),
				ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			),
		}

		schemaText, _, err := generator.GenerateSchema(ctx, defs)
		require.NoError(err)

		rev := writeSchema(ctx, t, ds, defs, schemaText)

		versions = append(versions, versionData{
			revision:   rev,
			schemaText: schemaText,
			numDefs:    len(defs),
		})
	}

	// Verify each revision independently.
	for i, v := range versions {
		readSchema, err := ds.SnapshotReader(v.revision).ReadStoredSchema(ctx)
		require.NoError(err, "failed to read schema at version %d", i)
		require.Equal(v.schemaText, readSchema.Get().GetV1().SchemaText,
			"schema text mismatch at version %d", i)
		require.Len(readSchema.Get().GetV1().NamespaceDefinitions, v.numDefs,
			"namespace count mismatch at version %d", i)
	}
}

// StoredSchemaReadWithinTransactionTest tests that ReadStoredSchema works within a
// read-write transaction and sees the schema written in the same transaction.
func StoredSchemaReadWithinTransactionTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	defs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
	}
	schemaText, _, err := generator.GenerateSchema(ctx, defs)
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Write within the transaction.
		if err := datalayer.WriteSchemaViaStoredSchema(ctx, rwt, toSchemaDefinitions(defs), schemaText, nil); err != nil {
			return err
		}

		// Read within the same transaction.
		readSchema, err := rwt.ReadStoredSchema(ctx)
		if err != nil {
			return err
		}

		require.Equal(schemaText, readSchema.Get().GetV1().SchemaText)
		return nil
	})
	require.NoError(err)
}

// StoredSchemaStableTextTest verifies that a schema with multiple namespaces and caveats
// produces stable text when definitions are sorted before generation, regardless of the
// initial ordering of definitions.
func StoredSchemaStableTextTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	schemaString := `caveat beta_caveat(flag bool) {
	flag
}

caveat alpha_caveat(allowed bool) {
	allowed
}

definition zebra {}

definition apple {
	relation viewer: zebra with alpha_caveat
}

definition middle {
	relation editor: zebra with beta_caveat
}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	// Build definitions in unsorted order (caveats first, then objects, as-compiled).
	unsortedDefs := make([]compiler.SchemaDefinition, 0,
		len(compiled.CaveatDefinitions)+len(compiled.ObjectDefinitions))
	for _, cd := range compiled.CaveatDefinitions {
		unsortedDefs = append(unsortedDefs, cd)
	}
	for _, od := range compiled.ObjectDefinitions {
		unsortedDefs = append(unsortedDefs, od)
	}

	// sortDefinitions returns a new slice with caveats sorted by name first,
	// then namespaces sorted by name.
	sortDefinitions := func(defs []compiler.SchemaDefinition) []compiler.SchemaDefinition {
		var caveatDefs, nsDefs []compiler.SchemaDefinition
		for _, def := range defs {
			switch def.(type) {
			case *core.CaveatDefinition:
				caveatDefs = append(caveatDefs, def)
			case *core.NamespaceDefinition:
				nsDefs = append(nsDefs, def)
			}
		}
		sort.Slice(caveatDefs, func(i, j int) bool {
			return caveatDefs[i].GetName() < caveatDefs[j].GetName()
		})
		sort.Slice(nsDefs, func(i, j int) bool {
			return nsDefs[i].GetName() < nsDefs[j].GetName()
		})

		sorted := make([]compiler.SchemaDefinition, 0, len(defs))
		sorted = append(sorted, caveatDefs...)
		sorted = append(sorted, nsDefs...)
		return sorted
	}

	sortedDefs := sortDefinitions(unsortedDefs)

	// Generate text from sorted definitions.
	sortedText, _, err := generator.GenerateSchema(ctx, sortedDefs)
	require.NoError(err)

	// Write the stored schema with sorted text.
	writtenRev := writeSchema(ctx, t, ds, sortedDefs, sortedText)

	// Read it back and verify text is preserved.
	readSchema, err := ds.SnapshotReader(writtenRev).ReadStoredSchema(ctx)
	require.NoError(err)
	require.Equal(sortedText, readSchema.Get().GetV1().SchemaText)

	// Generate text again from the same definitions in a different initial order
	// (reversed), sort, and verify the generated text is identical.
	reversedDefs := make([]compiler.SchemaDefinition, 0, len(unsortedDefs))
	for i := len(unsortedDefs) - 1; i >= 0; i-- {
		reversedDefs = append(reversedDefs, unsortedDefs[i])
	}

	reSortedDefs := sortDefinitions(reversedDefs)
	reSortedText, _, err := generator.GenerateSchema(ctx, reSortedDefs)
	require.NoError(err)
	require.Equal(sortedText, reSortedText,
		"text generated from differently-ordered definitions should be identical after sorting")
}

// StoredSchemaPhaseMigrationTest tests reading and writing schema through all four
// schema mode phases, simulating a live migration from legacy to unified storage.
// Each phase creates a new DataLayer on the same underlying datastore. For phases 2-4,
// it first reads the schema written by the previous phase to verify continuity, then
// writes an updated schema and verifies the change is reflected.
func StoredSchemaPhaseMigrationTest(t *testing.T, tester DatastoreTester) {
	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ctx := t.Context()

	// Each phase writes a different schema so we can verify the update is visible.
	// Caveats are included to verify they are properly handled across phases.
	phaseSchemas := []string{
		// Phase 1: base schema with a caveat
		`caveat is_public(public bool) {
	public
}

definition user {}

definition document {
	relation viewer: user with is_public
}`,
		// Phase 2: add editor relation and a second caveat
		`caveat is_public(public bool) {
	public
}

caveat has_access(allowed bool) {
	allowed
}

definition user {}

definition document {
	relation viewer: user with is_public
	relation editor: user with has_access
}`,
		// Phase 3: add permission
		`caveat is_public(public bool) {
	public
}

caveat has_access(allowed bool) {
	allowed
}

definition user {}

definition document {
	relation viewer: user with is_public
	relation editor: user with has_access
	permission view = viewer + editor
}`,
		// Phase 4: add a new type, remove first caveat
		`caveat has_access(allowed bool) {
	allowed
}

definition user {}

definition group {
	relation member: user
}

definition document {
	relation viewer: user | group#member
	relation editor: user with has_access
	permission view = viewer + editor
}`,
	}

	phases := []struct {
		name            string
		mode            datalayer.SchemaMode
		hasLegacy       bool
		hasUnified      bool
		expectedTypes   []string
		expectedCaveats []string
	}{
		{
			name:            "Phase1_ReadLegacyWriteLegacy",
			mode:            datalayer.SchemaModeReadLegacyWriteLegacy,
			hasLegacy:       true,
			hasUnified:      false,
			expectedTypes:   []string{"document", "user"},
			expectedCaveats: []string{"is_public"},
		},
		{
			name:            "Phase2_ReadLegacyWriteBoth",
			mode:            datalayer.SchemaModeReadLegacyWriteBoth,
			hasLegacy:       true,
			hasUnified:      true,
			expectedTypes:   []string{"document", "user"},
			expectedCaveats: []string{"has_access", "is_public"},
		},
		{
			name:            "Phase3_ReadNewWriteBoth",
			mode:            datalayer.SchemaModeReadNewWriteBoth,
			hasLegacy:       true,
			hasUnified:      true,
			expectedTypes:   []string{"document", "user"},
			expectedCaveats: []string{"has_access", "is_public"},
		},
		{
			name:            "Phase4_ReadNewWriteNew",
			mode:            datalayer.SchemaModeReadNewWriteNew,
			hasLegacy:       false,
			hasUnified:      true,
			expectedTypes:   []string{"document", "group", "user"},
			expectedCaveats: []string{"has_access"},
		},
	}

	// Track the last revision and expected types/caveats so subsequent phases can verify reads.
	var lastRev datastore.Revision
	var prevExpectedTypes []string
	var prevExpectedCaveats []string

	for i, phase := range phases {
		schemaText := phaseSchemas[i]
		compiled, err := compiler.Compile(compiler.InputSchema{
			Source:       input.Source("schema"),
			SchemaString: schemaText,
		}, compiler.AllowUnprefixedObjectType())
		require.NoError(t, err)

		allDefs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
		for _, caveatDef := range compiled.CaveatDefinitions {
			allDefs = append(allDefs, caveatDef)
		}
		for _, objDef := range compiled.ObjectDefinitions {
			allDefs = append(allDefs, objDef)
		}

		// Create a fresh DataLayer for this phase on the same underlying datastore.
		dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(phase.mode))

		// For phases 2+, verify we can read the schema written by the previous phase
		// before writing anything new. This validates cross-phase read continuity.
		if i > 0 {
			t.Run(phase.name+"/ReadFromPreviousPhase", func(t *testing.T) {
				verifySchemaTypes(t, ctx, dl, lastRev, prevExpectedTypes, prevExpectedCaveats)
			})
		}

		// Write schema through the datalayer.
		t.Run(phase.name+"/Write", func(t *testing.T) {
			rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
				return rwt.WriteSchema(ctx, allDefs, schemaText, caveattypes.Default.TypeSet)
			})
			require.NoError(t, err)
			lastRev = rev

			// Verify legacy storage state.
			legacyNsDefs, err := ds.SnapshotReader(rev).LegacyListAllNamespaces(ctx)
			require.NoError(t, err)
			if phase.hasLegacy {
				require.NotEmpty(t, legacyNsDefs, "phase %d: expected legacy data", i+1)
			}

			legacyCaveatDefs, err := ds.SnapshotReader(rev).LegacyListAllCaveats(ctx)
			require.NoError(t, err)
			if phase.hasLegacy {
				require.NotEmpty(t, legacyCaveatDefs, "phase %d: expected legacy caveat data", i+1)
			}

			// Verify unified storage state.
			storedSchema, err := ds.SnapshotReader(rev).ReadStoredSchema(ctx)
			if phase.hasUnified {
				require.NoError(t, err)
				require.NotNil(t, storedSchema)
				require.NotEmpty(t, storedSchema.Get().GetV1().SchemaText)
			} else {
				require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
			}
		})

		// Read back through the datalayer after writing and verify the new schema is visible.
		t.Run(phase.name+"/ReadAfterWrite", func(t *testing.T) {
			verifySchemaTypes(t, ctx, dl, lastRev, phase.expectedTypes, phase.expectedCaveats)
		})

		prevExpectedTypes = phase.expectedTypes
		prevExpectedCaveats = phase.expectedCaveats
	}
}

// StoredSchemaLargeTest generates a large schema (>2MB) with 4000 types, each having 20 relations,
// writes it, reads it back, and verifies the exact match.
func StoredSchemaLargeTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	// Build a large schema with 4000 types, each with 20 relations.
	const numTypes = 4000
	const numRelations = 20

	allDefs := make([]compiler.SchemaDefinition, 0, numTypes)
	for i := range numTypes {
		typeName := fmt.Sprintf("type%04d", i)
		rels := make([]*core.Relation, 0, numRelations)
		for j := range numRelations {
			relName := fmt.Sprintf("rel_%d", j)
			rels = append(rels, ns.MustRelation(relName, nil, ns.AllowedRelation(typeName, "...")))
		}
		allDefs = append(allDefs, ns.Namespace(typeName, rels...))
	}

	schemaText, _, err := generator.GenerateSchema(ctx, allDefs)
	require.NoError(err)

	// Verify it is indeed >2MB.
	require.Greater(len(schemaText), 2*1024*1024, "generated schema should exceed 2MB")

	writtenRev := writeSchema(ctx, t, ds, allDefs, schemaText)

	// Read back and verify exact match.
	readSchema, err := ds.SnapshotReader(writtenRev).ReadStoredSchema(ctx)
	require.NoError(err)
	require.Equal(schemaText, readSchema.Get().GetV1().SchemaText)
	require.Len(readSchema.Get().GetV1().NamespaceDefinitions, numTypes)
}

// HeadRevisionSchemaHashTest verifies that HeadRevision returns the correct schema hash
// after writing a stored schema, and that updating the schema changes the hash.
func HeadRevisionSchemaHashTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	// Before any schema is written, the hash should be empty.
	resultBefore, err := ds.HeadRevision(ctx)
	require.NoError(err)
	require.Empty(resultBefore.SchemaHash, "schema hash should be empty before any schema is written")

	// Write a schema.
	firstDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
		),
	}
	firstSchemaText, _, err := generator.GenerateSchema(ctx, firstDefs)
	require.NoError(err)

	writeSchema(ctx, t, ds, firstDefs, firstSchemaText)

	// HeadRevision should now return the hash matching the written schema.
	resultAfterFirst, err := ds.HeadRevision(ctx)
	require.NoError(err)

	expectedFirstHash, err := generator.ComputeSchemaHash(firstDefs)
	require.NoError(err)

	require.Equal(expectedFirstHash, resultAfterFirst.SchemaHash,
		"HeadRevision should return the schema hash matching the written schema")

	// Write a different schema.
	secondDefs := []compiler.SchemaDefinition{
		ns.Namespace("team"),
	}
	secondSchemaText, _, err := generator.GenerateSchema(ctx, secondDefs)
	require.NoError(err)

	writeSchema(ctx, t, ds, secondDefs, secondSchemaText)

	// HeadRevision should now return the updated hash.
	resultAfterSecond, err := ds.HeadRevision(ctx)
	require.NoError(err)

	expectedSecondHash, err := generator.ComputeSchemaHash(secondDefs)
	require.NoError(err)

	require.Equal(expectedSecondHash, resultAfterSecond.SchemaHash,
		"HeadRevision should return the updated schema hash after a schema change")
	require.NotEqual(resultAfterFirst.SchemaHash, resultAfterSecond.SchemaHash,
		"schema hashes should differ after writing a different schema")
}

// OptimizedRevisionSchemaHashTest verifies that OptimizedRevision returns the
// correct schema hash after a schema has been written, and that the hash
// updates when the schema changes. This is critical for the stored-schema
// cache to be hit on the common consistency paths (MinimizeLatency /
// AtLeastAsFresh), which route through OptimizedRevision rather than
// HeadRevision.
func OptimizedRevisionSchemaHashTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	ds, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ctx := t.Context()

	// Write a schema.
	firstDefs := []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
		),
	}
	firstSchemaText, _, err := generator.GenerateSchema(ctx, firstDefs)
	require.NoError(err)

	writeSchema(ctx, t, ds, firstDefs, firstSchemaText)

	expectedFirstHash, err := generator.ComputeSchemaHash(firstDefs)
	require.NoError(err)

	resultAfterFirst, err := ds.OptimizedRevision(ctx)
	require.NoError(err)
	require.Equal(expectedFirstHash, resultAfterFirst.SchemaHash,
		"OptimizedRevision should return the schema hash matching the written schema")

	// Write a different schema.
	secondDefs := []compiler.SchemaDefinition{
		ns.Namespace("team"),
	}
	secondSchemaText, _, err := generator.GenerateSchema(ctx, secondDefs)
	require.NoError(err)

	writeSchema(ctx, t, ds, secondDefs, secondSchemaText)

	expectedSecondHash, err := generator.ComputeSchemaHash(secondDefs)
	require.NoError(err)

	resultAfterSecond, err := ds.OptimizedRevision(ctx)
	require.NoError(err)
	require.Equal(expectedSecondHash, resultAfterSecond.SchemaHash,
		"OptimizedRevision should return the updated schema hash after a schema change")
	require.NotEqual(resultAfterFirst.SchemaHash, resultAfterSecond.SchemaHash,
		"OptimizedRevision schema hashes should differ after writing a different schema")
}

// verifySchemaTypes reads schema through the datalayer at the given revision
// and verifies the expected type definition and caveat definition names are present.
func verifySchemaTypes(t *testing.T, ctx context.Context, dl datalayer.DataLayer, rev datastore.Revision, expectedTypes []string, expectedCaveats []string) {
	t.Helper()

	reader := dl.SnapshotReader(rev, datalayer.NoSchemaHashForTesting)
	schemaReader, err := reader.ReadSchema(ctx)
	require.NoError(t, err)

	readText, err := schemaReader.SchemaText(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, readText)

	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(t, err)
	require.Len(t, typeDefs, len(expectedTypes))

	typeNames := make([]string, 0, len(typeDefs))
	for _, td := range typeDefs {
		typeNames = append(typeNames, td.Definition.Name)
	}
	sort.Strings(typeNames)
	require.Equal(t, expectedTypes, typeNames)

	caveatDefs, err := schemaReader.ListAllCaveatDefinitions(ctx)
	require.NoError(t, err)
	require.Len(t, caveatDefs, len(expectedCaveats))

	caveatNames := make([]string, 0, len(caveatDefs))
	for _, cd := range caveatDefs {
		caveatNames = append(caveatNames, cd.Definition.Name)
	}
	sort.Strings(caveatNames)
	require.Equal(t, expectedCaveats, caveatNames)
}

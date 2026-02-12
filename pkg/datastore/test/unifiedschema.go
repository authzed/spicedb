package test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/diff"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

var (
	testSchemaDefinitions = []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "...")),
		),
	}

	updatedSchemaDefinitions = []compiler.SchemaDefinition{
		ns.Namespace("user"),
		ns.Namespace("document",
			ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("editor", nil, ns.AllowedRelation("user", "...")),
			ns.MustRelation("owner", nil, ns.AllowedRelation("user", "...")),
		),
	}
)

// computeExpectedSchemaHash computes the expected schema hash by sorting definitions
// by name (matching datastore behavior) and then hashing the generated schema text.
func computeExpectedSchemaHash(t *testing.T, definitions []compiler.SchemaDefinition) string {
	// Sort definitions by name for consistent ordering (matches datastore behavior)
	sortedDefs := make([]compiler.SchemaDefinition, len(definitions))
	copy(sortedDefs, definitions)
	sort.Slice(sortedDefs, func(i, j int) bool {
		return sortedDefs[i].GetName() < sortedDefs[j].GetName()
	})

	// Generate schema text from sorted definitions
	schemaText, _, err := generator.GenerateSchema(sortedDefs)
	require.NoError(t, err)

	// Compute SHA256 hash
	hashBytes := sha256.Sum256([]byte(schemaText))
	return hex.EncodeToString(hashBytes[:])
}

// requireSchemasEqual compares two schema texts semantically using the diff engine.
// This allows schemas to be equivalent even if definitions are in different order.
func requireSchemasEqual(t *testing.T, expected, actual string) {
	require := require.New(t)

	// Compile both schemas
	expectedCompiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "expected",
		SchemaString: expected,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err, "failed to compile expected schema")

	actualCompiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "actual",
		SchemaString: actual,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err, "failed to compile actual schema")

	// Create diffable schemas
	expectedDiffable := diff.NewDiffableSchemaFromCompiledSchema(expectedCompiled)
	actualDiffable := diff.NewDiffableSchemaFromCompiledSchema(actualCompiled)

	// Compute diff
	schemaDiff, err := diff.DiffSchemas(expectedDiffable, actualDiffable, nil)
	require.NoError(err, "failed to diff schemas")

	// Check that there are no differences
	require.Empty(schemaDiff.AddedNamespaces, "unexpected added namespaces")
	require.Empty(schemaDiff.RemovedNamespaces, "unexpected removed namespaces")
	require.Empty(schemaDiff.AddedCaveats, "unexpected added caveats")
	require.Empty(schemaDiff.RemovedCaveats, "unexpected removed caveats")
	require.Empty(schemaDiff.ChangedNamespaces, "unexpected changed namespaces")
	require.Empty(schemaDiff.ChangedCaveats, "unexpected changed caveats")
}

// UnifiedSchemaTest tests basic unified schema storage functionality
// Note: This test assumes the datastore's readers and transactions support DualSchema interfaces.
// The specific schema mode (read legacy, write both, etc.) is configured at datastore
// initialization time by the specific datastore tests.
func UnifiedSchemaTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore readers and transactions support schema operations
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Check if transaction supports schema writer
	var schemaWriterErr error
	_, _ = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, schemaWriterErr = rwt.SchemaWriter()
		return nil
	})
	require.NoError(schemaWriterErr, "datastore transaction must provide SchemaWriter")

	// Get starting revision
	startRevision, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// Generate schema text
	schemaText, _, err := generator.GenerateSchema(testSchemaDefinitions)
	require.NoError(err)

	// Convert to datastore.SchemaDefinition by casting each element
	defs := make([]datastore.SchemaDefinition, 0, len(testSchemaDefinitions))
	for _, def := range testSchemaDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}

	// Write schema
	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)
	require.True(startRevision.LessThan(writtenRev))

	// Read schema using SchemaReader
	reader = ds.SnapshotReader(writtenRev, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	// Verify schema text (using semantic comparison to allow for different definition order)
	readSchemaText, err := schemaReader.SchemaText()
	require.NoError(err)
	requireSchemasEqual(t, schemaText, readSchemaText)

	// Verify namespace definitions
	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs, 2)

	userFound := false
	docFound := false
	for _, def := range typeDefs {
		switch def.Definition.Name {
		case "user":
			userFound = true
			require.NotNil(def.LastWrittenRevision)
		case "document":
			docFound = true
			require.NotNil(def.LastWrittenRevision)
		}
	}
	require.True(userFound, "user namespace should be found")
	require.True(docFound, "document namespace should be found")

	// Lookup individual namespace
	docDef, found, err := schemaReader.LookupTypeDefByName(ctx, "document")
	require.NoError(err)
	require.True(found)
	require.Equal("document", docDef.Definition.Name)
}

// UnifiedSchemaUpdateTest tests updating schemas
func UnifiedSchemaUpdateTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Write initial schema
	schemaText, _, err := generator.GenerateSchema(testSchemaDefinitions)
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(testSchemaDefinitions))
	for _, def := range testSchemaDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}

	firstRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)

	// Update schema with additional relation
	updatedText, _, err := generator.GenerateSchema(updatedSchemaDefinitions)
	require.NoError(err)

	updatedDefs := make([]datastore.SchemaDefinition, 0, len(updatedSchemaDefinitions))
	for _, def := range updatedSchemaDefinitions {
		updatedDefs = append(updatedDefs, def.(datastore.SchemaDefinition))
	}

	secondRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, updatedDefs, updatedText, nil)
	})
	require.NoError(err)
	require.True(secondRev.GreaterThan(firstRev))

	// Read at first revision - should see old schema
	reader1 := ds.SnapshotReader(firstRev, datastore.NoSchemaHashForTesting)
	schemaReader1, err := reader1.SchemaReader()
	require.NoError(err)

	typeDefs1, err := schemaReader1.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs1, 2)

	docDef1, found, err := schemaReader1.LookupTypeDefByName(ctx, "document")
	require.NoError(err)
	require.True(found)
	require.Len(docDef1.Definition.Relation, 2, "should have 2 relations at first revision")

	// Read at second revision - should see updated schema
	reader2 := ds.SnapshotReader(secondRev, datastore.NoSchemaHashForTesting)
	schemaReader2, err := reader2.SchemaReader()
	require.NoError(err)

	docDef2, found, err := schemaReader2.LookupTypeDefByName(ctx, "document")
	require.NoError(err)
	require.True(found)
	require.Len(docDef2.Definition.Relation, 3, "should have 3 relations at second revision")

	// Verify owner relation exists in updated schema
	ownerFound := false
	for _, rel := range docDef2.Definition.Relation {
		if rel.Name == "owner" {
			ownerFound = true
			break
		}
	}
	require.True(ownerFound, "owner relation should exist in updated schema")
}

// UnifiedSchemaRevisionTest verifies that schema revisions are tracked correctly
func UnifiedSchemaRevisionTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Write schema
	schemaText, _, err := generator.GenerateSchema(testSchemaDefinitions)
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(testSchemaDefinitions))
	for _, def := range testSchemaDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}

	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)

	// Read schema and verify all definitions have consistent revisions
	reader = ds.SnapshotReader(writtenRev, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs, 2)

	// All definitions should have valid revisions
	for _, def := range typeDefs {
		require.NotNil(def.LastWrittenRevision,
			"definition revision should be set")
	}
}

// UnifiedSchemaWithCaveatsTest tests unified schema with caveats
func UnifiedSchemaWithCaveatsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Define schema with caveat
	schemaTextWithCaveat := `caveat is_allowed(allowed bool) {
	allowed
}

definition user {}

definition document {
	relation viewer: user with is_allowed
}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "schema",
		SchemaString: schemaTextWithCaveat,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, objDef := range compiled.ObjectDefinitions {
		defs = append(defs, objDef)
	}
	for _, caveatDef := range compiled.CaveatDefinitions {
		defs = append(defs, caveatDef)
	}

	// Write schema
	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaTextWithCaveat, nil)
	})
	require.NoError(err)

	// Read schema
	reader = ds.SnapshotReader(writtenRev, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	// Verify caveat
	caveats, err := schemaReader.ListAllCaveatDefinitions(ctx)
	require.NoError(err)
	require.Len(caveats, 1)
	require.Equal("is_allowed", caveats[0].Definition.Name)
	require.NotNil(caveats[0].LastWrittenRevision)

	// Lookup caveat by name
	caveat, found, err := schemaReader.LookupCaveatDefByName(ctx, "is_allowed")
	require.NoError(err)
	require.True(found)
	require.Equal("is_allowed", caveat.Definition.Name)

	// Verify namespace definitions
	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Len(typeDefs, 2)
}

// UnifiedSchemaEmptyTest tests reading when no schema exists
func UnifiedSchemaEmptyTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	startRevision, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader = ds.SnapshotReader(startRevision, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	// Schema text should be empty or return error - we don't check the result
	// since different datastores may behave differently with no schema
	_, _ = schemaReader.SchemaText()

	// Should have no type definitions initially
	typeDefs, err := schemaReader.ListAllTypeDefinitions(ctx)
	require.NoError(err)
	require.Empty(typeDefs)

	// Should have no caveat definitions initially
	caveats, err := schemaReader.ListAllCaveatDefinitions(ctx)
	require.NoError(err)
	require.Empty(caveats)
}

// UnifiedSchemaLookupTest tests lookup operations
func UnifiedSchemaLookupTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Write schema
	schemaText, _, err := generator.GenerateSchema(testSchemaDefinitions)
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(testSchemaDefinitions))
	for _, def := range testSchemaDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}

	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)

	reader = ds.SnapshotReader(writtenRev, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	// Test ListAllSchemaDefinitions
	allDefs, err := schemaReader.ListAllSchemaDefinitions(ctx)
	require.NoError(err)
	require.Len(allDefs, 2)
	require.Contains(allDefs, "user")
	require.Contains(allDefs, "document")

	// Test looking up non-existent namespace
	_, found, err := schemaReader.LookupTypeDefByName(ctx, "nonexistent")
	require.NoError(err)
	require.False(found)

	// Test looking up non-existent caveat
	_, found, err = schemaReader.LookupCaveatDefByName(ctx, "nonexistent")
	require.NoError(err)
	require.False(found)
}

// UnifiedSchemaValidationTest tests that stored schemas are validated
func UnifiedSchemaValidationTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if transaction supports SchemaWriter
	var schemaWriterErr error
	_, _ = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		_, schemaWriterErr = rwt.SchemaWriter()
		return nil
	})
	require.NoError(schemaWriterErr, "datastore transaction must provide SchemaWriter")

	// Write a simple valid schema to verify the writer works
	schemaText := "definition user {}"
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "schema",
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions))
	for _, objDef := range compiled.ObjectDefinitions {
		defs = append(defs, objDef)
	}

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)
}

// UnifiedSchemaMultipleIterationsTest tests writing and reading randomly generated
// schema data across multiple iterations, verifying that older revisions remain readable.
// This test uses schema diff for comparison, so definition order doesn't matter.
func UnifiedSchemaMultipleIterationsTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	const numIterations = 5

	// Store all revisions and their corresponding compiled schemas
	type revisionData struct {
		revision datastore.Revision
		compiled *compiler.CompiledSchema
	}
	revisions := make([]revisionData, 0, numIterations)

	// Write schemas in a loop
	for i := 0; i < numIterations; i++ {
		// Generate random bytes for schema hash
		randomBytes := make([]byte, 16)
		_, err := rand.Read(randomBytes)
		require.NoError(err)

		// Create a unique schema for this iteration
		schemaText := fmt.Sprintf(`definition user {}

definition resource_%d {
	relation viewer: user
	relation editor: user
	permission view = viewer + editor
}`, i)

		// Compile the schema
		compiled, err := compiler.Compile(compiler.InputSchema{
			Source:       "schema",
			SchemaString: schemaText,
		}, compiler.AllowUnprefixedObjectType())
		require.NoError(err)

		// Convert to datastore.SchemaDefinition
		defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions))
		for _, objDef := range compiled.ObjectDefinitions {
			defs = append(defs, objDef)
		}

		// Write schema
		writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			schemaWriter, err := rwt.SchemaWriter()
			if err != nil {
				return err
			}
			return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
		})
		require.NoError(err)

		// Store revision data
		revisions = append(revisions, revisionData{
			revision: writtenRev,
			compiled: compiled,
		})

		t.Logf("Iteration %d: Written schema at revision %v", i, writtenRev)
	}

	// First, verify the latest schema can be read without AS OF SYSTEM TIME
	latestRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)
	latestReader := ds.SnapshotReader(latestRev, datastore.NoSchemaHashForTesting)
	latestSchemaReader, err := latestReader.SchemaReader()
	require.NoError(err)
	latestSchemaText, err := latestSchemaReader.SchemaText()
	require.NoError(err)
	require.NotEmpty(latestSchemaText, "Latest schema text should not be empty")
	t.Logf("Latest schema (at %v) read successfully", latestRev)

	// Verify all revisions can be read back
	for i, revData := range revisions {
		t.Logf("Verifying iteration %d at revision %v", i, revData.revision)

		reader := ds.SnapshotReader(revData.revision, datastore.NoSchemaHashForTesting)
		schemaReader, err := reader.SchemaReader()
		require.NoError(err, "Failed to get schema reader for iteration %d", i)

		// Read the schema text and compile it
		readSchemaText, err := schemaReader.SchemaText()
		require.NoError(err, "Failed to read schema text for iteration %d", i)
		require.NotEmpty(readSchemaText, "Schema text should not be empty for iteration %d", i)

		// Compile the read schema
		readCompiled, err := compiler.Compile(compiler.InputSchema{
			Source:       "schema",
			SchemaString: readSchemaText,
		}, compiler.AllowUnprefixedObjectType())
		require.NoError(err, "Failed to compile read schema for iteration %d", i)

		// Use schema diff to compare (order doesn't matter)
		expectedDiffable := diff.NewDiffableSchemaFromCompiledSchema(revData.compiled)
		actualDiffable := diff.NewDiffableSchemaFromCompiledSchema(readCompiled)

		schemaDiff, err := diff.DiffSchemas(expectedDiffable, actualDiffable, nil)
		require.NoError(err, "Failed to diff schemas for iteration %d", i)

		// Verify no differences
		require.Empty(schemaDiff.AddedNamespaces, "Unexpected added namespaces at iteration %d", i)
		require.Empty(schemaDiff.RemovedNamespaces, "Unexpected removed namespaces at iteration %d", i)
		require.Empty(schemaDiff.AddedCaveats, "Unexpected added caveats at iteration %d", i)
		require.Empty(schemaDiff.RemovedCaveats, "Unexpected removed caveats at iteration %d", i)
		require.Empty(schemaDiff.ChangedNamespaces, "Unexpected changed namespaces at iteration %d", i)
		require.Empty(schemaDiff.ChangedCaveats, "Unexpected changed caveats at iteration %d", i)

		t.Logf("Iteration %d: Successfully verified schema at revision %v", i, revData.revision)
	}
}

// UnifiedSchemaLookupByNamesTest tests the LookupTypeDefinitionsByNames and LookupCaveatDefinitionsByNames methods
func UnifiedSchemaLookupByNamesTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Define schema with multiple namespaces and caveats
	schemaText := `caveat is_admin(is_admin bool) {
	is_admin
}

caveat is_owner(is_owner bool) {
	is_owner
}

caveat has_permission(has_permission bool) {
	has_permission
}

definition user {}

definition document {
	relation viewer: user with is_admin
	relation editor: user with is_owner
	relation owner: user
}

definition folder {
	relation viewer: user
	relation owner: user with has_permission
}

definition organization {
	relation member: user
}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       "schema",
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, objDef := range compiled.ObjectDefinitions {
		defs = append(defs, objDef)
	}
	for _, caveatDef := range compiled.CaveatDefinitions {
		defs = append(defs, caveatDef)
	}

	// Write schema
	writtenRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)

	// Read schema
	reader = ds.SnapshotReader(writtenRev, datastore.NoSchemaHashForTesting)
	schemaReader, err := reader.SchemaReader()
	require.NoError(err)

	// Test LookupTypeDefinitionsByNames
	t.Run("lookup existing type definitions", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"user", "document"})
		require.NoError(err)
		require.Len(typeDefs, 2)
		require.Contains(typeDefs, "user")
		require.Contains(typeDefs, "document")

		userDef, ok := typeDefs["user"].(*core.NamespaceDefinition)
		require.True(ok)
		require.Equal("user", userDef.Name)

		docDef, ok := typeDefs["document"].(*core.NamespaceDefinition)
		require.True(ok)
		require.Equal("document", docDef.Name)
	})

	t.Run("lookup all type definitions", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"user", "document", "folder", "organization"})
		require.NoError(err)
		require.Len(typeDefs, 4)
		require.Contains(typeDefs, "user")
		require.Contains(typeDefs, "document")
		require.Contains(typeDefs, "folder")
		require.Contains(typeDefs, "organization")
	})

	t.Run("lookup non-existent type definitions", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"nonexistent"})
		require.NoError(err)
		require.Empty(typeDefs)
	})

	t.Run("lookup mixed existing and non-existent type definitions", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"user", "nonexistent", "document"})
		require.NoError(err)
		require.Len(typeDefs, 2)
		require.Contains(typeDefs, "user")
		require.Contains(typeDefs, "document")
		require.NotContains(typeDefs, "nonexistent")
	})

	t.Run("lookup empty list of type definitions", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{})
		require.NoError(err)
		require.Empty(typeDefs)
	})

	t.Run("type lookup does not return caveats", func(t *testing.T) {
		typeDefs, err := schemaReader.LookupTypeDefinitionsByNames(ctx, []string{"is_admin"})
		require.NoError(err)
		require.Empty(typeDefs)
	})

	// Test LookupCaveatDefinitionsByNames
	t.Run("lookup existing caveat definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"is_admin", "is_owner"})
		require.NoError(err)
		require.Len(caveatDefs, 2)
		require.Contains(caveatDefs, "is_admin")
		require.Contains(caveatDefs, "is_owner")

		isAdminDef, ok := caveatDefs["is_admin"].(*core.CaveatDefinition)
		require.True(ok)
		require.Equal("is_admin", isAdminDef.Name)

		isOwnerDef, ok := caveatDefs["is_owner"].(*core.CaveatDefinition)
		require.True(ok)
		require.Equal("is_owner", isOwnerDef.Name)
	})

	t.Run("lookup all caveat definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"is_admin", "is_owner", "has_permission"})
		require.NoError(err)
		require.Len(caveatDefs, 3)
		require.Contains(caveatDefs, "is_admin")
		require.Contains(caveatDefs, "is_owner")
		require.Contains(caveatDefs, "has_permission")
	})

	t.Run("lookup non-existent caveat definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"nonexistent"})
		require.NoError(err)
		require.Empty(caveatDefs)
	})

	t.Run("lookup mixed existing and non-existent caveat definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"is_admin", "nonexistent", "has_permission"})
		require.NoError(err)
		require.Len(caveatDefs, 2)
		require.Contains(caveatDefs, "is_admin")
		require.Contains(caveatDefs, "has_permission")
		require.NotContains(caveatDefs, "nonexistent")
	})

	t.Run("lookup empty list of caveat definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{})
		require.NoError(err)
		require.Empty(caveatDefs)
	})

	t.Run("caveat lookup does not return type definitions", func(t *testing.T) {
		caveatDefs, err := schemaReader.LookupCaveatDefinitionsByNames(ctx, []string{"user"})
		require.NoError(err)
		require.Empty(caveatDefs)
	})

	// Test LookupSchemaDefinitionsByNames (mixed types and caveats)
	t.Run("lookup both types and caveats", func(t *testing.T) {
		allDefs, err := schemaReader.LookupSchemaDefinitionsByNames(ctx, []string{"user", "is_admin", "document", "is_owner"})
		require.NoError(err)
		require.Len(allDefs, 4)
		require.Contains(allDefs, "user")
		require.Contains(allDefs, "is_admin")
		require.Contains(allDefs, "document")
		require.Contains(allDefs, "is_owner")

		// Verify correct types
		_, ok := allDefs["user"].(*core.NamespaceDefinition)
		require.True(ok, "user should be a NamespaceDefinition")

		_, ok = allDefs["is_admin"].(*core.CaveatDefinition)
		require.True(ok, "is_admin should be a CaveatDefinition")
	})
}

// DatastoreTesterWithSchemaMode is a function that creates a datastore with a specific schema mode.
type DatastoreTesterWithSchemaMode func(schemaMode options.SchemaMode) DatastoreTester

// UnifiedSchemaAllModesTest tests unified schema storage with all four schema modes:
// ReadLegacyWriteLegacy, ReadLegacyWriteBoth, ReadNewWriteBoth, and ReadNewWriteNew.
// This ensures that schema writes happen in the same transaction as the ReadWriteTx,
// allowing AS OF SYSTEM TIME (or equivalent MVCC) queries to work correctly.
func UnifiedSchemaAllModesTest(t *testing.T, testerFactory DatastoreTesterWithSchemaMode) {
	testCases := []struct {
		name       string
		schemaMode options.SchemaMode
	}{
		{
			name:       "ReadLegacyWriteLegacy",
			schemaMode: options.SchemaModeReadLegacyWriteLegacy,
		},
		{
			name:       "ReadLegacyWriteBoth",
			schemaMode: options.SchemaModeReadLegacyWriteBoth,
		},
		{
			name:       "ReadNewWriteBoth",
			schemaMode: options.SchemaModeReadNewWriteBoth,
		},
		{
			name:       "ReadNewWriteNew",
			schemaMode: options.SchemaModeReadNewWriteNew,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tester := testerFactory(tc.schemaMode)
			UnifiedSchemaMultipleIterationsTest(t, tester)
		})
	}
}

// UnifiedSchemaHashTest tests that the schema_revision table is correctly populated with the schema hash
func UnifiedSchemaHashTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()

	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)
	defer ds.Close()

	// Check if datastore supports SchemaReader
	headRev, _, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev, datastore.NoSchemaHashForTesting)
	_, err = reader.SchemaReader()
	require.NoError(err, "datastore reader must provide SchemaReader")

	// Get the hash reader (test-only interface)
	hashReader, hasHashReader := ds.(interface {
		SchemaHashReaderForTesting() interface {
			ReadSchemaHash(ctx context.Context) (string, error)
		}
	})
	require.True(hasHashReader, "datastore must implement SchemaHashReaderForTesting")

	// Get the reader implementation
	readerImpl := hashReader.SchemaHashReaderForTesting()
	require.NotNil(readerImpl, "SchemaHashReaderForTesting() must return non-nil reader")

	// Generate schema text
	schemaText, _, err := generator.GenerateSchema(testSchemaDefinitions)
	require.NoError(err)

	// Convert to datastore.SchemaDefinition
	defs := make([]datastore.SchemaDefinition, 0, len(testSchemaDefinitions))
	for _, def := range testSchemaDefinitions {
		defs = append(defs, def.(datastore.SchemaDefinition))
	}

	// Write schema
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, defs, schemaText, nil)
	})
	require.NoError(err)

	// Get the schema mode from the datastore to determine expected behavior
	schemaModeProvider, ok := ds.(interface {
		SchemaModeForTesting() (options.SchemaMode, error)
	})
	require.True(ok, "datastore must implement SchemaModeForTesting() for this test")
	schemaMode, err := schemaModeProvider.SchemaModeForTesting()
	require.NoError(err, "failed to get schema mode from datastore")

	// Determine if this mode should write the unified schema hash
	// The hash is written in all modes that write to the unified schema
	shouldWriteHash := schemaMode == options.SchemaModeReadLegacyWriteBoth ||
		schemaMode == options.SchemaModeReadNewWriteBoth ||
		schemaMode == options.SchemaModeReadNewWriteNew

	// Read the schema hash from schema_revision table
	hash, hashErr := readerImpl.ReadSchemaHash(ctx)

	if shouldWriteHash {
		// Hash MUST be present and correct
		require.NoError(hashErr, "schema hash should be present in mode %s", schemaMode)
		require.NotEmpty(hash, "schema hash should not be empty")
		expectedHash := computeExpectedSchemaHash(t, testSchemaDefinitions)
		require.Equal(expectedHash, hash, "schema hash should match computed hash of sorted schema text")
	} else {
		// Hash should NOT be present in ReadLegacyWriteLegacy mode
		require.ErrorIs(hashErr, datastore.ErrSchemaNotFound,
			"expected ErrSchemaNotFound in mode %s, got: %v", schemaMode, hashErr)
	}

	// Update the schema
	updatedSchemaText, _, err := generator.GenerateSchema(updatedSchemaDefinitions)
	require.NoError(err)

	updatedDefs := make([]datastore.SchemaDefinition, 0, len(updatedSchemaDefinitions))
	for _, def := range updatedSchemaDefinitions {
		updatedDefs = append(updatedDefs, def.(datastore.SchemaDefinition))
	}

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		schemaWriter, err := rwt.SchemaWriter()
		if err != nil {
			return err
		}
		return schemaWriter.WriteSchema(ctx, updatedDefs, updatedSchemaText, nil)
	})
	require.NoError(err)

	// Read the updated schema hash and verify based on mode
	updatedHash, updatedHashErr := hashReader.SchemaHashReaderForTesting().ReadSchemaHash(ctx)

	if shouldWriteHash {
		// Hash MUST be present, correct, and different from the first hash
		require.NoError(updatedHashErr, "updated schema hash should be present in mode %s", schemaMode)
		require.NotEmpty(updatedHash, "updated schema hash should not be empty")
		require.NotEqual(hash, updatedHash, "schema hash should change after update")

		// Verify the updated hash is correct
		expectedUpdatedHash := computeExpectedSchemaHash(t, updatedSchemaDefinitions)
		require.Equal(expectedUpdatedHash, updatedHash, "updated schema hash should match computed hash of sorted updated schema text")
	} else {
		// Hash should still NOT be present in ReadLegacyWriteLegacy mode
		require.ErrorIs(updatedHashErr, datastore.ErrSchemaNotFound,
			"expected ErrSchemaNotFound after update in mode %s, got: %v", schemaMode, updatedHashErr)
	}
}

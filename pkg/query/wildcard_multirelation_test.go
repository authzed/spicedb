package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestIterSubjectsWildcardWithMultipleRelations tests that when there's a wildcard
// on one relation and concrete subjects on another relation of the same resource,
// the wildcard branch should only enumerate subjects from its own relation, not
// from other relations on the same resource.
func TestIterSubjectsWildcardWithMultipleRelations(t *testing.T) {
	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	// Create a schema similar to the directandindirect.yaml test case
	schemaText := `
		definition user {}

		definition document {
			relation viewer: user | user:*
			relation banned: user
		}
	`

	ctx := t.Context()

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	// Write the schema
	_, err = datalayer.WriteStoredSchemaForTest(ctx, rawDS, schemaText)
	require.NoError(err)

	// Write test data:
	// - document:publicdoc#viewer@user:* (wildcard on viewer)
	// - document:publicdoc#viewer@user:tom (concrete user on viewer)
	// - document:publicdoc#banned@user:fred (concrete user on banned - should NOT appear in viewer results)
	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate,
		tuple.MustParse("document:publicdoc#viewer@user:*"),
		tuple.MustParse("document:publicdoc#viewer@user:tom"),
		tuple.MustParse("document:publicdoc#banned@user:fred"),
	)
	require.NoError(err)

	// Build schema for querying
	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(err)

	documentDef, _ := dsSchema.GetTypeDefinition("document")
	viewerRel, _ := documentDef.GetRelation("viewer")

	// Test the wildcard branch for viewer
	t.Run("WildcardBranchEnumeratesAllDefinedSubjects", func(t *testing.T) {
		// The wildcard branch should enumerate ALL subjects of the appropriate type that are
		// defined in the datastore, not just those with a relationship to this specific resource.
		// This is the intended behavior: when a wildcard (e.g., user:*) exists on a relation,
		// it grants access to "all subjects of that type", so we enumerate all defined subjects.
		wildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[1]) // user:* (wildcard)

		queryCtx := NewLocalContext(ctx,
			WithRevisionedReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
			WithTraceLogger(NewTraceLogger())) // Enable tracing for debugging
		subjects, err := queryCtx.IterSubjects(wildcardBranch, NewObject("document", "publicdoc"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Print trace if test fails
		if queryCtx.TraceLogger != nil && t.Failed() {
			t.Logf("Trace:\n%s", queryCtx.TraceLogger.DumpTrace())
		}

		// The wildcard branch now returns the wildcard path itself (user:*), which
		// is stripped at the top level by FilterWildcardSubjects. The caller sees
		// no concrete results — wildcards propagate internally for intersection/exclusion
		// semantics and are removed before reaching the service layer.
		require.Empty(paths, "Wildcard paths are filtered at the top level")
	})

	// Test the Union (combined behavior) - this is what happens in the actual query plan
	t.Run("UnionDeduplicatesSubjects", func(t *testing.T) {
		// The Union of both branches should return all subjects with deduplication
		union := NewUnionIterator(
			NewDatastoreIterator(viewerRel.BaseRelations()[0]), // user (non-wildcard)
			NewDatastoreIterator(viewerRel.BaseRelations()[1]), // user:* (wildcard)
		)

		queryCtx := NewLocalContext(ctx,
			WithRevisionedReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
			WithTraceLogger(NewTraceLogger())) // Enable tracing for debugging
		subjects, err := queryCtx.IterSubjects(union, NewObject("document", "publicdoc"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Print trace if test fails
		if queryCtx.TraceLogger != nil && t.Failed() {
			t.Logf("Trace:\n%s", queryCtx.TraceLogger.DumpTrace())
		}

		// The non-wildcard branch returns concrete subjects (tom on viewer).
		// The wildcard branch returns user:* which is filtered at the top level.
		// Only concrete subjects from explicit relationships are returned.
		require.Len(paths, 1, "Should return concrete subjects only; wildcard is filtered at top level")
		require.Equal("tom", paths[0].Subject.ObjectID)
	})
}

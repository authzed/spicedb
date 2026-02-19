package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
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
	t.Parallel()

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

	ctx := context.Background()

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err)

	// Write the schema
	_, err = rawDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
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
		t.Parallel()
		// The wildcard branch should enumerate ALL subjects of the appropriate type that are
		// defined in the datastore, not just those with a relationship to this specific resource.
		// This is the intended behavior: when a wildcard (e.g., user:*) exists on a relation,
		// it grants access to "all subjects of that type", so we enumerate all defined subjects.
		wildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[1]) // user:* (wildcard)

		queryCtx := NewLocalContext(ctx,
			WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)),
			WithTraceLogger(NewTraceLogger())) // Enable tracing for debugging
		subjects, err := queryCtx.IterSubjects(wildcardBranch, NewObject("document", "publicdoc"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Print trace if test fails
		if queryCtx.TraceLogger != nil && t.Failed() {
			t.Logf("Trace:\n%s", queryCtx.TraceLogger.DumpTrace())
		}

		// Should get both tom and fred because they are both defined subjects in the datastore:
		// - tom has a concrete relationship on viewer for this resource
		// - fred has a concrete relationship on banned for this resource
		// The wildcard means "all users", so we enumerate all defined users
		require.Len(paths, 2, "Should return all defined subjects of the appropriate type")
		subjectIDs := slicez.Map(paths, func(p Path) string { return p.Subject.ObjectID })
		require.ElementsMatch([]string{"tom", "fred"}, subjectIDs, "Should include both tom and fred")
	})

	// Test the Union (combined behavior) - this is what happens in the actual query plan
	t.Run("UnionDeduplicatesSubjects", func(t *testing.T) {
		t.Parallel()
		// The Union of both branches should return all subjects with deduplication
		union := NewUnionIterator(
			NewDatastoreIterator(viewerRel.BaseRelations()[0]), // user (non-wildcard)
			NewDatastoreIterator(viewerRel.BaseRelations()[1]), // user:* (wildcard)
		)

		queryCtx := NewLocalContext(ctx,
			WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)),
			WithTraceLogger(NewTraceLogger())) // Enable tracing for debugging
		subjects, err := queryCtx.IterSubjects(union, NewObject("document", "publicdoc"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Print trace if test fails
		if queryCtx.TraceLogger != nil && t.Failed() {
			t.Logf("Trace:\n%s", queryCtx.TraceLogger.DumpTrace())
		}

		// Should get both tom and fred:
		// - tom from both branches (non-wildcard has concrete tom on viewer, wildcard enumerates tom)
		// - fred from wildcard branch (has relationship on banned)
		// The Union should deduplicate tom
		require.Len(paths, 2, "Should return both subjects with deduplication")
		subjectIDs := slicez.Map(paths, func(p Path) string { return p.Subject.ObjectID })
		require.ElementsMatch([]string{"tom", "fred"}, subjectIDs, "Should include both tom and fred")
	})
}

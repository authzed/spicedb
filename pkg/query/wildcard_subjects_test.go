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

// TestIterSubjectsWithWildcard tests that wildcards are properly filtered and expanded
func TestIterSubjectsWithWildcard(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	// Create a schema with wildcards: relation viewer: user | user:*
	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user | user:*
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
	// - resource:first#viewer@user:* (wildcard)
	// - resource:first#viewer@user:concrete (concrete user)
	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate,
		tuple.MustParse("resource:first#viewer@user:*"),
		tuple.MustParse("resource:first#viewer@user:concrete"),
	)
	require.NoError(err)

	// Build schema for querying
	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(err)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")

	// Test the non-wildcard branch (user)
	t.Run("NonWildcardBranch", func(t *testing.T) {
		t.Parallel()
		// The non-wildcard branch should only return concrete subjects, filtering out wildcards
		nonWildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[0]) // user (non-wildcard)

		queryCtx := NewLocalContext(ctx, WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)))
		subjects, err := queryCtx.IterSubjects(nonWildcardBranch, NewObject("resource", "first"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Should only get the concrete user, not the wildcard
		require.Len(paths, 1)
		require.Equal("user", paths[0].Subject.ObjectType)
		require.Equal("concrete", paths[0].Subject.ObjectID)
	})

	// Test the wildcard branch (user:*)
	t.Run("WildcardBranch", func(t *testing.T) {
		t.Parallel()
		// The wildcard branch should enumerate concrete subjects when a wildcard exists
		wildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[1]) // user:* (wildcard)

		queryCtx := NewLocalContext(ctx, WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)))
		subjects, err := queryCtx.IterSubjects(wildcardBranch, NewObject("resource", "first"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Should only get the concrete user, not the wildcard itself
		require.Len(paths, 1)
		require.Equal("user", paths[0].Subject.ObjectType)
		require.Equal("concrete", paths[0].Subject.ObjectID)
	})

	// Test the Union (combined behavior)
	t.Run("UnionDeduplication", func(t *testing.T) {
		t.Parallel()
		// The Union of both branches should deduplicate the concrete user
		union := NewUnionIterator(
			NewDatastoreIterator(viewerRel.BaseRelations()[0]), // user
			NewDatastoreIterator(viewerRel.BaseRelations()[1]), // user:*
		)

		queryCtx := NewLocalContext(ctx, WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)))
		subjects, err := queryCtx.IterSubjects(union, NewObject("resource", "first"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Should get exactly one concrete user (deduplicated)
		require.Len(paths, 1)
		require.Equal("user", paths[0].Subject.ObjectType)
		require.Equal("concrete", paths[0].Subject.ObjectID)
	})
}

// TestIterSubjectsWildcardWithoutWildcardRelationship tests that the wildcard branch
// returns empty when no wildcard relationship exists
func TestIterSubjectsWildcardWithoutWildcardRelationship(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(err)

	// Create a schema with wildcards: relation viewer: user | user:*
	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user | user:*
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

	// Write test data with ONLY concrete users, NO wildcard
	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate,
		tuple.MustParse("resource:second#viewer@user:alice"),
		tuple.MustParse("resource:second#viewer@user:bob"),
	)
	require.NoError(err)

	// Build schema for querying
	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(err)

	resourceDef, _ := dsSchema.GetTypeDefinition("resource")
	viewerRel, _ := resourceDef.GetRelation("viewer")

	// Test the wildcard branch when no wildcard relationship exists
	t.Run("WildcardBranchWithoutWildcard", func(t *testing.T) {
		t.Parallel()
		// The wildcard branch should return empty because there's no wildcard relationship
		wildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[1]) // user:* (wildcard)

		queryCtx := NewLocalContext(ctx, WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)))
		subjects, err := queryCtx.IterSubjects(wildcardBranch, NewObject("resource", "second"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Should be empty - no wildcard relationship exists
		require.Empty(paths)
	})

	// Test the non-wildcard branch still works
	t.Run("NonWildcardBranchWorksNormally", func(t *testing.T) {
		t.Parallel()
		nonWildcardBranch := NewDatastoreIterator(viewerRel.BaseRelations()[0]) // user (non-wildcard)

		queryCtx := NewLocalContext(ctx, WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)))
		subjects, err := queryCtx.IterSubjects(nonWildcardBranch, NewObject("resource", "second"), NoObjectFilter())
		require.NoError(err)

		paths, err := CollectAll(subjects)
		require.NoError(err)

		// Should get both concrete users
		require.Len(paths, 2)
		subjectIDs := slicez.Map(paths, func(p Path) string { return p.Subject.ObjectID })
		require.ElementsMatch([]string{"alice", "bob"}, subjectIDs)
	})
}

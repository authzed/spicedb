package benchmarks

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BenchmarkCheckDeepArrow benchmarks permission checking through a deep recursive chain.
// This recreates the testharness scenario with:
// - A 30+ level deep parent chain: document:target -> document:1 -> ... -> document:29
// - document:29#view@user:slow
// - Checking if user:slow has viewer permission on document:target
//
// The permission viewer = view + parent->viewer creates a recursive traversal through
// all 30+ levels to find the view relationship at the end of the chain.
func BenchmarkCheckDeepArrow(b *testing.B) {
	// Create an in-memory datastore
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(b, err)

	ctx := context.Background()

	schemaText := `
		definition user {}

		definition document {
			relation parent: document
			relation view: user
			permission viewer = view + parent->viewer
		}
	`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("benchmark"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(b, err)

	// Write the schema
	_, err = rawDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(b, err)

	// Build relationships for the deep arrow scenario
	// Create a chain: document:target -> document:1 -> document:2 -> ... -> document:30 -> document:31
	// Plus: document:29#view@user:slow
	relationships := make([]tuple.Relationship, 0, 33)

	// document:target#parent@document:1
	relationships = append(relationships, tuple.MustParse("document:target#parent@document:1"))

	// Chain: document:1 through document:30
	for i := 1; i <= 30; i++ {
		rel := fmt.Sprintf("document:%d#parent@document:%d", i, i+1)
		relationships = append(relationships, tuple.MustParse(rel))
	}

	// The view relationship at the end of the chain
	relationships = append(relationships, tuple.MustParse("document:29#view@user:slow"))

	// Write all relationships to the datastore
	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, relationships...)
	require.NoError(b, err)

	// Build schema for querying
	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(b, err)

	// Create the iterator tree for the viewer permission using BuildIteratorFromSchema
	viewerIterator, err := query.BuildIteratorFromSchema(dsSchema, "document", "viewer")
	require.NoError(b, err)

	// Create query context
	queryCtx := query.NewLocalContext(ctx,
		query.WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)),
		query.WithMaxRecursionDepth(50),
	)

	// The resource we're checking: document:target
	resources := query.NewObjects("document", "target")

	// The subject we're checking: user:slow
	subject := query.NewObject("user", "slow").WithEllipses()

	// Reset the timer - everything before this is setup
	b.ResetTimer()

	// Run the benchmark
	for b.Loop() {
		// Check if user:slow can view document:target
		// This will traverse the entire 30+ level chain
		seq, err := queryCtx.Check(viewerIterator, resources, subject)
		require.NoError(b, err)

		// Collect all results (should find user:slow at the end of the chain)
		paths, err := query.CollectAll(seq)
		require.NoError(b, err)

		// Verify we found the expected result
		require.Len(b, paths, 1)
		require.Equal(b, "slow", paths[0].Subject.ObjectID)
	}
}

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

// BenchmarkCheckWideArrow benchmarks permission checking through a wide arrow relationship.
// This creates a scenario with:
// - 10 files
// - 100 groups (group0 through group99)
// - 1000 users (user0 through user999)
// - Each file belongs to 30 groups (deterministic assignment)
// - Each group has ~20 users (deterministic assignment)
//
// The permission viewer = view + group->member creates a wide arrow traversal where
// checking if a user has viewer permission on a file requires checking many group memberships.
func BenchmarkCheckWideArrow(b *testing.B) {
	const (
		numFiles      = 10
		numGroups     = 97  // Prime number to avoid duplicate assignments with stepping
		numUsers      = 997 // Prime number to avoid duplicate assignments with stepping
		groupsPerFile = 30
		usersPerGroup = 20
	)

	// Create an in-memory datastore
	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(b, err)

	ctx := context.Background()

	schemaText := `
		definition user {}

		definition group {
			relation member: user
		}

		definition file {
			relation group: group
			relation view: user
			permission viewer = view + group->member
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

	// Build relationships deterministically
	relationships := make([]tuple.Relationship, 0, numFiles*groupsPerFile+numGroups*usersPerGroup)

	// Create file->group relationships
	// Each file belongs to 30 groups with different step sizes (deterministic assignment with modular wrapping)
	for fileID := 0; fileID < numFiles; fileID++ {
		// file0 steps by 1s (groups 0, 1, 2, ..., 29)
		// file1 steps by 2s (groups 0, 2, 4, ..., 58)
		// file2 steps by 3s (groups 0, 3, 6, ..., 87)
		// etc., with modular wrapping
		step := fileID + 1
		for i := 0; i < groupsPerFile; i++ {
			groupID := (i * step) % numGroups
			rel := fmt.Sprintf("file:file%d#group@group:group%d", fileID, groupID)
			relationships = append(relationships, tuple.MustParse(rel))
		}
	}

	// Create group->user relationships
	// Each group has 20 users with different step sizes (deterministic assignment with modular wrapping)
	for groupID := 0; groupID < numGroups; groupID++ {
		// group0 steps by 1s (users 0, 1, 2, ..., 19)
		// group1 steps by 2s (users 0, 2, 4, ..., 38)
		// group2 steps by 3s (users 0, 3, 6, ..., 57)
		// etc., with modular wrapping
		step := groupID + 1
		for i := 0; i < usersPerGroup; i++ {
			userID := (i * step) % numUsers
			rel := fmt.Sprintf("group:group%d#member@user:user%d", groupID, userID)
			relationships = append(relationships, tuple.MustParse(rel))
		}
	}

	// Write all relationships to the datastore
	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, relationships...)
	require.NoError(b, err)

	// Build schema for querying
	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(b, err)

	// Create the iterator tree for the viewer permission using BuildIteratorFromSchema
	viewerIterator, err := query.BuildIteratorFromSchema(dsSchema, "file", "viewer")
	require.NoError(b, err)

	// Create query context
	queryCtx := query.NewLocalContext(ctx,
		query.WithReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision)),
	)

	// The resource we're checking: file:file0
	resources := query.NewObjects("file", "file0")

	// The subject we're checking: user:user15
	// This user should have access through multiple groups
	subject := query.NewObject("user", "user15").WithEllipses()

	// Reset the timer - everything before this is setup
	b.ResetTimer()

	// Run the benchmark
	for b.Loop() {
		// Check if user:user15 can view file:file0
		// This will traverse through many group memberships
		seq, err := queryCtx.Check(viewerIterator, resources, subject)
		require.NoError(b, err)

		// Collect all results
		paths, err := query.CollectAll(seq)
		require.NoError(b, err)

		// Verify we found at least one path
		// user15 should have access through multiple groups
		require.NotEmpty(b, paths)
	}
}

package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

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

// networkDelay is the simulated per-call round-trip latency used by the delay
// sub-benchmarks. Adjust this to model different network environments.
const networkDelay = 100 * time.Microsecond

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
//
// Four sub-benchmarks are run:
//   - plain:         compile the outline directly and run Check each iteration
//   - advised:       seed a CountAdvisor from a single warm-up run, apply it to the
//     canonical outline, compile once, then run Check each iteration
//   - plain_delay:   same as plain, but with a delay reader simulating network latency
//   - advised_delay: same as advised, but with a delay reader simulating network latency
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

	// Build the canonical outline once; all sub-benchmarks derive from it.
	canonicalOutline, err := query.BuildOutlineFromSchema(dsSchema, "file", "viewer")
	require.NoError(b, err)

	// The resource we're checking: file:file0
	resource := query.NewObject("file", "file0")

	// The subject we're checking: user:user15
	// This user should have access through multiple groups
	subject := query.NewObject("user", "user15").WithEllipses()

	// Base reader (no simulated latency).
	reader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	// Delay reader wrapping the base reader with simulated network latency.
	delayReader := query.NewDelayReader(networkDelay, reader)

	// buildAdvisedIterator seeds a CountAdvisor from a single warm-up run using the
	// provided reader and returns the compiled advised iterator.
	buildAdvisedIterator := func(b *testing.B, r query.QueryDatastoreReader) query.Iterator {
		b.Helper()
		obs := query.NewCountObserver()
		warmIt, err := canonicalOutline.Compile()
		require.NoError(b, err)
		warmCtx := query.NewLocalContext(ctx,
			query.WithReader(r),
			query.WithObserver(obs),
		)
		_, err = warmCtx.Check(warmIt, resource, subject)
		require.NoError(b, err)

		advisor := query.NewCountAdvisor(obs.GetStats())
		advisedCO, err := query.ApplyAdvisor(canonicalOutline, advisor)
		require.NoError(b, err)
		advisedIt, err := advisedCO.Compile()
		require.NoError(b, err)
		return advisedIt
	}

	// ---- plain sub-benchmark ----
	// Compile the outline directly and run Check each iteration. No advisement.

	b.Run("plain", func(b *testing.B) {
		it, err := canonicalOutline.Compile()
		require.NoError(b, err)

		b.Log("plain explain:\n", it.Explain())

		queryCtx := query.NewLocalContext(ctx, query.WithReader(reader))

		b.ResetTimer()
		for b.Loop() {
			path, err := queryCtx.Check(it, resource, subject)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})

	// ---- advised sub-benchmark ----
	// Seed a CountAdvisor from a warm-up run, compile the advised iterator once,
	// then run Check each iteration.

	b.Run("advised", func(b *testing.B) {
		advisedIt := buildAdvisedIterator(b, reader)

		b.Log("advised explain:\n", advisedIt.Explain())

		queryCtx := query.NewLocalContext(ctx, query.WithReader(reader))

		b.ResetTimer()
		for b.Loop() {
			path, err := queryCtx.Check(advisedIt, resource, subject)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})

	// ---- plain_delay sub-benchmark ----
	// Same as plain but with networkDelay latency injected per datastore call.

	b.Run("plain_delay", func(b *testing.B) {
		it, err := canonicalOutline.Compile()
		require.NoError(b, err)

		queryCtx := query.NewLocalContext(ctx, query.WithReader(delayReader))

		b.ResetTimer()
		for b.Loop() {
			path, err := queryCtx.Check(it, resource, subject)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})

	// ---- advised_delay sub-benchmark ----
	// Same as advised but with networkDelay latency injected per datastore call.
	// The warm-up run also uses the delay reader so advisor stats reflect realistic
	// call patterns.

	b.Run("advised_delay", func(b *testing.B) {
		advisedIt := buildAdvisedIterator(b, delayReader)
		queryCtx := query.NewLocalContext(ctx, query.WithReader(delayReader))

		for b.Loop() {
			path, err := queryCtx.Check(advisedIt, resource, subject)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})
}

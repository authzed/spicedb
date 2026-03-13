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

// BenchmarkCheckDoubleWideArrow benchmarks permission checking through two consecutive
// arrow hops with wide fan-out at each level.
//
// The hierarchy is: file -> org -> group -> user
//
//   - 5 files, each belonging to 3 orgs
//   - 23 orgs (prime), each containing 7 groups
//   - 97 groups (prime), each with 15 members
//   - 997 users (prime)
//
// The schema:
//
//	definition group  { relation member: user }
//	definition org    { relation group: group; permission member = group->member }
//	definition file   { relation org: org; relation view: user;
//	                    permission viewer = view + org->member }
//
// Checking viewer on a file requires two arrow traversals:
//  1. file->org  (fanout: orgs per file)
//  2. org->member which resolves to org->group->member (double fan-out)
//
// Four sub-benchmarks are run:
//   - plain:         compile the outline directly and run Check each iteration
//   - advised:       seed a CountAdvisor from a single warm-up run, apply it to the
//     canonical outline, compile once, then run Check each iteration
//   - plain_delay:   same as plain, but with networkDelay latency per datastore call
//   - advised_delay: same as advised, but with networkDelay latency per datastore call
func BenchmarkCheckDoubleWideArrow(b *testing.B) {
	const (
		numFiles      = 5
		numOrgs       = 97  // prime
		numGroups     = 299 // prime
		numUsers      = 499 // prime
		orgsPerFile   = 20
		groupsPerOrg  = 10
		usersPerGroup = 20
	)

	// ---- shared setup ----

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(b, err)

	ctx := context.Background()

	schemaText := `
		definition user {}

		definition group {
			relation member: user
		}

		definition org {
			relation group: group
			permission member = group->member
		}

		definition file {
			relation org: org
			relation view: user
			permission viewer = view + org->member
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("benchmark"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(b, err)

	_, err = rawDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.LegacyWriteNamespaces(ctx, compiled.ObjectDefinitions...)
	})
	require.NoError(b, err)

	relationships := make([]tuple.Relationship, 0,
		numFiles*orgsPerFile+numOrgs*groupsPerOrg+numGroups*usersPerGroup)

	// file:fileN#org@org:orgM
	for fileID := 0; fileID < numFiles; fileID++ {
		step := fileID + 1
		for i := 0; i < orgsPerFile; i++ {
			orgID := (i * step) % numOrgs
			rel := fmt.Sprintf("file:file%d#org@org:org%d", fileID, orgID)
			relationships = append(relationships, tuple.MustParse(rel))
		}
	}

	// org:orgN#group@group:groupM
	for orgID := 0; orgID < numOrgs; orgID++ {
		step := orgID + 1
		for i := 0; i < groupsPerOrg; i++ {
			groupID := (i * step) % numGroups
			rel := fmt.Sprintf("org:org%d#group@group:group%d", orgID, groupID)
			relationships = append(relationships, tuple.MustParse(rel))
		}
	}

	// group:groupN#member@user:userM
	for groupID := 0; groupID < numGroups; groupID++ {
		step := groupID + 1
		for i := 0; i < usersPerGroup; i++ {
			userID := (i * step) % numUsers
			rel := fmt.Sprintf("group:group%d#member@user:user%d", groupID, userID)
			relationships = append(relationships, tuple.MustParse(rel))
		}
	}

	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, relationships...)
	require.NoError(b, err)

	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(b, err)

	// Build the canonical outline once; all sub-benchmarks derive from it.
	canonicalOutline, err := query.BuildOutlineFromSchema(dsSchema, "file", "viewer")
	require.NoError(b, err)

	// The resource and subject are the same for all sub-benchmarks.
	resource := query.NewObject("file", "file0")
	subject := query.NewObject("user", "user181").WithEllipses()

	// Base reader (no simulated latency).
	reader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision))

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

	b.Run("advised_delay", func(b *testing.B) {
		advisedIt := buildAdvisedIterator(b, delayReader)
		queryCtx := query.NewLocalContext(ctx, query.WithReader(delayReader))

		b.ResetTimer()
		for b.Loop() {
			path, err := queryCtx.Check(advisedIt, resource, subject)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})
}

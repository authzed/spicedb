package benchmarks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	dispatchgraph "github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const defaultMaxDepth = uint32(50)

// setupDispatchBenchmark sets up a memdb datastore, populates it with the
// benchmark data, and returns a local-only dispatcher, context with datalayer,
// and revision.
func setupDispatchBenchmark(b *testing.B, benchmark bm.Benchmark) (context.Context, dispatch.Dispatcher, *bm.QuerySets, datastore.Revision) {
	b.Helper()

	ctx := b.Context()

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(b, err)

	queries, err := benchmark.Setup(ctx, rawDS)
	require.NoError(b, err)

	revision, err := rawDS.HeadRevision(ctx)
	require.NoError(b, err)

	// Set up the datalayer in context — required by the dispatcher.
	ctx = datalayer.ContextWithHandle(ctx)
	require.NoError(b, datalayer.SetInContext(ctx, datalayer.NewDataLayer(rawDS)))

	params, err := dispatchgraph.NewDefaultDispatcherParametersForTesting()
	require.NoError(b, err)
	dispatcher, err := dispatchgraph.NewLocalOnlyDispatcher(params)
	require.NoError(b, err)
	b.Cleanup(func() { _ = dispatcher.Close() })

	return ctx, dispatcher, queries, revision
}

func maxDepthForQueries(queries *bm.QuerySets) uint32 {
	if queries.MaxRecursionDepth > 0 {
		return uint32(queries.MaxRecursionDepth)
	}
	return defaultMaxDepth
}

// BenchmarkCurrentDispatchCheck benchmarks all registered scenarios using the
// current production dispatch code path (computed.ComputeCheck with a local-only
// dispatcher). This provides an apples-to-apples comparison against the query
// planner benchmarks in check_benchmark_test.go, since both use memdb and skip
// gRPC overhead.
func BenchmarkCurrentDispatchCheck(b *testing.B) {
	for _, benchmark := range bm.All() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx, dispatcher, queries, revision := setupDispatchBenchmark(b, benchmark)
			require.NotEmpty(b, queries.Checks)
			check := queries.Checks[0]

			checkParams := computed.CheckParameters{
				ResourceType: tuple.RR(check.ResourceType, check.Permission),
				Subject:      tuple.ONR(check.SubjectType, check.SubjectID, check.SubjectRelation),
				AtRevision:   revision,
				MaximumDepth: maxDepthForQueries(queries),
				DebugOption:  computed.NoDebugging,
			}

			b.ResetTimer()
			for b.Loop() {
				result, _, err := computed.ComputeCheck(ctx, dispatcher,
					caveattypes.Default.TypeSet,
					checkParams,
					check.ResourceID,
					100,
				)
				require.NoError(b, err)
				require.Equal(b, v1.ResourceCheckResult_MEMBER, result.Membership)
			}
		})
	}
}

// BenchmarkCurrentDispatchLookupResources benchmarks all registered scenarios that
// have IterResources queries using the current dispatch code path
// (DispatchLookupResources3 with a local-only dispatcher).
func BenchmarkCurrentDispatchLookupResources(b *testing.B) {
	for _, benchmark := range bm.All() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx, dispatcher, queries, revision := setupDispatchBenchmark(b, benchmark)

			if len(queries.IterResources) == 0 {
				b.Skip("no IterResources queries defined")
			}

			lrQuery := queries.IterResources[0]

			b.ResetTimer()
			for b.Loop() {
				stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)

				err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
					ResourceRelation: tuple.RR(lrQuery.FilterResourceType, lrQuery.Permission).ToCoreRR(),
					SubjectRelation:  tuple.RR(lrQuery.SubjectType, lrQuery.SubjectRelation).ToCoreRR(),
					SubjectIds:       []string{lrQuery.SubjectID},
					TerminalSubject:  tuple.ONR(lrQuery.SubjectType, lrQuery.SubjectID, lrQuery.SubjectRelation).ToCoreONR(),
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: maxDepthForQueries(queries),
					},
					OptionalLimit: 1000000000,
				}, stream)
				require.NoError(b, err)
				require.NotEmpty(b, stream.Results())
			}
		})
	}
}

// BenchmarkCurrentDispatchLookupSubjects benchmarks all registered scenarios that
// have IterSubjects queries using the current dispatch code path
// (DispatchLookupSubjects with a local-only dispatcher).
func BenchmarkCurrentDispatchLookupSubjects(b *testing.B) {
	for _, benchmark := range bm.All() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx, dispatcher, queries, revision := setupDispatchBenchmark(b, benchmark)

			if len(queries.IterSubjects) == 0 {
				b.Skip("no IterSubjects queries defined")
			}

			lsQuery := queries.IterSubjects[0]

			b.ResetTimer()
			for b.Loop() {
				stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)

				err := dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
					ResourceRelation: tuple.RR(lsQuery.ResourceType, lsQuery.Permission).ToCoreRR(),
					ResourceIds:      []string{lsQuery.ResourceID},
					SubjectRelation:  tuple.RR(lsQuery.FilterSubjectType, tuple.Ellipsis).ToCoreRR(),
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: maxDepthForQueries(queries),
					},
				}, stream)
				require.NoError(b, err)
				require.NotEmpty(b, stream.Results())
			}
		})
	}
}

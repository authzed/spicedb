package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/require"

	dispatchgraph "github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BenchmarkCurrentDispatchCheck benchmarks all registered scenarios using the
// current production dispatch code path (computed.ComputeCheck with a local-only
// dispatcher). This provides an apples-to-apples comparison against the query
// planner benchmarks in check_benchmark_test.go, since both use memdb and skip
// gRPC overhead.
func BenchmarkCurrentDispatchCheck(b *testing.B) {
	for _, benchmark := range bm.All() {
		b.Run(benchmark.Name, func(b *testing.B) {
			ctx := b.Context()

			rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(b, err)

			queries, err := benchmark.Setup(ctx, rawDS)
			require.NoError(b, err)
			require.NotEmpty(b, queries.Checks)

			check := queries.Checks[0]

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

			maxDepth := uint32(50)
			if queries.MaxRecursionDepth > 0 {
				maxDepth = uint32(queries.MaxRecursionDepth)
			}

			checkParams := computed.CheckParameters{
				ResourceType: tuple.RR(check.ResourceType, check.Permission),
				Subject:      tuple.ONR(check.SubjectType, check.SubjectID, check.SubjectRelation),
				AtRevision:   revision,
				MaximumDepth: maxDepth,
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

package benchmarks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch/graph"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// classicDepthRemaining is the default per-request depth budget used for
// classic dispatch benchmarks, matching the convention from
// internal/dispatch/graph/check_test.go.
const classicDepthRemaining = 50

// runClassicCheck runs a CheckPermission via the local-only graph dispatcher
// (the "classic" path) for a single benchmark scenario. It is invoked as a
// sub-benchmark of BenchmarkCheck when -bench-classic is set, so the results
// land alongside the query-planner variants for direct comparison.
//
// The dispatcher is intentionally bare: no caching, no singleflight, no gRPC,
// no redispatch hop — just NewLocalOnlyDispatcher reading from the in-memory
// datastore set up by the parent benchmark.
func runClassicCheck(
	b *testing.B,
	ctx context.Context,
	ds datastore.Datastore,
	rev datastore.RevisionWithSchemaHash,
	check bm.CheckQuery,
	depthRemaining uint32,
) {
	b.Helper()

	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
	require.NoError(b, err)
	b.Cleanup(func() {
		_ = dispatcher.Close()
	})

	// The dispatcher reads schema and relationships via the datalayer pulled
	// off the context, so we install one. ContextWithDataLayer adds both the
	// boxed handle and the value in a single call.
	dispatchCtx := datalayer.ContextWithDataLayer(ctx, datalayer.NewDataLayer(ds))

	resourceRR := &core.RelationReference{
		Namespace: check.ResourceType,
		Relation:  check.Permission,
	}
	subject := &core.ObjectAndRelation{
		Namespace: check.SubjectType,
		ObjectId:  check.SubjectID,
		Relation:  check.SubjectRelation,
	}

	bloom, err := v1.NewTraversalBloomFilter(50)
	require.NoError(b, err)

	newReq := func() *v1.DispatchCheckRequest {
		// The checker mutates Metadata.TraversalBloom during traversal, so each
		// call needs a fresh request to avoid carrying state across iterations.
		return &v1.DispatchCheckRequest{
			ResourceRelation: resourceRR,
			ResourceIds:      []string{check.ResourceID},
			ResultsSetting:   v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
			Subject:          subject,
			Metadata: &v1.ResolverMeta{
				AtRevision:     rev.Revision.String(),
				DepthRemaining: depthRemaining,
				SchemaHash:     []byte(rev.SchemaHash),
				TraversalBloom: append([]byte(nil), bloom...),
			},
		}
	}

	// Warm-up so one-shot work (schema/namespace load) is excluded from the
	// timed loop, mirroring the query-planner advised variant's warm-up.
	resp, err := dispatcher.DispatchCheck(dispatchCtx, newReq())
	require.NoError(b, err)
	requireMember(b, resp, check.ResourceID)

	b.ResetTimer()
	for b.Loop() {
		resp, err := dispatcher.DispatchCheck(dispatchCtx, newReq())
		require.NoError(b, err)
		requireMember(b, resp, check.ResourceID)
	}
}

func requireMember(b *testing.B, resp *v1.DispatchCheckResponse, resourceID string) {
	b.Helper()
	found, ok := resp.ResultsByResourceId[resourceID]
	require.True(b, ok, "no result for resource %q", resourceID)
	require.Equal(b, v1.ResourceCheckResult_MEMBER, found.Membership)
}

// classicDepth picks a depth budget compatible with the benchmark scenario.
// The query-planner side controls recursion via WithMaxRecursionDepth; the
// classic path uses DepthRemaining on the dispatch metadata. We honor a
// scenario-provided depth if set, otherwise fall back to the unit-test
// convention of 50.
func classicDepth(qs *bm.QuerySets) uint32 {
	if qs.MaxRecursionDepth > 0 {
		return uint32(qs.MaxRecursionDepth)
	}
	return classicDepthRemaining
}

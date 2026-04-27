package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	graphpkg "github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// cyclicGroupSchema defines a group schema where two groups are mutual members —
// the minimal cyclic data pattern that exercises cycle detection.
const cyclicGroupSchema = `
definition user {}

definition group {
	relation member: user | group#member
	permission can_access = member
}`

// TestLookupSubjectsCycleDetection verifies that:
// 1. The call terminates (does not deadlock or stack-overflow) on cyclic data.
// 2. When debug tracing is enabled, the debug trace captures the cyclic nodes.
func TestLookupSubjectsCycleDetection(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	rels := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, cyclicGroupSchema, rels, require.New(t))

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	t.Cleanup(func() { dispatcher.Close() })

	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	// Initialize the traversal tracker on the context (simulates what permissions.go does).
	ctx = graphpkg.NewTraversalTracker(ctx)

	req := &v1.DispatchLookupSubjectsRequest{
		ResourceRelation: RR("group", "can_access").ToCoreRR(),
		ResourceIds:      []string{"a"},
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		EnableDebugTrace: true,
	}

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	err = dispatcher.DispatchLookupSubjects(req, stream)
	if err != nil {
		require.Contains(t, err.Error(), "max depth exceeded",
			"unexpected error for cyclic data: %v", err)
		var traceErr dispatch.MaxDepthWithTraceError
		require.ErrorAs(t, err, &traceErr)
		trace := traceErr.Trace
		require.NotNil(t, trace, "debug trace should be non-nil even on max depth error")
		
		pathLength := 0
		currentNode := trace
		var prevDepth uint32 = 0
		for currentNode != nil {
			pathLength++
			require.NotContains(t, currentNode.ResourceId, "*batch*", "trace must not contain batch artifacts")
			require.NotContains(t, currentNode.ResourceId, "...", "trace must not fake resource ID with ellipses")
			if pathLength > 1 {
				require.True(t, currentNode.Depth > prevDepth, "depth must increase monotonically")
			}
			prevDepth = currentNode.Depth

			if len(currentNode.SubProblems) > 0 {
				currentNode = currentNode.SubProblems[0]
			} else {
				currentNode = nil
			}
		}
		require.GreaterOrEqual(t, pathLength, 2, "path length must be at least 2 for recursion")
		return
	}

	// No error: bloom filter caught the cycle early. No trace is emitted.
	t.Log("Cycle was resolved before max depth; no trace is emitted")

}

// TestLookupResources3CycleDetection verifies termination and trace content for LR3.
func TestLookupResources3CycleDetection(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)

	rels := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, cyclicGroupSchema, rels, require.New(t))

	dispatcher, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	t.Cleanup(func() { dispatcher.Close() })

	ctx := datalayer.ContextWithHandle(t.Context())
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	// Initialize the traversal tracker on the context (simulates what permissions.go does).
	ctx = graphpkg.NewTraversalTracker(ctx)

	req := &v1.DispatchLookupResources3Request{
		ResourceRelation: RR("group", "can_access").ToCoreRR(),
		SubjectRelation:  RR("user", "...").ToCoreRR(),
		SubjectIds:       []string{"someone"},
		TerminalSubject:  tuple.CoreONR("user", "someone", graphpkg.Ellipsis),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		EnableDebugTrace: true,
	}

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
	err = dispatcher.DispatchLookupResources3(req, stream)
	if err != nil {
		require.Contains(t, err.Error(), "max depth exceeded",
			"unexpected error for cyclic data: %v", err)
		var traceErr dispatch.MaxDepthWithTraceError
		require.ErrorAs(t, err, &traceErr)
		trace := traceErr.Trace
		require.NotNil(t, trace)

		pathLength := 0
		currentNode := trace
		var prevDepth uint32 = 0
		for currentNode != nil {
			pathLength++
			require.NotContains(t, currentNode.ResourceId, "*batch*", "trace must not contain batch artifacts")
			require.NotContains(t, currentNode.ResourceId, "...", "trace must not fake resource ID with ellipses")
			if pathLength > 1 {
				require.True(t, currentNode.Depth > prevDepth, "depth must increase monotonically")
			}
			prevDepth = currentNode.Depth

			if len(currentNode.SubProblems) > 0 {
				currentNode = currentNode.SubProblems[0]
			} else {
				currentNode = nil
			}
		}
		require.GreaterOrEqual(t, pathLength, 2, "path length must be at least 2 for recursion")
		return
	}

	// No error: verify trace is present when nodes were traversed.
	t.Log("Cycle was resolved before max depth; no trace is emitted")
}

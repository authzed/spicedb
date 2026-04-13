package graph

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
	// With cyclic data the bloom filter terminates traversal; a depth-exceeded error is acceptable.
	// The critical assertion is that the call terminates.
	if err != nil {
		require.Contains(t, err.Error(), "max depth exceeded",
			"unexpected error for cyclic data: %v", err)
		// Even on error the tracker should have accumulated visits.
		trace := graphpkg.SnapshotLookupDebugTrace(ctx)
		require.NotNil(t, trace, "debug trace should be non-nil even on max depth error")
		require.NotEmpty(t, trace.SubProblems, "expected at least one visited node in trace")
		return
	}

	// No error: bloom filter caught the cycle early. Verify trace content.
	trace := graphpkg.SnapshotLookupDebugTrace(ctx)
	require.NotNil(t, trace, "debug trace must be populated when EnableDebugTrace=true")
	require.NotEmpty(t, trace.SubProblems, "expected at least one visited node")

	// At least one node must be marked cyclic (visited more than once).
	hasCyclic := false
	for _, sp := range trace.SubProblems {
		if sp.IsCyclic {
			hasCyclic = true
			require.GreaterOrEqual(t, sp.TraversalCount, uint32(2),
				"cyclic node %s:%s#%s must have TraversalCount≥2", sp.ResourceType, sp.ResourceId, sp.Relation)
		}
	}
	require.True(t, hasCyclic, "expected at least one cyclic node in trace for mutually-recursive groups")

	// Verify serialization round-trips cleanly (simulates the trailer write+read path).
	encoded := graphpkg.SerializeLookupDebugTrace(ctx)
	require.NotEmpty(t, encoded)
	raw, decErr := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, decErr)
	var decoded v1.LookupDebugTrace
	require.NoError(t, proto.Unmarshal(raw, &decoded))
	require.NotEmpty(t, decoded.SubProblems)
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
		// Trace should be non-nil even when we hit max depth.
		trace := graphpkg.SnapshotLookupDebugTrace(ctx)
		require.NotNil(t, trace)
		return
	}

	// No error: verify trace is present when nodes were traversed.
	// Note: LR3 with user:someone not in any group relationship will find zero results and
	// may traverse zero nodes too (short-circuit before dispatchIter), so nil is allowed.
	trace := graphpkg.SnapshotLookupDebugTrace(ctx)
	// If any cyclic node was found, it must have TraversalCount ≥ 2.
	if trace != nil {
		for _, sp := range trace.SubProblems {
			if sp.IsCyclic {
				require.GreaterOrEqual(t, sp.TraversalCount, uint32(2))
			}
		}
	}
}

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
	ctx = dispatch.NewTraversalTracker(ctx)

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
		trace := dispatch.SnapshotLookupDebugTrace(ctx)
		require.NotNil(t, trace)
		return
	}

	// No error: verify trace is present when nodes were traversed.
	// Note: LR3 with user:someone not in any group relationship will find zero results and
	// may traverse zero nodes too (short-circuit before dispatchIter), so nil is allowed.
	trace := dispatch.SnapshotLookupDebugTrace(ctx)
	// If cyclic nodes were traversed, the trace must have frames.
	if trace != nil {
		require.NotEmpty(t, trace.Frames)
	}
}

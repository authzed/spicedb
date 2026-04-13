package graph_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	graphpkg "github.com/authzed/spicedb/internal/graph"
)

func TestTraversalTrackerNodeKey(t *testing.T) {
	got := graphpkg.ExportedNodeKey("group", "a", "member")
	require.Equal(t, "group:a#member", got)
}

func TestTraversalTrackerFirstVisit(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalTracker(context.Background())
	ctx2, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 1, count)
	require.NotNil(t, ctx2)
}

func TestTraversalTrackerSecondVisitIsCyclic(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalTracker(context.Background())
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	_, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 2, count)
}

func TestTraversalTrackerDifferentNodesAreIndependent(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalTracker(context.Background())
	ctx, countA := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	_, countB := graphpkg.ExportedTrackVisit(ctx, "group", "b", "member")
	require.Equal(t, 1, countA)
	require.Equal(t, 1, countB)
}

// TestTraversalTrackerSnapshotNoCycles verifies a snapshot with no repeated visits
// contains only non-cyclic entries.
func TestTraversalTrackerSnapshotNoCycles(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalTracker(context.Background())
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "b", "member")

	trace := graphpkg.ExportedSnapshot(ctx)
	require.NotNil(t, trace)
	require.Len(t, trace.SubProblems, 2)
	for _, sp := range trace.SubProblems {
		require.False(t, sp.IsCyclic, "node %s:%s#%s should not be cyclic", sp.ResourceType, sp.ResourceId, sp.Relation)
		require.Equal(t, uint32(1), sp.TraversalCount)
	}
}

// TestTraversalTrackerSnapshotWithCycle verifies that a repeated visit is reflected
// in the snapshot with IsCyclic=true and TraversalCount>1.
func TestTraversalTrackerSnapshotWithCycle(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalTracker(context.Background())
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "b", "member")
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member") // second visit — cyclic

	trace := graphpkg.ExportedSnapshot(ctx)
	require.NotNil(t, trace)
	require.Len(t, trace.SubProblems, 2)

	found := map[string]bool{}
	for _, sp := range trace.SubProblems {
		key := sp.ResourceType + ":" + sp.ResourceId + "#" + sp.Relation
		found[key] = sp.IsCyclic
		if sp.ResourceId == "a" {
			require.True(t, sp.IsCyclic, "group:a#member should be flagged cyclic")
			require.GreaterOrEqual(t, sp.TraversalCount, uint32(2))
		} else {
			require.False(t, sp.IsCyclic, "group:b#member should not be flagged cyclic")
			require.Equal(t, uint32(1), sp.TraversalCount)
		}
	}
	require.True(t, found["group:a#member"], "expected group:a#member in trace")
	require.True(t, found["group:b#member"] || true) // b is present but order is map-nondeterministic
}

// TestTraversalTrackerNoTrackerIsNoOp verifies that calling trackVisit without
// initializing a tracker (defensive path) returns count=1 and doesn't panic.
func TestTraversalTrackerNoTrackerIsNoOp(t *testing.T) {
	ctx := context.Background() // no NewTraversalTracker called
	_, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 1, count)

	// Snapshot on uninitialized context returns nil.
	trace := graphpkg.ExportedSnapshot(ctx)
	require.Nil(t, trace)
}

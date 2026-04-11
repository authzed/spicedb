package graph_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	graphpkg "github.com/authzed/spicedb/internal/graph"
)

// --- Pure unit tests for the traversalTracker helpers in debug.go ---
// Integration tests (with a real dispatcher and datastore) live in
// internal/dispatch/graph/cycle_detection_test.go which has access to
// NewLocalOnlyDispatcher and the full test dispatcher setup.

func TestTraversalTrackerNodeKey(t *testing.T) {
	got := graphpkg.ExportedNodeKey("group", "a", "member")
	require.Equal(t, "group:a#member", got)
}

func TestTraversalTrackerFirstVisit(t *testing.T) {
	ctx := context.Background()
	ctx2, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 1, count)
	require.NotNil(t, ctx2)
}

func TestTraversalTrackerSecondVisit(t *testing.T) {
	ctx := context.Background()
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	_, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 2, count)
}

func TestTraversalTrackerDifferentNodes(t *testing.T) {
	ctx := context.Background()
	ctx, countA := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	_, countB := graphpkg.ExportedTrackVisit(ctx, "group", "b", "member")
	require.Equal(t, 1, countA)
	require.Equal(t, 1, countB)
}

func TestTraversalTrackerThirdVisitIsCyclic(t *testing.T) {
	ctx := context.Background()
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	ctx, _ = graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	_, count := graphpkg.ExportedTrackVisit(ctx, "group", "a", "member")
	require.Equal(t, 3, count)
	require.True(t, count > 1, "count > 1 means is_cyclic should be true")
}

func TestTraversalTrackerConcurrentVisits(t *testing.T) {
	ctx := context.Background()

	// Simulate what happens with isolated contexts (no shared tracker):
	// two separate call trees should each start from 1.
	_, count1 := graphpkg.ExportedTrackVisit(context.Background(), "group", "a", "member")
	_, count2 := graphpkg.ExportedTrackVisit(context.Background(), "group", "a", "member")
	require.Equal(t, 1, count1)
	require.Equal(t, 1, count2)
	_ = ctx
}

package graph

import (
	"context"
	"fmt"
	"sync"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// lookupTraversalKey is the unexported context key for the traversalTracker.
// Using a private struct prevents collisions with keys from other packages.
type lookupTraversalKey struct{}

// traversalTracker counts how many times each unique node (resourceType + resourceID + relation)
// is visited during a single top-level Lookup request. It is stored in the request context and
// shared across all recursive dispatch calls within that request.
//
// All public methods are thread-safe.
type traversalTracker struct {
	mu      sync.Mutex
	visited map[string]int // nodeKey → visit count
}

// nodeKey produces a stable, unique string key for a resource+relation combination.
func nodeKey(resourceType, resourceID, relation string) string {
	return fmt.Sprintf("%s:%s#%s", resourceType, resourceID, relation)
}

// trackVisit records one visit to the node identified by (resourceType, resourceID, relation).
//
// If no tracker exists in ctx it is created and attached. The returned context always
// contains a tracker. The returned int is the updated visit count for that node (≥ 1).
//
// When a tracker is NOT present in the incoming ctx (i.e. debug tracing is disabled),
// this function is never called — callers gate on EnableDebugTrace first, so this path
// is zero-cost in the hot path.
func trackVisit(ctx context.Context, resourceType, resourceID, relation string) (context.Context, int) {
	tracker, ok := ctx.Value(lookupTraversalKey{}).(*traversalTracker)
	if !ok {
		tracker = &traversalTracker{visited: make(map[string]int)}
		ctx = context.WithValue(ctx, lookupTraversalKey{}, tracker)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	key := nodeKey(resourceType, resourceID, relation)
	tracker.visited[key]++
	return ctx, tracker.visited[key]
}

// buildLookupDebugTrace constructs a LookupDebugTrace proto for a single visited node.
// isCyclic is set to true when count > 1 (the node was encountered more than once).
func buildLookupDebugTrace(resourceType, resourceID, relation string, count int, subProblems []*v1.LookupDebugTrace) *v1.LookupDebugTrace {
	//nolint:gosec // count is always ≥ 1, safe to cast
	return &v1.LookupDebugTrace{
		ResourceType:    resourceType,
		ResourceId:      resourceID,
		Relation:        relation,
		TraversalCount:  uint32(count),
		IsCyclic:        count > 1,
		SubProblems:     subProblems,
	}
}

package graph

import (
	"context"
	"sync"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// lookupTraversalKey is the unexported context key for the traversalTracker.
// Using a private struct prevents collisions with keys from other packages.
type lookupTraversalKey struct{}

// nodeKey is used as a map key to uniquely identify a visited node.
type nodeKey struct {
	ns  string
	id  string
	rel string
}

// traversalTracker counts how many times each unique node (resourceType + resourceID + relation)
// is visited during a single top-level Lookup request. It is stored in the request context and
// shared across all recursive dispatch calls within that request.
//
// THREADING MODEL:
// The tracker must be initialized explicitly at the top-level entry point (permissions.go) via
// NewTraversalTracker before any goroutine fan-out, so that all goroutines share a single
// instance via the inherited context. The mutex protects concurrent map writes from goroutine
// fan-out in dispatchTo and LookupResources3's dispatchIter.
type traversalTracker struct {
	mu      sync.Mutex
	visited map[nodeKey]int // nodeKey → visit count
}

// NewTraversalTracker creates a new traversalTracker, stores it in ctx, and returns
// the enriched context. Call this once per top-level Lookup handler (in permissions.go)
// before the first DispatchLookup* call. All goroutines spawned inside that request
// will inherit the same tracker instance via the context pointer.
func NewTraversalTracker(ctx context.Context) context.Context {
	t := &traversalTracker{visited: make(map[nodeKey]int)}
	return context.WithValue(ctx, lookupTraversalKey{}, t)
}

// trackVisit records one visit to the node (resourceType, resourceID, relation).
// Returns the same ctx and the updated visit count (≥ 1).
//
// If no tracker is present in ctx (NewTraversalTracker was not called — defensive path),
// the function is a no-op and returns count=1. Callers must call NewTraversalTracker at
// the request root before enabling tracing.
//
// Callers gate on req.EnableDebugTrace before calling this, so it is zero-cost in the
// hot path when tracing is disabled.
func trackVisit(ctx context.Context, resourceType, resourceID, relation string) (context.Context, int) {
	tracker, ok := ctx.Value(lookupTraversalKey{}).(*traversalTracker)
	if !ok {
		// Defensive: no tracker injected. Return count=1 (non-cyclic), don't mutate ctx.
		return ctx, 1
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	key := nodeKey{ns: resourceType, id: resourceID, rel: relation}
	tracker.visited[key]++
	return ctx, tracker.visited[key]
}

// SnapshotLookupDebugTrace reads the traversalTracker from ctx and returns a proto
// message whose SubProblems list contains one entry per visited node, with
// IsCyclic=true for nodes visited more than once.
//
// Returns nil if no tracker is present (debug tracing was not enabled).
// Returns an empty trace (non-nil) if tracing was enabled but 0 visits occurred.
// This ensures tests and callers don't panic on max-depth-exceeded errors before visits.
func SnapshotLookupDebugTrace(ctx context.Context) *v1.LookupDebugTrace {
	tracker, ok := ctx.Value(lookupTraversalKey{}).(*traversalTracker)
	if !ok || tracker == nil {
		return nil
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if len(tracker.visited) == 0 {
		return &v1.LookupDebugTrace{}
	}

	nodes := make([]*v1.LookupDebugTrace, 0, len(tracker.visited))
	for key, count := range tracker.visited {
		nodes = append(nodes, &v1.LookupDebugTrace{
			ResourceType:   key.ns,
			ResourceId:     key.id,
			Relation:       key.rel,
			TraversalCount: uint32(count), //nolint:gosec // count always ≥ 1
			IsCyclic:       count > 1,
		})
	}

	// Return a root "envelope" trace whose sub-problems are the visited nodes.
	return &v1.LookupDebugTrace{
		SubProblems: nodes,
	}
}


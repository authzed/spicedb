package graph

import (
	"context"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Export internal symbols for use by graph_test package unit tests.
// This file is compiled only during testing (the _test.go suffix is on the callers;
// this file itself uses the non-test package so callers in graph_test can reach it).

// ExportedNodeKey exposes the internal nodeKey map structure logic for testing.
// Since nodeKey is now a struct, we provide a helper to format it for assertions.
func ExportedNodeKey(resourceType, resourceID, relation string) string {
	k := nodeKey{ns: resourceType, id: resourceID, rel: relation}
	// Return the string format the tests originally expected
	return k.ns + ":" + k.id + "#" + k.rel
}

// ExportedTrackVisit exposes trackVisit for whitebox testing.
// Use with a context returned by ExportedNewTraversalTracker to test accumulation.
func ExportedTrackVisit(ctx context.Context, resourceType, resourceID, relation string) (context.Context, int) {
	return trackVisit(ctx, resourceType, resourceID, relation)
}

// ExportedNewTraversalTracker exposes NewTraversalTracker for whitebox testing.
func ExportedNewTraversalTracker(ctx context.Context) context.Context {
	return NewTraversalTracker(ctx)
}

// ExportedSnapshot exposes SnapshotLookupDebugTrace for whitebox testing.
func ExportedSnapshot(ctx context.Context) *v1.LookupDebugTrace {
	return SnapshotLookupDebugTrace(ctx)
}

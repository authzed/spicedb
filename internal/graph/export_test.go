// export_test.go exposes internal helpers to the graph_test package.
// This file is only compiled during tests.
package graph

import "context"

// ExportedNodeKey wraps nodeKey for testing.
func ExportedNodeKey(namespace, objectID, relation string) string {
	return nodeKey(namespace, objectID, relation)
}

// ExportedTrackVisit wraps trackVisit for testing.
func ExportedTrackVisit(ctx context.Context, namespace, objectID, relation string) (context.Context, int) {
	return trackVisit(ctx, namespace, objectID, relation)
}

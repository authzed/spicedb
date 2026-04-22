package graph

import (
	"context"
)

// Export internal symbols for use by graph_test package unit tests.
// This file is compiled only during testing (callers are in graph_test package).

// ExportedNewTraversalStack exposes NewTraversalStack for whitebox testing.
func ExportedNewTraversalStack(ctx context.Context) context.Context {
	return NewTraversalStack(ctx)
}

// ExportedCloneStack exposes CloneTraversalStack for whitebox testing.
func ExportedCloneStack(ctx context.Context) context.Context {
	return CloneTraversalStack(ctx)
}

// ExportedPushFrame exposes PushTraversalFrame for whitebox testing.
func ExportedPushFrame(ctx context.Context, resourceType, resourceID, relation, permission string) {
	PushTraversalFrame(ctx, resourceType, resourceID, relation, permission)
}

// ExportedPopFrame exposes PopTraversalFrame for whitebox testing.
func ExportedPopFrame(ctx context.Context) {
	PopTraversalFrame(ctx)
}

// ExportedSnapshotStack exposes SnapshotTraversalStack for whitebox testing.
func ExportedSnapshotStack(ctx context.Context) []traversalFrame {
	return SnapshotTraversalStack(ctx)
}

package graph_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	graphpkg "github.com/authzed/spicedb/internal/graph"
)

// TestTraversalStackNoOpWithoutInit verifies that Push/Pop/Snapshot are all
// no-ops (and don't panic) when the stack has not been initialized.
func TestTraversalStackNoOpWithoutInit(t *testing.T) {
	ctx := context.Background() // no NewTraversalStack called

	// Should not panic.
	graphpkg.ExportedPushFrame(ctx, "group", "a", "member", "group#member")
	graphpkg.ExportedPopFrame(ctx)

	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Nil(t, snap, "snapshot of uninitialised stack must be nil")
}

// TestTraversalStackPushPop verifies basic LIFO semantics.
func TestTraversalStackPushPop(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalStack(context.Background())

	graphpkg.ExportedPushFrame(ctx, "group", "a", "member", "group#member")
	graphpkg.ExportedPushFrame(ctx, "group", "b", "member", "group#member")

	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Len(t, snap, 2)
	require.Equal(t, "a", snap[0].ResourceID())
	require.Equal(t, "b", snap[1].ResourceID())

	graphpkg.ExportedPopFrame(ctx) // removes "b"

	snap2 := graphpkg.ExportedSnapshotStack(ctx)
	require.Len(t, snap2, 1)
	require.Equal(t, "a", snap2[0].ResourceID())
}

// TestTraversalStackSnapshotIsCopy verifies that mutating the original stack
// after a snapshot does not affect the snapshot.
func TestTraversalStackSnapshotIsCopy(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalStack(context.Background())

	graphpkg.ExportedPushFrame(ctx, "group", "a", "member", "group#member")
	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Len(t, snap, 1)

	// Push another frame AFTER taking the snapshot.
	graphpkg.ExportedPushFrame(ctx, "group", "b", "member", "group#member")

	// The old snapshot must still have only one frame.
	require.Len(t, snap, 1, "snapshot must be a copy, not a reference")
}

// TestTraversalStackOrdering verifies that frames are returned in push order.
func TestTraversalStackOrdering(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalStack(context.Background())

	graphpkg.ExportedPushFrame(ctx, "res", "1", "rel", "perm")
	graphpkg.ExportedPushFrame(ctx, "res", "2", "rel", "perm")
	graphpkg.ExportedPushFrame(ctx, "res", "3", "rel", "perm")

	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Len(t, snap, 3)
	require.Equal(t, "1", snap[0].ResourceID())
	require.Equal(t, "2", snap[1].ResourceID())
	require.Equal(t, "3", snap[2].ResourceID())
}

// TestTraversalStackEmptySnapshotAfterAllPops verifies that popping all frames
// results in a nil snapshot (not an empty slice).
func TestTraversalStackEmptySnapshotAfterAllPops(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalStack(context.Background())

	graphpkg.ExportedPushFrame(ctx, "group", "a", "member", "group#member")
	graphpkg.ExportedPopFrame(ctx)

	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Nil(t, snap)
}

// TestTraversalStackNonEmptyFields verifies that all fields are preserved.
func TestTraversalStackNonEmptyFields(t *testing.T) {
	ctx := graphpkg.ExportedNewTraversalStack(context.Background())

	graphpkg.ExportedPushFrame(ctx, "mytype", "myid", "myrelation", "mypermission")

	snap := graphpkg.ExportedSnapshotStack(ctx)
	require.Len(t, snap, 1)
	f := snap[0]
	require.NotEmpty(t, f.ResourceType())
	require.NotEmpty(t, f.ResourceID())
	require.NotEmpty(t, f.Relation())
	require.NotEmpty(t, f.Permission())
	require.Equal(t, "mytype", f.ResourceType())
	require.Equal(t, "myid", f.ResourceID())
	require.Equal(t, "myrelation", f.Relation())
	require.Equal(t, "mypermission", f.Permission())
}

// TestCloneTraversalStackInheritsParentPath verifies that a cloned stack starts
// with a copy of the parent's frames so the full ancestry is preserved.
func TestCloneTraversalStackInheritsParentPath(t *testing.T) {
	parent := graphpkg.ExportedNewTraversalStack(context.Background())
	graphpkg.ExportedPushFrame(parent, "res", "1", "rel", "perm")
	graphpkg.ExportedPushFrame(parent, "res", "2", "rel", "perm")

	child := graphpkg.ExportedCloneStack(parent)

	// Child must inherit both parent frames.
	snap := graphpkg.ExportedSnapshotStack(child)
	require.Len(t, snap, 2)
	require.Equal(t, "1", snap[0].ResourceID())
	require.Equal(t, "2", snap[1].ResourceID())
}

// TestCloneTraversalStackIsolation verifies that mutations in the cloned stack
// do not affect the parent stack and vice-versa.
func TestCloneTraversalStackIsolation(t *testing.T) {
	parent := graphpkg.ExportedNewTraversalStack(context.Background())
	graphpkg.ExportedPushFrame(parent, "res", "1", "rel", "perm")

	child := graphpkg.ExportedCloneStack(parent)

	// Push a new frame into the child.
	graphpkg.ExportedPushFrame(child, "res", "2", "rel", "perm")

	// Parent must still have exactly 1 frame.
	parentSnap := graphpkg.ExportedSnapshotStack(parent)
	require.Len(t, parentSnap, 1, "parent stack must not be affected by child push")

	// Child must have 2 frames.
	childSnap := graphpkg.ExportedSnapshotStack(child)
	require.Len(t, childSnap, 2)

	// Popping in parent must not affect child.
	graphpkg.ExportedPopFrame(parent)
	childSnap2 := graphpkg.ExportedSnapshotStack(child)
	require.Len(t, childSnap2, 2, "child stack must not be affected by parent pop")
}

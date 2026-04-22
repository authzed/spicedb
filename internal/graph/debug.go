package graph

import (
	"context"
	"sync"
)

// traversalStackKey is the unexported context key for the traversalStack.
// Using a private struct prevents collisions with keys from other packages.
type traversalStackKey struct{}

// traversalFrame holds the data for a single step in a lookup traversal path.
// Fields are all strings to avoid proto aliasing issues across goroutines.
type traversalFrame struct {
	resourceType string
	resourceID   string
	relation     string
	permission   string
}

// ResourceType returns the resource namespace for this frame.
func (f traversalFrame) ResourceType() string { return f.resourceType }

// ResourceID returns the resource object ID for this frame.
func (f traversalFrame) ResourceID() string { return f.resourceID }

// Relation returns the relation being traversed for this frame.
func (f traversalFrame) Relation() string { return f.relation }

// Permission returns the permission/relation being evaluated for this frame.
func (f traversalFrame) Permission() string { return f.permission }

// traversalStack is a per-execution-path record of traversal frames.
// The top-level handler initializes the stack once via NewTraversalStack.
// Whenever a new parallel goroutine is spawned, CloneTraversalStack must be
// used so that the goroutine inherits the parent's path while owning its own
// copy — preventing concurrent mutation of a shared slice.
type traversalStack struct {
	mu     sync.Mutex
	frames []traversalFrame
}

// NewTraversalStack installs a fresh, empty traversalStack into ctx and returns
// the enriched context. Call this exactly once at the top-level handler (when
// debug is enabled). All recursion on the same goroutine propagates ctx normally;
// goroutines that fan out must use CloneTraversalStack instead.
//
// If debug is NOT enabled, this function must NOT be called; Push/Pop will be
// no-ops only when the stack is absent from the context.
func NewTraversalStack(ctx context.Context) context.Context {
	return context.WithValue(ctx, traversalStackKey{}, &traversalStack{
		frames: make([]traversalFrame, 0, 16),
	})
}

// CloneTraversalStack creates a new traversalStack that is a copy of the one
// currently in ctx, installs it into a new context, and returns that context.
// Use this at every goroutine fan-out boundary so that:
//   - the goroutine inherits the full parent traversal path, and
//   - subsequent Push/Pop in the goroutine do not affect the parent's stack.
//
// If no stack is present in ctx (debug disabled), ctx is returned unchanged so
// Push/Pop remain no-ops.
func CloneTraversalStack(ctx context.Context) context.Context {
	stack, ok := ctx.Value(traversalStackKey{}).(*traversalStack)
	if !ok || stack == nil {
		return ctx
	}

	stack.mu.Lock()
	defer stack.mu.Unlock()

	cp := make([]traversalFrame, len(stack.frames))
	copy(cp, stack.frames)

	return context.WithValue(ctx, traversalStackKey{}, &traversalStack{
		frames: cp,
	})
}

// PushTraversalFrame records one hop in the traversal path.
// If no stack is present in ctx (NewTraversalStack was not called), this is a
// no-op — guaranteeing zero overhead when debug is disabled.
func PushTraversalFrame(ctx context.Context, resourceType, resourceID, relation, permission string) {
	stack, ok := ctx.Value(traversalStackKey{}).(*traversalStack)
	if !ok || stack == nil {
		return
	}
	stack.mu.Lock()
	stack.frames = append(stack.frames, traversalFrame{
		resourceType: resourceType,
		resourceID:   resourceID,
		relation:     relation,
		permission:   permission,
	})
	stack.mu.Unlock()
}

// PopTraversalFrame removes the most-recently-pushed frame.
// If no stack is present in ctx, this is a no-op.
func PopTraversalFrame(ctx context.Context) {
	stack, ok := ctx.Value(traversalStackKey{}).(*traversalStack)
	if !ok || stack == nil {
		return
	}
	stack.mu.Lock()
	if len(stack.frames) > 0 {
		stack.frames = stack.frames[:len(stack.frames)-1]
	}
	stack.mu.Unlock()
}

// SnapshotTraversalStack returns a copy of the current traversal stack.
// The copy is safe to read after the function returns even if the original
// stack is mutated by other goroutines.
// Returns nil if no stack is present in ctx.
func SnapshotTraversalStack(ctx context.Context) []traversalFrame {
	stack, ok := ctx.Value(traversalStackKey{}).(*traversalStack)
	if !ok || stack == nil {
		return nil
	}
	stack.mu.Lock()
	defer stack.mu.Unlock()
	if len(stack.frames) == 0 {
		return nil
	}
	cp := make([]traversalFrame, len(stack.frames))
	copy(cp, stack.frames)
	return cp
}

// Package applybranchorder consumes strong branch-order recommendations.
package applybranchorder

import (
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
)

// New returns the pass that applies supported branch-order recommendations.
func New() optimization.Optimizer {
	return optimization.Optimizer{
		Name:          "apply-branch-order",
		OptionalHints: []planhint.Kind{planhint.BranchOrderKind},
		Run:           run,
	}
}

func run(ctx *optimization.Context, co query.CanonicalOutline) (optimization.Result, error) {
	// Read the branch-order hints from the planning context to obtain the child
	// permutations recommended by earlier passes. Applying those orders lets unions
	// try likely grants earlier and intersections try likely denials earlier, reducing
	// short-circuit work while preserving the permission expression.
	hints, err := ctx.Hints(planhint.BranchOrderKind)
	if err != nil {
		return optimization.Result{}, err
	}

	// The ledger resolves competing producers before this pass. Index the remaining
	// hints by occurrence ID so identical subtrees in different positions stay distinct.
	byID := map[query.OutlineNodeID]planhint.Hint{}

	for _, h := range hints {
		byID[query.OutlineNodeID(h.Target.Node)] = h
	}

	// Reorder children after their own descendants have been processed. No subtree
	// is removed: recommendations affect execution order, preserving the set expression.
	root, err := query.WalkOutlineBottomUp(co.Root, func(n query.Outline) (query.Outline, error) {
		h, ok := byID[n.ID]
		if !ok {
			return n, nil
		}

		// Union and intersection operands are commutative, though ordering changes
		// short-circuit work. Exclusions and arrows give positions semantic meaning,
		// so decline hints for those nodes and record why rather than silently ignoring them.
		if n.Type != query.UnionIteratorType && n.Type != query.IntersectionIteratorType {
			return n, ctx.Respond(h.ID, planhint.Declined, planhint.UnsupportedTarget)
		}

		// The ledger already validated a duplicate-free permutation. Verify its arity
		// still matches this target before indexing; rewrites can change the branch count.
		order := h.Payload.(planhint.BranchOrder).Order
		if len(order) != len(n.SubOutlines) {
			return n, ctx.Respond(h.ID, planhint.Declined, planhint.StaleTarget)
		}

		// Allocate a new child slice to avoid mutating an outline shared by another plan.
		// Recording acceptance accounts for the strong recommendation and removes this
		// consumed order hint from the pending set before rewrite invalidation runs.
		subs := make([]query.Outline, len(order))

		for i, index := range order {
			subs[i] = n.SubOutlines[index]
		}

		n.SubOutlines = subs
		return n, ctx.Respond(h.ID, planhint.Accepted, "")
	})
	co.Root = root
	return optimization.Result{Outline: co}, err
}

package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// PlanAdvisor provides query plan optimization guidance through hints and mutations.
// Implementations can use internal cost models or other heuristics to suggest
// optimizations without exposing those details to callers.
type PlanAdvisor interface {
	// GetHints returns a list of hints to apply to the given outline node.
	// keySource can be used to resolve the CanonicalKey for any node in the
	// outline tree by its ID, including the current node and its children.
	// The caller is responsible for walking the outline tree and calling this
	// method on each node, then applying the returned hints.
	GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error)

	// GetMutations returns a list of outline mutations to apply to the given
	// outline node. keySource can be used to resolve the CanonicalKey for any
	// node in the outline tree by its ID, including the current node and its children.
	// The caller is responsible for walking the outline tree and
	// applying these transformations.
	GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error)
}

// ApplyAdvisor applies a PlanAdvisor to a CanonicalOutline in two phases:
//
//  1. Bottom-up mutations: for each node, GetMutations is called and the
//     returned mutations are applied. Each mutation must preserve the node's
//     ID (it is a bug if it does not). Newly synthesised child nodes (ID==0)
//     receive fresh IDs and keys via assignNewNodeIDs, producing an updated
//     CanonicalOutline.
//
//  2. Hints collection: for each node in the updated outline, GetHints is
//     called using the updated CanonicalOutline as the key source, and the
//     returned hints are stored in the Hints map.
//
// The returned CanonicalOutline has the same CanonicalKeys as the input for
// all pre-existing nodes, extended with keys for any newly synthesised nodes,
// and a fully populated Hints map.
func ApplyAdvisor(co CanonicalOutline, advisor PlanAdvisor) (CanonicalOutline, error) {
	// Phase 1: apply mutations bottom-up.
	mutatedRoot, err := applyAdvisorMutations(co.Root, co, advisor)
	if err != nil {
		return CanonicalOutline{}, err
	}

	// Phase 2: collect hints using the post-mutation outline as the key source.
	hints := make(map[OutlineNodeID][]Hint)
	if err := collectAdvisorHints(mutatedRoot, co, advisor, hints); err != nil {
		return CanonicalOutline{}, err
	}

	return CanonicalOutline{
		Root:          mutatedRoot,
		CanonicalKeys: co.CanonicalKeys,
		Hints:         hints,
	}, nil
}

// applyAdvisorMutations walks the outline tree bottom-up via WalkOutlineBottomUp.
// For each node it calls GetMutations on the advisor and applies the returned
// mutations in sequence. After each mutation it verifies that the resulting
// node's ID matches the original node's ID; a mismatch is a programmer bug.
// After all mutations are applied, any newly synthesized nodes (ID==0) receive
// fresh IDs via FillMissingNodeIDs, and their CanonicalKeys are recorded in
// the provided keys map.
func applyAdvisorMutations(outline Outline, co CanonicalOutline, advisor PlanAdvisor) (Outline, error) {
	mutated, err := WalkOutlineBottomUp(outline, func(node Outline) (Outline, error) {
		mutations, err := advisor.GetMutations(node, co)
		if err != nil {
			return Outline{}, err
		}
		result := node
		for _, fn := range mutations {
			next := fn(result)
			if next.ID != result.ID {
				return Outline{}, spiceerrors.MustBugf(
					"advisor mutation changed outline node ID from %d to %d; mutations must preserve the root node ID",
					result.ID, next.ID,
				)
			}
			result = next
		}
		return result, nil
	})
	if err != nil {
		return Outline{}, err
	}
	return FillMissingNodeIDs(mutated, co.CanonicalKeys), nil
}

// collectAdvisorHints walks the outline tree pre-order via WalkOutlinePreOrder,
// calling GetHints on the advisor for each node and storing any returned hints
// in the hints map keyed by the node's ID.
func collectAdvisorHints(outline Outline, co CanonicalOutline, advisor PlanAdvisor, hints map[OutlineNodeID][]Hint) error {
	return WalkOutlinePreOrder(outline, func(node Outline) error {
		nodeHints, err := advisor.GetHints(node, co)
		if err != nil {
			return err
		}
		if len(nodeHints) > 0 {
			hints[node.ID] = nodeHints
		}
		return nil
	})
}

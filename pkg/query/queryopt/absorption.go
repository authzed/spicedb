package queryopt

import (
	"slices"

	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "absorption-idempotency",
		Description: `
		Removes subsumed branches from union and intersection expressions, and
		annihilates exclusions of identical operands, using five set-theoretic laws:

		  Union laws (drop Y when Y ⊆ X for some sibling X):
		    - Idempotency:           A ∪ A = A
		    - Absorption:            A ∪ (A ∩ B) = A; generalizes to
		                             (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B
		    - Complement-absorption: A ∪ (A − B) = A (since (A−B) ⊆ A)

		  Intersection laws (drop Y when X ⊆ Y for some sibling X):
		    - Idempotency:  A ∩ A = A
		    - Absorption:   A ∩ (A ∪ B) = A (A is a stricter constraint than A ∪ B)

		  Exclusion law:
		    - Self-annihilation: A − A = ∅

		Caveats and arrows are treated as opaque structural units throughout.
		Runs after simple-caveat-pushdown (priority 20) so it sees the final caveat
		shape, and before reachability-pruning (priority 0) so pruning works on a
		smaller tree.
		`,
		Priority: 10,
		NewTransform: func(_ RequestParams) OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{
					absorptionIdempotency,
					intersectionIdempotencyAbsorption,
					exclusionSelfAnnihilation,
					query.NullPropagation,
				})
			}
		},
	})
}

// absorptionIdempotency is an OutlineMutation that removes subsumed children
// from UnionIteratorType nodes. It is called bottom-up by MutateOutline, so
// by the time a Union node is visited its children have already been processed.
//
// For each child Y, if any other child X subsumes Y (Y ⊆ X), Y is dropped.
// This implements two set-theoretic laws:
//
//   - Idempotency: A ∪ A = A
//   - Absorption:  A ∪ (A ∩ B) = A, generalizing to
//     (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B
//
// When all but one child are removed, the single surviving child is returned
// directly (unwrapped from the Union) to keep the tree clean.
//
// # Caveat handling
//
// Caveat nodes are treated as opaque structural units. A ∪ Caveat(A) is NOT
// simplified because A and Caveat(A) are structurally distinct — a future
// caveat-aware pass could reason that Caveat(X) ⊆ X, but that is out of scope
// here. However, A ∪ (A ∩ Caveat(B)) = A because A appears as a direct factor
// in the intersection, so everything satisfying the intersection satisfies A.
//
// # Arrow handling
//
// Arrow and IntersectionArrow nodes are treated as opaque structural units.
// (A->B) ∪ (A->B ∩ C) = A->B because Arrow[A,B] is a direct factor in
// Intersection[Arrow[A,B], C]. Arrow-internal structure is never examined;
// two arrows are considered equal only when structurally identical.
func absorptionIdempotency(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.UnionIteratorType, isSubsumedBy)
}

// intersectionIdempotencyAbsorption is an OutlineMutation that removes redundant
// children from IntersectionIteratorType nodes using two set-theoretic laws:
//
//   - Idempotency: A ∩ A = A
//   - Absorption:  A ∩ (A ∪ B) = A; anything satisfying A trivially satisfies any
//     union containing A, so the union is a weaker constraint and is dropped.
//
// For each child Y, if any other child X satisfies X ⊆ Y (X is a stricter
// constraint than Y), then Y is dropped. This uses isSubsumedBy(x, y) — the
// same predicate as absorptionIdempotency but with arguments swapped — because
// the intersection dual requires checking X ⊆ Y rather than Y ⊆ X.
//
// When all but one child are removed, the single surviving child is returned
// directly (unwrapped from the Intersection) to keep the tree clean.
func intersectionIdempotencyAbsorption(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.IntersectionIteratorType, func(y, x query.Outline) bool {
		return isSubsumedBy(x, y) // dual: drop Y when X ⊆ Y
	})
}

// eliminateRedundantChildren is the shared loop logic for absorptionIdempotency
// and intersectionIdempotencyAbsorption. It removes each child Y of a composite
// node (union or intersection) for which shouldDrop(Y, X) returns true for some
// other surviving sibling X.
//
// The keep-then-rebuild approach ensures a child already marked for removal
// cannot become the reason another sibling is also removed. For example, in
// A ∪ A, the first A marks the second for removal; without tracking removals,
// the second A would also mark the first, yielding an empty union.
//
// A single surviving child is returned unwrapped to avoid a redundant composite
// node wrapper.
func eliminateRedundantChildren(outline query.Outline, nodeType query.IteratorType, shouldDrop func(y, x query.Outline) bool) query.Outline {
	if outline.Type != nodeType || len(outline.SubOutlines) <= 1 {
		return outline
	}

	children := outline.SubOutlines
	keep := slices.Repeat([]bool{true}, len(children))

	for i, y := range children {
		if !keep[i] {
			continue
		}
		for j, x := range children {
			if i == j || !keep[j] {
				continue
			}
			if shouldDrop(y, x) {
				keep[i] = false
				break
			}
		}
	}

	if !slices.Contains(keep, false) {
		return outline
	}

	newSubs := make([]query.Outline, 0, len(children))
	for i, child := range children {
		if keep[i] {
			newSubs = append(newSubs, child)
		}
	}

	if len(newSubs) == 1 {
		return newSubs[0]
	}

	return query.Outline{
		ID:          0, // FillMissingNodeIDs assigns a fresh canonical key
		Type:        nodeType,
		Args:        outline.Args,
		SubOutlines: newSubs,
	}
}

// isSubsumedBy reports whether Y ⊆ X, i.e. X ∪ Y = X.
//
// Case 1 — Idempotency: X and Y are structurally identical.
//
// Case 2 — Absorption (union): X is a UnionIteratorType and Y appears as a
// direct child of X. Anything satisfying Y trivially satisfies any union
// containing Y, so Y ⊆ X.
//
// Case 3 — Absorption (intersection): Y is an IntersectionIteratorType and
// every factor of X (see intersectionFactors) appears as a direct child of Y
// by structural equality. If Y = X₁ ∩ … ∩ Xₙ ∩ B ∩ …  and {X₁, …, Xₙ} are
// all factors of X, then anything satisfying Y must satisfy X, so Y ⊆ X.
//
// Case 4 — Complement-absorption: Y is an ExclusionIteratorType and its minuend
// (first child) is itself subsumed by X. Since (A − B) ⊆ A for all B, if X
// subsumes A then X subsumes (A − B).
func isSubsumedBy(y, x query.Outline) bool {
	switch {
	// Case 1: structural equality
	case query.OutlineCompare(x, y) == 0:
		return true

	// Case 2: union-absorption — if X is a union and Y is one of its direct
	// children, then anything satisfying Y trivially satisfies X, so Y ⊆ X.
	case x.Type == query.UnionIteratorType:
		return slices.ContainsFunc(x.SubOutlines, func(c query.Outline) bool {
			return query.OutlineCompare(y, c) == 0
		})

	// Case 4: complement-absorption — (A − B) ⊆ A, so if X subsumes the minuend
	// of Y then X subsumes all of Y.
	case y.Type == query.ExclusionIteratorType && len(y.SubOutlines) == 2:
		return isSubsumedBy(y.SubOutlines[0], x)

	// Case 3: Y must be an intersection for absorption to apply
	case y.Type == query.IntersectionIteratorType:
		for _, factor := range intersectionFactors(x) {
			if !slices.ContainsFunc(y.SubOutlines, func(c query.Outline) bool {
				return query.OutlineCompare(factor, c) == 0
			}) {
				return false
			}
		}
		return true

	default:
		return false
	}
}

// intersectionFactors returns the operands of X for the absorption check.
// If X is an IntersectionIteratorType, its factors are its direct children.
// Otherwise, X is its own single factor — it is treated as an atomic term.
//
// Example: factors of Intersection[A, B] = {A, B}
//
//	factors of A              = {A}
func intersectionFactors(x query.Outline) []query.Outline {
	if x.Type == query.IntersectionIteratorType {
		return x.SubOutlines
	}
	return []query.Outline{x}
}

// exclusionSelfAnnihilation is an OutlineMutation that replaces A − A with ∅.
// If both operands of an ExclusionIteratorType are structurally equal, the node
// is replaced with NullIteratorType. NullPropagation (run after this mutation in
// the same MutateOutline call) then cascades the null through parent intersections
// and caveats according to each node's semantics.
func exclusionSelfAnnihilation(outline query.Outline) query.Outline {
	if outline.Type == query.ExclusionIteratorType &&
		len(outline.SubOutlines) == 2 &&
		query.OutlineCompare(outline.SubOutlines[0], outline.SubOutlines[1]) == 0 {
		return query.Outline{Type: query.NullIteratorType}
	}

	return outline
}

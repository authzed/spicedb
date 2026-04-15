package queryopt

import (
	"slices"

	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "absorption-idempotency",
		Description: `
		Removes subsumed branches from union expressions using two set-theoretic laws:
		  - Idempotency: A ∪ A = A
		  - Absorption:  A ∪ (A ∩ B) = A; generalizes to (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B

		Caveats are treated as opaque structural units: A ∪ Caveat(A) is not simplified,
		but A ∪ (A ∩ Caveat(B)) = A because A appears as a direct factor.

		Arrows are treated as opaque structural units: (A->B) ∪ (A->B ∩ C) = A->B,
		but (A->B) ∪ (A->C) is not simplified.

		Runs after simple-caveat-pushdown (priority 20) so it sees the final caveat
		shape, and before reachability-pruning (priority 0) so pruning works on a
		smaller tree.
		`,
		Priority: 10,
		NewTransform: func(_ RequestParams) OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{
					absorptionIdempotency,
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
	if outline.Type != query.UnionIteratorType {
		return outline
	}
	if len(outline.SubOutlines) <= 1 {
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
			if isSubsumedBy(y, x) {
				keep[i] = false
				break
			}
		}
	}

	// If everything was kept, return the original outline.
	if slices.Index(keep, false) == -1 {
		return outline
	}

	newSubs := make([]query.Outline, 0, len(children))
	for i, k := range keep {
		if k {
			newSubs = append(newSubs, children[i])
		}
	}

	// Collapse a single surviving child to avoid a redundant Union wrapper.
	if len(newSubs) == 1 {
		return newSubs[0]
	}

	// Leave ID=0 so FillMissingNodeIDs assigns a fresh canonical key.
	return query.Outline{
		Type:        query.UnionIteratorType,
		Args:        outline.Args,
		SubOutlines: newSubs,
	}
}

// isSubsumedBy reports whether Y ⊆ X, i.e. X ∪ Y = X.
//
// Case 1 — Idempotency: X and Y are structurally identical.
//
// Case 2 — Absorption: Y is an IntersectionIteratorType and every factor of X
// (see intersectionFactors) appears as a direct child of Y by structural equality.
// If Y = X₁ ∩ … ∩ Xₙ ∩ B ∩ …  and {X₁, …, Xₙ} are all factors of X, then
// anything satisfying Y must satisfy X, so Y ⊆ X.
//
// Case 3 — Complement-absorption: Y is an ExclusionIteratorType and its minuend
// (first child) is itself subsumed by X. Since (A − B) ⊆ A for all B, if X
// subsumes A then X subsumes (A − B).
func isSubsumedBy(y, x query.Outline) bool {
	// Case 1: structural equality
	if query.OutlineCompare(x, y) == 0 {
		return true
	}

	// Case 3: complement-absorption — (A − B) ⊆ A, so if X subsumes the minuend
	// of Y then X subsumes all of Y.
	// The len == 2 guard is defensive; the outline runtime enforces exactly two
	// children for ExclusionIteratorType. A malformed node falls through safely
	// to Case 2, which returns false for non-intersection types.
	if y.Type == query.ExclusionIteratorType && len(y.SubOutlines) == 2 {
		return isSubsumedBy(y.SubOutlines[0], x)
	}

	// Case 2: Y must be an intersection for absorption to apply
	if y.Type != query.IntersectionIteratorType {
		return false
	}

	// Every factor of X must appear as a direct child of Y
	for _, factor := range intersectionFactors(x) {
		found := false
		for _, yChild := range y.SubOutlines {
			if query.OutlineCompare(factor, yChild) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
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
	if outline.Type != query.ExclusionIteratorType {
		return outline
	}
	if len(outline.SubOutlines) != 2 {
		return outline
	}
	if query.OutlineCompare(outline.SubOutlines[0], outline.SubOutlines[1]) == 0 {
		return query.Outline{Type: query.NullIteratorType}
	}
	return outline
}

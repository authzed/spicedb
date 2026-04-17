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
		annihilates exclusions whose minuend is a subset of the subtrahend, using
		ten set-theoretic laws:

		  Normalization (pre-pass):
		    - Associativity:         (A ∪ B) ∪ C = A ∪ B ∪ C  (and same for ∩)

		  Union laws (drop Y when Y ⊆ X for some sibling X):
		    - Idempotency:           A ∪ A = A
		    - Absorption:            A ∪ (A ∩ B) = A; generalizes to
		                             (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B
		    - Complement-absorption: A ∪ (A − B) = A (since (A−B) ⊆ A)

		  Intersection laws (drop Y when X ⊆ Y for some sibling X):
		    - Idempotency:  A ∩ A = A
		    - Absorption:   A ∩ (A ∪ B) = A (A is a stricter constraint than A ∪ B)
		    - Complement:   Y ∩ (A − C) = ∅ when Y ⊆ C

		  Exclusion laws:
		    - Annihilation:    X − A = ∅ when X ⊆ A (includes A − A = ∅ as the self case)
		    - Left-pruning:    (A ∪ B) − Y = B − Y when A ⊆ Y
		    - Null identity:   A − ∅ = A

		Caveats and arrows are treated as opaque structural units throughout.
		Runs after simple-caveat-pushdown (priority 20) so it sees the final caveat
		shape, and before reachability-pruning (priority 0) so pruning works on a
		smaller tree.
		`,
		Priority: 10,
		NewTransform: func(_ RequestParams) OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{
					flattenAssociativity,
					absorptionIdempotency,
					intersectionIdempotencyAbsorption,
					exclusionAnnihilation,
					exclusionNullIdentity,
					exclusionLeftPruning,
					intersectionComplementAnnihilation,
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

// flattenAssociativity is an OutlineMutation that inlines nested same-type
// union or intersection nodes into their parent, normalizing the tree to a
// flat n-ary form before absorption rules run.
//
// This is a prerequisite for absorption: without flattening, children of a
// nested union are not visible as peers to the outer union's other children,
// so absorption rules cannot compare them.
//
// Example: Union[A, Union[A∩B, C]] → Union[A, A∩B, C]
//
//	→ (after absorptionIdempotency) Union[A, C]
//
// A single surviving child is unwrapped as with other composite-node mutations.
func flattenAssociativity(outline query.Outline) query.Outline {
	if outline.Type != query.UnionIteratorType && outline.Type != query.IntersectionIteratorType {
		return outline
	}

	var newChildren []query.Outline
	changed := false
	for _, child := range outline.SubOutlines {
		if child.Type == outline.Type {
			newChildren = append(newChildren, child.SubOutlines...)
			changed = true
		} else {
			newChildren = append(newChildren, child)
		}
	}

	if !changed {
		return outline
	}

	if len(newChildren) == 1 {
		return newChildren[0]
	}

	return query.Outline{
		Type:        outline.Type,
		Args:        outline.Args,
		SubOutlines: newChildren,
	}
}

// exclusionNullIdentity is an OutlineMutation that simplifies A − ∅ = A.
// When the subtrahend of an exclusion is NullIteratorType, subtracting nothing
// leaves the minuend unchanged, so the exclusion wrapper is dropped entirely.
//
// NullPropagation already handles the left-null case (∅ − A = ∅); this handles
// the right-null case. It lives here rather than in NullPropagation so that
// reachability pruning, which also calls NullPropagation, is not affected.
func exclusionNullIdentity(outline query.Outline) query.Outline {
	if outline.Type == query.ExclusionIteratorType &&
		len(outline.SubOutlines) == 2 &&
		outline.SubOutlines[1].Type == query.NullIteratorType {
		return outline.SubOutlines[0]
	}
	return outline
}

// exclusionLeftPruning is an OutlineMutation that removes union children from
// the left side of an exclusion when those children are subsumed by the
// subtrahend (right side). Elements of a subsumed child are entirely within
// the subtrahend and would all be removed by the subtraction anyway.
//
// Law: (A ∪ B) − Y = B − Y  when A ⊆ Y
//
// If all union children are pruned the union is replaced with NullIteratorType;
// NullPropagation then converts ∅ − Y = ∅ in the same MutateOutline pass.
//
// Reuses isSubsumedBy unchanged.
func exclusionLeftPruning(outline query.Outline) query.Outline {
	if outline.Type != query.ExclusionIteratorType || len(outline.SubOutlines) != 2 {
		return outline
	}

	left, right := outline.SubOutlines[0], outline.SubOutlines[1]
	if left.Type != query.UnionIteratorType {
		return outline
	}

	keep := slices.Repeat([]bool{true}, len(left.SubOutlines))
	anyPruned := false
	for i, child := range left.SubOutlines {
		if isSubsumedBy(child, right) {
			keep[i] = false
			anyPruned = true
		}
	}

	if !anyPruned {
		return outline
	}

	var survivors []query.Outline
	for i, child := range left.SubOutlines {
		if keep[i] {
			survivors = append(survivors, child)
		}
	}

	var newLeft query.Outline
	switch len(survivors) {
	case 0:
		newLeft = query.Outline{Type: query.NullIteratorType}
	case 1:
		newLeft = survivors[0]
	default:
		newLeft = query.Outline{Type: query.UnionIteratorType, SubOutlines: survivors}
	}

	return query.Outline{
		Type:        query.ExclusionIteratorType,
		Args:        outline.Args,
		SubOutlines: []query.Outline{newLeft, right},
	}
}

// intersectionComplementAnnihilation is an OutlineMutation that replaces an
// intersection with ∅ when it contains an ExclusionIteratorType child (A − C)
// and a sibling Y where Y ⊆ C.
//
// Law: Y ∩ (A − C) = ∅  when Y ⊆ C
//
// Proof: elements of A − C are outside C by definition; elements of Y are
// inside C (since Y ⊆ C); the two sets are disjoint so their intersection
// is empty.
//
// Reuses isSubsumedBy unchanged.
func intersectionComplementAnnihilation(outline query.Outline) query.Outline {
	if outline.Type != query.IntersectionIteratorType {
		return outline
	}

	for i, child := range outline.SubOutlines {
		if child.Type != query.ExclusionIteratorType || len(child.SubOutlines) != 2 {
			continue
		}
		subtrahend := child.SubOutlines[1]
		for j, sibling := range outline.SubOutlines {
			if i == j {
				continue
			}
			if isSubsumedBy(sibling, subtrahend) {
				return query.Outline{Type: query.NullIteratorType}
			}
		}
	}

	return outline
}

// exclusionAnnihilation is an OutlineMutation that replaces X − A with ∅ when
// X ⊆ A (i.e. when the minuend is subsumed by the subtrahend). If everything
// satisfying X also satisfies A, then subtracting A removes everything.
//
// This generalizes the self case (A − A = ∅) to cover, for example:
//
//   - (A ∩ B) − A = ∅  (A ∩ B ⊆ A)
//   - (A − B) − A = ∅  (A − B ⊆ A for all B)
//   - A − (A ∪ B) = ∅  (A ⊆ A ∪ B)
//
// NullPropagation (run after this mutation in the same MutateOutline call) then
// cascades the null through parent intersections and caveats.
func exclusionAnnihilation(outline query.Outline) query.Outline {
	if outline.Type == query.ExclusionIteratorType &&
		len(outline.SubOutlines) == 2 &&
		isSubsumedBy(outline.SubOutlines[0], outline.SubOutlines[1]) {
		return query.Outline{Type: query.NullIteratorType}
	}
	return outline
}

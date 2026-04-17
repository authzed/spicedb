package queryopt

import (
	"slices"

	"github.com/authzed/spicedb/pkg/query"
)

func init() {
	MustRegisterOptimization(Optimizer{
		Name: "set-simplification",
		Description: `
		Removes subsumed branches from union and intersection expressions, and
		annihilates exclusions whose minuend is a subset of the subtrahend, using
		ten set-theoretic laws applied in the order listed:

		  Normalization:
		    - Associativity:         (A ∪ B) ∪ C = A ∪ B ∪ C  (and same for ∩)

		  Union laws (drop Y when Y ⊆ X for some sibling X):
		    - Idempotency:           A ∪ A = A
		    - Absorption:            A ∪ (A ∩ B) = A
		    - Complement-absorption: A ∪ (A − B) = A

		  Intersection laws (drop Y when X ⊆ Y for some sibling X):
		    - Idempotency:           A ∩ A = A
		    - Absorption:            A ∩ (A ∪ B) = A
		    - Complement:            Y ∩ (A − C) = ∅ when Y ⊆ C

		  Exclusion laws:
		    - Annihilation:          X − A = ∅ when X ⊆ A
		    - Left-pruning:          (A ∪ B) − Y = B − Y when A ⊆ Y
		    - Null identity:         A − ∅ = A

		Caveats and arrows are treated as opaque structural units throughout:
		isSubsumedBy never looks inside them, so two caveat nodes or arrow nodes
		are equal only when structurally identical.

		Runs after simple-caveat-pushdown (priority 20) so it sees the final caveat
		shape, and before reachability-pruning (priority 0) so pruning works on a
		smaller tree.
		`,
		Priority: 10,
		NewTransform: func(_ RequestParams) OutlineTransform {
			return func(outline query.Outline) query.Outline {
				return query.MutateOutline(outline, []query.OutlineMutation{
					flattenAssociativity,
					unionIdempotency,
					unionAbsorption,
					unionComplementAbsorption,
					intersectionIdempotency,
					intersectionAbsorption,
					intersectionComplementAnnihilation,
					exclusionAnnihilation,
					exclusionLeftPruning,
					exclusionNullIdentity,
					query.NullPropagation,
				})
			}
		},
	})
}

// flattenAssociativity is an OutlineMutation that inlines nested same-type union
// or intersection children into their parent, normalizing to a flat n-ary form.
// Law: (A ∪ B) ∪ C = A ∪ B ∪ C  (and same for ∩)
//
// This runs first so that all siblings are visible at the same level before any
// absorption rule runs. Without it, A and A∩B in Union[A, Union[A∩B, C]] are not
// peers and unionAbsorption cannot fire.
//
// A single surviving child is returned unwrapped to avoid a redundant wrapper.
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

// unionIdempotency is an OutlineMutation that collapses structurally identical
// children in a union.
// Law: A ∪ A = A
func unionIdempotency(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.UnionIteratorType, func(y, x query.Outline) bool {
		return query.OutlineCompare(x, y) == 0
	})
}

// unionAbsorption is an OutlineMutation that drops intersection children from a
// union when they are subsumed by a sibling.
// Law: A ∪ (A ∩ B) = A; generalizes to (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B.
//
// A child Y is dropped when Y is an intersection and every factor of some sibling X
// appears among Y's direct children. Anything satisfying Y must also satisfy X, so
// the union already covers Y via X.
func unionAbsorption(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.UnionIteratorType, func(y, x query.Outline) bool {
		if y.Type != query.IntersectionIteratorType {
			return false
		}
		for _, factor := range intersectionFactors(x) {
			if !slices.ContainsFunc(y.SubOutlines, func(c query.Outline) bool {
				return query.OutlineCompare(factor, c) == 0
			}) {
				return false
			}
		}
		return true
	})
}

// unionComplementAbsorption is an OutlineMutation that drops exclusion children
// from a union when they are subsumed by a sibling.
// Law: A ∪ (A − B) = A
//
// A child Y is dropped when Y is an exclusion (M − S) and the minuend M is
// subsumed by some sibling X. Since (M − S) ⊆ M ⊆ X, the union already covers Y.
func unionComplementAbsorption(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.UnionIteratorType, func(y, x query.Outline) bool {
		return y.Type == query.ExclusionIteratorType &&
			len(y.SubOutlines) == 2 &&
			isSubsumedBy(y.SubOutlines[0], x)
	})
}

// intersectionIdempotency is an OutlineMutation that collapses structurally
// identical children in an intersection.
// Law: A ∩ A = A
func intersectionIdempotency(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.IntersectionIteratorType, func(y, x query.Outline) bool {
		return query.OutlineCompare(x, y) == 0
	})
}

// intersectionAbsorption is an OutlineMutation that drops weaker children from an
// intersection when a stricter sibling is present.
// Law: A ∩ (A ∪ B) = A
//
// A child Y is dropped when some sibling X satisfies X ⊆ Y. The union A ∪ B is a
// weaker constraint than A alone, so intersecting with both is redundant.
//
// This also subsumes the case where two intersection children differ in specificity:
// (A ∩ B) ∩ (A ∩ B ∩ C) = A ∩ B ∩ C, because A ∩ B ∩ C ⊆ A ∩ B.
func intersectionAbsorption(outline query.Outline) query.Outline {
	return eliminateRedundantChildren(outline, query.IntersectionIteratorType, func(y, x query.Outline) bool {
		return isSubsumedBy(x, y)
	})
}

// intersectionComplementAnnihilation is an OutlineMutation that replaces an
// intersection with ∅ when it contains an exclusion child (A − C) and a sibling
// Y where Y ⊆ C.
// Law: Y ∩ (A − C) = ∅  when Y ⊆ C
//
// Elements of A − C are outside C by definition; elements of Y are inside C
// (since Y ⊆ C); the two sets are disjoint so their intersection is empty.
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
// X ⊆ A — if everything satisfying X also satisfies A, subtracting A removes all of X.
// Law: X − A = ∅  when X ⊆ A
//
// Examples: A − A = ∅, (A ∩ B) − A = ∅, A − (A ∪ B) = ∅.
//
// NullPropagation cascades the resulting null through any parent nodes.
func exclusionAnnihilation(outline query.Outline) query.Outline {
	if outline.Type == query.ExclusionIteratorType &&
		len(outline.SubOutlines) == 2 &&
		isSubsumedBy(outline.SubOutlines[0], outline.SubOutlines[1]) {
		return query.Outline{Type: query.NullIteratorType}
	}
	return outline
}

// exclusionLeftPruning is an OutlineMutation that removes union children from the
// left side of an exclusion when they are subsumed by the subtrahend. Elements of
// a subsumed child would be fully removed by the subtraction anyway.
// Law: (A ∪ B) − Y = B − Y  when A ⊆ Y
//
// If all union children are pruned, the minuend becomes NullIteratorType and
// NullPropagation converts ∅ − Y = ∅ in the same pass.
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

// exclusionNullIdentity is an OutlineMutation that drops the exclusion wrapper
// when the subtrahend is null — subtracting nothing leaves the minuend unchanged.
// Law: A − ∅ = A
//
// NullPropagation handles the symmetric left-null case (∅ − A = ∅). This rule
// lives here rather than in NullPropagation so that reachability pruning, which
// also calls NullPropagation, is not affected.
func exclusionNullIdentity(outline query.Outline) query.Outline {
	if outline.Type == query.ExclusionIteratorType &&
		len(outline.SubOutlines) == 2 &&
		outline.SubOutlines[1].Type == query.NullIteratorType {
		return outline.SubOutlines[0]
	}
	return outline
}

// eliminateRedundantChildren removes children from a composite node (union or
// intersection) for which shouldDrop(Y, X) returns true for some surviving sibling X.
//
// The keep-bitmap approach ensures a child already marked for removal cannot also
// become the justification for removing a sibling: in A ∪ A, the first copy marks
// the second for removal without letting the second also mark the first.
//
// A single surviving child is returned unwrapped to avoid a redundant wrapper.
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

// isSubsumedBy reports whether Y ⊆ X — every element of Y is also an element of X.
//
// Caveats and arrows are treated as opaque: isSubsumedBy never looks inside them,
// so two caveat or arrow nodes are equal only when structurally identical.
//
// Four structural patterns are recognized:
//
//   - Equality: X and Y are structurally identical.
//   - Union membership: X is a union and Y is one of its direct children.
//     Anything satisfying Y trivially satisfies the union, so Y ⊆ X.
//   - Exclusion: Y is an exclusion (M − S) whose minuend M satisfies M ⊆ X.
//     Since (M − S) ⊆ M for all S, Y ⊆ M ⊆ X.
//   - Intersection: Y is an intersection and every factor of X (see
//     intersectionFactors) appears among Y's direct children. If Y = X₁ ∩ … ∩ Xₙ ∩ …
//     and {X₁, …, Xₙ} are all factors of X, then satisfying Y requires satisfying
//     each Xᵢ, so Y ⊆ X.
func isSubsumedBy(y, x query.Outline) bool {
	switch {
	case query.OutlineCompare(x, y) == 0:
		return true

	case x.Type == query.UnionIteratorType:
		return slices.ContainsFunc(x.SubOutlines, func(c query.Outline) bool {
			return query.OutlineCompare(y, c) == 0
		})

	case y.Type == query.ExclusionIteratorType && len(y.SubOutlines) == 2:
		return isSubsumedBy(y.SubOutlines[0], x)

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

// intersectionFactors returns the atomic operands of X for subsumption checks.
// For an intersection, the factors are its direct children; otherwise X is its
// own single factor.
//
//	factors of Intersection[A, B] = {A, B}
//	factors of A                  = {A}
func intersectionFactors(x query.Outline) []query.Outline {
	if x.Type == query.IntersectionIteratorType {
		return x.SubOutlines
	}
	return []query.Outline{x}
}

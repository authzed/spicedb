package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

// applyAbsorption runs all absorption-family mutations bottom-up over outline.
func applyAbsorption(outline query.Outline) query.Outline {
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

func TestAbsorptionIdempotency(t *testing.T) {
	// Leaf nodes used as distinct structural units throughout these tests.
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("idempotency: union of two identical children collapses to one", func(t *testing.T) {
		// Union[A, A] → A
		result := applyAbsorption(unionOutline(a, a))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("idempotency: removes one duplicate in three-child union", func(t *testing.T) {
		// Union[A, A, B] → Union[A, B]
		result := applyAbsorption(unionOutline(a, a, b))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("absorption: A ∪ (A ∩ B) = A", func(t *testing.T) {
		// Union[A, Intersection[A, B]] → A
		result := applyAbsorption(unionOutline(a, intersectionOutline(a, b)))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("absorption: (A ∩ B) ∪ A = A — intersection on left", func(t *testing.T) {
		// Union[Intersection[A, B], A] → A  (same law, intersection child first)
		result := applyAbsorption(unionOutline(intersectionOutline(a, b), a))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("absorption: A ∪ (A ∩ B ∩ C) = A", func(t *testing.T) {
		// Union[A, Intersection[A, B, C]] → A
		result := applyAbsorption(unionOutline(a, intersectionOutline(a, b, c)))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("generalized absorption: (A ∩ B) ∪ (A ∩ B ∩ C) = A ∩ B", func(t *testing.T) {
		// Union[Intersection[A,B], Intersection[A,B,C]] → Intersection[A,B]
		ab := intersectionOutline(a, b)
		abc := intersectionOutline(a, b, c)
		result := applyAbsorption(unionOutline(ab, abc))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, ab))
	})

	t.Run("multi-child: A ∪ B ∪ (A ∩ B) = A ∪ B", func(t *testing.T) {
		// Union[A, B, Intersection[A, B]] → Union[A, B]
		result := applyAbsorption(unionOutline(a, b, intersectionOutline(a, b)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("no-op: distinct children, nothing to remove", func(t *testing.T) {
		// Union[A, B, C] → unchanged
		result := applyAbsorption(unionOutline(a, b, c))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
	})

	t.Run("no-op: non-union node is returned unchanged", func(t *testing.T) {
		// Intersection[A, B] → unchanged
		result := applyAbsorption(intersectionOutline(a, b))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})

	t.Run("nested: flattening + dedup yields flat union via MutateOutline", func(t *testing.T) {
		// Union[C, Union[A, A, B]]: inner dedup fires bottom-up (A,A→A), then
		// flattenAssociativity inlines the result → Union[C, A, B].
		inner := unionOutline(a, a, b)
		outer := unionOutline(c, inner)
		result := applyAbsorption(outer)
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], c))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[2], b))
	})
}

func TestAbsorptionCaveatHandling(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	cavA := caveatOutline("is_admin", a)

	t.Run("A ∪ Caveat(A) is not simplified — structurally distinct", func(t *testing.T) {
		// A and Caveat(A) are different structural nodes; no subsumption applies.
		result := applyAbsorption(unionOutline(a, cavA))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], cavA))
	})

	t.Run("A ∪ (A ∩ Caveat(B)) = A — A appears as direct factor in intersection", func(t *testing.T) {
		// Everything satisfying (A ∩ Caveat(B)) also satisfies A.
		result := applyAbsorption(unionOutline(a, intersectionOutline(a, caveatOutline("some_caveat", b))))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("Caveat(A) ∪ (Caveat(A) ∩ B) = Caveat(A)", func(t *testing.T) {
		// Caveat(A) appears as a direct factor in Intersection[Caveat(A), B].
		result := applyAbsorption(unionOutline(cavA, intersectionOutline(cavA, b)))
		require.Equal(t, query.CaveatIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, cavA))
	})
}

func TestAbsorptionArrowHandling(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")
	arrowAB := arrowOutline(a, b)
	arrowAC := arrowOutline(a, c)

	t.Run("(A->B) ∪ (A->B) = A->B — idempotency on arrows", func(t *testing.T) {
		result := applyAbsorption(unionOutline(arrowAB, arrowAB))
		require.Equal(t, query.ArrowIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, arrowAB))
	})

	t.Run("(A->B) ∪ (A->B ∩ C) = A->B — arrow as opaque factor in intersection", func(t *testing.T) {
		result := applyAbsorption(unionOutline(arrowAB, intersectionOutline(arrowAB, c)))
		require.Equal(t, query.ArrowIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, arrowAB))
	})

	t.Run("(A->B) ∪ (A->C) is not simplified — distinct arrows", func(t *testing.T) {
		result := applyAbsorption(unionOutline(arrowAB, arrowAC))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})

	t.Run("(A=>B) ∪ (A=>B ∩ C) = A=>B — intersection arrow as opaque factor", func(t *testing.T) {
		iArrowAB := intersectionArrowOutline(a, b)
		result := applyAbsorption(unionOutline(iArrowAB, intersectionOutline(iArrowAB, c)))
		require.Equal(t, query.IntersectionArrowIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, iArrowAB))
	})
}

func TestAbsorptionRegistered(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")

	t.Run("optimizer is registered and applies via ApplyOptimizations", func(t *testing.T) {
		// Union[A, Intersection[A, B]] should reduce to A via the named optimizer.
		input := unionOutline(a, intersectionOutline(a, b))
		co := canonicalize(input)

		result, err := ApplyOptimizations(co, []string{"set-simplification"}, RequestParams{})
		require.NoError(t, err)
		require.Equal(t, query.DatastoreIteratorType, result.Root.Type)
		require.Equal(t, 0, query.OutlineCompare(result.Root, a))
	})
}

func TestComplementAbsorption(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("A ∪ (A − B) = A", func(t *testing.T) {
		// (A−B) ⊆ A, so A already subsumes the exclusion.
		result := applyAbsorption(unionOutline(a, exclusionOutline(a, b)))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("(A − B) ∪ A = A — exclusion on left", func(t *testing.T) {
		// Same law with operands reversed; order must not matter.
		result := applyAbsorption(unionOutline(exclusionOutline(a, b), a))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("(A ∩ B) ∪ ((A ∩ B) − C) = A ∩ B", func(t *testing.T) {
		// Minuend is an intersection; works identically — (A∩B)−C ⊆ A∩B.
		ab := intersectionOutline(a, b)
		result := applyAbsorption(unionOutline(ab, exclusionOutline(ab, c)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, ab))
	})

	t.Run("A ∪ B ∪ (A − C) = A ∪ B", func(t *testing.T) {
		// A subsumes (A−C); B is unaffected; result is a 2-child union.
		result := applyAbsorption(unionOutline(a, b, exclusionOutline(a, c)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("A ∪ (B − C) is not simplified — B is not subsumed by A", func(t *testing.T) {
		// B's minuend is B, not A; no subsumption.
		result := applyAbsorption(unionOutline(a, exclusionOutline(b, c)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})

	t.Run("(A − B) ∪ (A − C) is not simplified — neither exclusion subsumes the other", func(t *testing.T) {
		// Both (A−B) and (A−C) have minuend A; A−C ⊆ A−B is not generally true,
		// but A subsumes both minuends, so if A also appears as a union sibling the
		// rule fires. Without a bare A sibling, neither exclusion subsumes the other.
		// This test confirms no simplification occurs when no bare A is present.
		result := applyAbsorption(unionOutline(exclusionOutline(a, b), exclusionOutline(a, c)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})
}

func TestExclusionAnnihilation(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")

	t.Run("A − A = ∅ — self case (structural equality)", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(a, a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(A ∩ B) − (A ∩ B) = ∅ — compound minuend and subtrahend", func(t *testing.T) {
		ab := intersectionOutline(a, b)
		result := applyAbsorption(exclusionOutline(ab, ab))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(A ∩ B) − A = ∅ — minuend is a strict subset of subtrahend", func(t *testing.T) {
		// A ∩ B ⊆ A, so subtracting A removes everything.
		result := applyAbsorption(exclusionOutline(intersectionOutline(a, b), a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(A − B) − A = ∅ — complement-absorption: A−B ⊆ A", func(t *testing.T) {
		// A−B ⊆ A for all B, so subtracting A removes everything.
		result := applyAbsorption(exclusionOutline(exclusionOutline(a, b), a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("A − (A ∪ B) = ∅ — minuend is a child of subtrahend union", func(t *testing.T) {
		// A ⊆ (A ∪ B), so subtracting A ∪ B removes everything.
		result := applyAbsorption(exclusionOutline(a, unionOutline(a, b)))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("A − B is not simplified — structurally distinct operands", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(a, b))
		require.Equal(t, query.ExclusionIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("null propagates to parent intersection: B ∩ (A − A) = ∅", func(t *testing.T) {
		// After A−A becomes Null, NullPropagation converts B ∩ Null to Null.
		result := applyAbsorption(intersectionOutline(b, exclusionOutline(a, a)))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("Union[A, (A − A)] leaves Union[A, Null] — null-in-union cleanup is out of scope", func(t *testing.T) {
		// exclusionAnnihilation turns (A−A) into Null bottom-up before the parent
		// Union is visited. NullPropagation only collapses a Union to Null when ALL
		// children are null, so Union[A, Null] remains. The runtime union iterator
		// evaluates both branches and unions the results; the null branch contributes
		// nothing, so A ∪ ∅ = A is semantically correct at runtime.
		result := applyAbsorption(unionOutline(b, exclusionOutline(a, a)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], b))
		require.Equal(t, query.NullIteratorType, result.SubOutlines[1].Type)
	})
}

func TestIntersectionIdempotencyAbsorption(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("idempotency: A ∩ A = A", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, a))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("idempotency: removes one duplicate in three-child intersection", func(t *testing.T) {
		// A ∩ A ∩ B → A ∩ B
		result := applyAbsorption(intersectionOutline(a, a, b))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("absorption: A ∩ (A ∪ B) = A", func(t *testing.T) {
		// (A ∪ B) is a weaker constraint than A alone; drop the union.
		result := applyAbsorption(intersectionOutline(a, unionOutline(a, b)))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("absorption: (A ∪ B) ∩ A = A — union on left", func(t *testing.T) {
		// Same law with operands swapped; order must not matter.
		result := applyAbsorption(intersectionOutline(unionOutline(a, b), a))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("multi-child: A ∩ B ∩ (A ∪ B) = A ∩ B", func(t *testing.T) {
		// Both A and B subsume the union; drop the union, keep A and B.
		result := applyAbsorption(intersectionOutline(a, b, unionOutline(a, b)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
	})

	t.Run("no-op: distinct children, nothing to remove", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, b, c))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
	})

	t.Run("no-op: A ∩ (B ∪ C) — A does not appear in the union", func(t *testing.T) {
		// A is not a child of (B ∪ C), so no simplification.
		result := applyAbsorption(intersectionOutline(a, unionOutline(b, c)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})
}

func TestFlattenAssociativity(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("Union[A, Union[B, C]] flattens to Union[A, B, C]", func(t *testing.T) {
		result := applyAbsorption(unionOutline(a, unionOutline(b, c)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[2], c))
	})

	t.Run("Union[Union[A, B], C] flattens to Union[A, B, C]", func(t *testing.T) {
		result := applyAbsorption(unionOutline(unionOutline(a, b), c))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[2], c))
	})

	t.Run("Intersection[A, Intersection[B, C]] flattens to Intersection[A, B, C]", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, intersectionOutline(b, c)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], b))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[2], c))
	})

	t.Run("Union[A, Union[A∩B, C]] flattens then absorbs to Union[A, C]", func(t *testing.T) {
		// Without flattening, A and A∩B are not peers so absorption cannot fire.
		// After flattening to Union[A, A∩B, C], absorptionIdempotency drops A∩B.
		result := applyAbsorption(unionOutline(a, unionOutline(intersectionOutline(a, b), c)))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], c))
	})

	t.Run("no-op: already-flat union is unchanged", func(t *testing.T) {
		result := applyAbsorption(unionOutline(a, b, c))
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
	})

	t.Run("no-op: union nested inside intersection is not flattened into it", func(t *testing.T) {
		// Union children are not inlined into a parent Intersection.
		result := applyAbsorption(intersectionOutline(a, unionOutline(b, c)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})
}

func TestExclusionNullIdentity(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	null := query.Outline{Type: query.NullIteratorType}

	t.Run("A − ∅ = A — right-null exclusion reduces to left child", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(a, null))
		require.Equal(t, query.DatastoreIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, a))
	})

	t.Run("(A ∩ B) − ∅ = A ∩ B — compound left child preserved", func(t *testing.T) {
		ab := intersectionOutline(a, b)
		result := applyAbsorption(exclusionOutline(ab, null))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result, ab))
	})

	t.Run("∅ − A = ∅ — left-null case is unchanged (already covered by NullPropagation)", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(null, a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})
}

func TestExclusionLeftPruning(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("(A ∪ B) − A = B − A — subsumed child pruned from union", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(unionOutline(a, b), a))
		require.Equal(t, query.ExclusionIteratorType, result.Type)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], b))
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[1], a))
	})

	t.Run("(A ∪ B ∪ C) − A = (B ∪ C) − A — one of three children pruned", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(unionOutline(a, b, c), a))
		require.Equal(t, query.ExclusionIteratorType, result.Type)
		left := result.SubOutlines[0]
		require.Equal(t, query.UnionIteratorType, left.Type)
		require.Len(t, left.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(left.SubOutlines[0], b))
		require.Equal(t, 0, query.OutlineCompare(left.SubOutlines[1], c))
	})

	t.Run("(A ∪ B) − (A ∪ B) = ∅ — all children pruned annihilates", func(t *testing.T) {
		y := unionOutline(a, b)
		result := applyAbsorption(exclusionOutline(unionOutline(a, b), y))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("no-op: (A ∪ B) − C — neither A nor B is subsumed by C", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(unionOutline(a, b), c))
		require.Equal(t, query.ExclusionIteratorType, result.Type)
		require.Equal(t, query.UnionIteratorType, result.SubOutlines[0].Type)
		require.Len(t, result.SubOutlines[0].SubOutlines, 2)
	})

	t.Run("no-op: non-union left child is untouched", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(a, a))
		// exclusionAnnihilation fires first (A − A = ∅), not exclusionLeftPruning
		require.Equal(t, query.NullIteratorType, result.Type)
	})
}

func TestIntersectionComplementAnnihilation(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")
	c := dsOutlineForType("document", "owner", "user", "...")

	t.Run("A ∩ (B − A) = ∅ — sibling equals subtrahend", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, exclusionOutline(b, a)))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(B − A) ∩ A = ∅ — exclusion child first", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(exclusionOutline(b, a), a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("A ∩ B ∩ (C − A) = ∅ — sibling A ⊆ A (subtrahend of exclusion)", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, b, exclusionOutline(c, a)))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(A ∩ B) ∩ (C − (A ∪ D)) = ∅ — sibling A ⊆ A ∪ D", func(t *testing.T) {
		d := dsOutlineForType("document", "banned", "user", "...")
		subtrahend := unionOutline(a, d)
		result := applyAbsorption(intersectionOutline(intersectionOutline(a, b), exclusionOutline(c, subtrahend)))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("no-op: A ∩ (B − C) — A is not subsumed by C", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, exclusionOutline(b, c)))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})

	t.Run("no-op: intersection with no exclusion child", func(t *testing.T) {
		result := applyAbsorption(intersectionOutline(a, b))
		require.Equal(t, query.IntersectionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
	})
}

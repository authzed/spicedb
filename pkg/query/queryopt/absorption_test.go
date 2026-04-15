package queryopt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
)

// applyAbsorption runs the absorptionIdempotency mutation bottom-up over outline.
func applyAbsorption(outline query.Outline) query.Outline {
	return query.MutateOutline(outline, []query.OutlineMutation{
		absorptionIdempotency,
		exclusionSelfAnnihilation,
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

	t.Run("nested: applies at all depths via MutateOutline", func(t *testing.T) {
		// Union[C, Union[A, A, B]] → Union[C, Union[A, B]]
		// (inner union is processed first since MutateOutline is bottom-up)
		inner := unionOutline(a, a, b)
		outer := unionOutline(c, inner)
		result := applyAbsorption(outer)
		require.Equal(t, query.UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(result.SubOutlines[0], c))

		innerResult := result.SubOutlines[1]
		require.Equal(t, query.UnionIteratorType, innerResult.Type)
		require.Len(t, innerResult.SubOutlines, 2)
		require.Equal(t, 0, query.OutlineCompare(innerResult.SubOutlines[0], a))
		require.Equal(t, 0, query.OutlineCompare(innerResult.SubOutlines[1], b))
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

		result, err := ApplyOptimizations(co, []string{"absorption-idempotency"}, RequestParams{})
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

func TestExclusionSelfAnnihilation(t *testing.T) {
	a := dsOutlineForType("document", "viewer", "user", "...")
	b := dsOutlineForType("document", "editor", "user", "...")

	t.Run("A − A = ∅", func(t *testing.T) {
		result := applyAbsorption(exclusionOutline(a, a))
		require.Equal(t, query.NullIteratorType, result.Type)
	})

	t.Run("(A ∩ B) − (A ∩ B) = ∅ — compound minuend and subtrahend", func(t *testing.T) {
		ab := intersectionOutline(a, b)
		result := applyAbsorption(exclusionOutline(ab, ab))
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
}

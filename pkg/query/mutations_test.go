package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNullPropagation(t *testing.T) {
	live := Outline{Type: FixedIteratorType}
	null := Outline{Type: NullIteratorType}

	// --- UnionIteratorType ---

	t.Run("union: all null → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("union: all null (3 children) → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, SubOutlines: []Outline{null, null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("union: one live child → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, SubOutlines: []Outline{live, null}})
		require.Equal(t, UnionIteratorType, result.Type)
	})

	t.Run("union: mixed (3 children, 1 live) → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, SubOutlines: []Outline{null, live, null}})
		require.Equal(t, UnionIteratorType, result.Type)
	})

	t.Run("union: all live → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, SubOutlines: []Outline{live, live}})
		require.Equal(t, UnionIteratorType, result.Type)
	})

	t.Run("union: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: UnionIteratorType, ID: 7, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(7), result.ID)
	})

	// --- IntersectionIteratorType ---

	t.Run("intersection: any null child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, SubOutlines: []Outline{live, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection: null first child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection: all null → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection: one null in 3 children → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, SubOutlines: []Outline{live, null, live}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection: all live → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, SubOutlines: []Outline{live, live}})
		require.Equal(t, IntersectionIteratorType, result.Type)
	})

	t.Run("intersection: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionIteratorType, ID: 8, SubOutlines: []Outline{live, null}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(8), result.ID)
	})

	// --- ArrowIteratorType ---

	t.Run("arrow: null left child → null", func(t *testing.T) {
		// Null → B has no sources to traverse, so result is empty.
		result := NullPropagation(Outline{Type: ArrowIteratorType, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("arrow: null right child → null", func(t *testing.T) {
		// A → Null has no target to reach.
		result := NullPropagation(Outline{Type: ArrowIteratorType, SubOutlines: []Outline{live, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("arrow: both children null → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ArrowIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("arrow: neither child null → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ArrowIteratorType, SubOutlines: []Outline{live, live}})
		require.Equal(t, ArrowIteratorType, result.Type)
	})

	t.Run("arrow: wrong child count (1) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ArrowIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, ArrowIteratorType, result.Type)
	})

	t.Run("arrow: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ArrowIteratorType, ID: 42, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(42), result.ID)
	})

	// --- IntersectionArrowIteratorType ---

	t.Run("intersection arrow: null left child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection arrow: null right child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{live, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection arrow: both children null → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("intersection arrow: neither child null → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{live, live}})
		require.Equal(t, IntersectionArrowIteratorType, result.Type)
	})

	t.Run("intersection arrow: wrong child count (1) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, IntersectionArrowIteratorType, result.Type)
	})

	t.Run("intersection arrow: wrong child count (3) with null child → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, SubOutlines: []Outline{null, live, live}})
		require.Equal(t, IntersectionArrowIteratorType, result.Type)
	})

	t.Run("intersection arrow: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: IntersectionArrowIteratorType, ID: 9, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(9), result.ID)
	})

	// --- ExclusionIteratorType ---

	t.Run("exclusion: null left child (main set) → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ExclusionIteratorType, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("exclusion: null right child (excluded set) → unchanged (A − ∅ = A)", func(t *testing.T) {
		// Subtracting the empty set is a no-op; the node stays for a later pass.
		result := NullPropagation(Outline{Type: ExclusionIteratorType, SubOutlines: []Outline{live, null}})
		require.Equal(t, ExclusionIteratorType, result.Type)
	})

	t.Run("exclusion: both null → null (left child triggers)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ExclusionIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("exclusion: neither null → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ExclusionIteratorType, SubOutlines: []Outline{live, live}})
		require.Equal(t, ExclusionIteratorType, result.Type)
	})

	t.Run("exclusion: wrong child count (1) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ExclusionIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, ExclusionIteratorType, result.Type)
	})

	t.Run("exclusion: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: ExclusionIteratorType, ID: 5, SubOutlines: []Outline{null, live}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(5), result.ID)
	})

	// --- CaveatIteratorType ---

	t.Run("caveat: null child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: CaveatIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("caveat: live child → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: CaveatIteratorType, SubOutlines: []Outline{live}})
		require.Equal(t, CaveatIteratorType, result.Type)
	})

	t.Run("caveat: wrong child count (2) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: CaveatIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, CaveatIteratorType, result.Type)
	})

	t.Run("caveat: preserves ID when null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: CaveatIteratorType, ID: 3, SubOutlines: []Outline{null}})
		require.Equal(t, NullIteratorType, result.Type)
		require.Equal(t, OutlineNodeID(3), result.ID)
	})

	// --- AliasIteratorType ---

	t.Run("alias: null child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: AliasIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("alias: live child → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: AliasIteratorType, SubOutlines: []Outline{live}})
		require.Equal(t, AliasIteratorType, result.Type)
	})

	t.Run("alias: wrong child count (2) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: AliasIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, AliasIteratorType, result.Type)
	})

	// --- RecursiveIteratorType ---

	t.Run("recursive: null child → null", func(t *testing.T) {
		result := NullPropagation(Outline{Type: RecursiveIteratorType, SubOutlines: []Outline{null}})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("recursive: live child → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: RecursiveIteratorType, SubOutlines: []Outline{live}})
		require.Equal(t, RecursiveIteratorType, result.Type)
	})

	t.Run("recursive: wrong child count (2) → unchanged (defensive guard)", func(t *testing.T) {
		result := NullPropagation(Outline{Type: RecursiveIteratorType, SubOutlines: []Outline{null, null}})
		require.Equal(t, RecursiveIteratorType, result.Type)
	})

	// --- Leaf / unhandled types → always unchanged ---

	t.Run("null node itself → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: NullIteratorType})
		require.Equal(t, NullIteratorType, result.Type)
	})

	t.Run("datastore leaf → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: DatastoreIteratorType})
		require.Equal(t, DatastoreIteratorType, result.Type)
	})

	t.Run("self leaf → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: SelfIteratorType})
		require.Equal(t, SelfIteratorType, result.Type)
	})

	t.Run("fixed leaf → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: FixedIteratorType})
		require.Equal(t, FixedIteratorType, result.Type)
	})

	t.Run("recursive sentinel leaf → unchanged", func(t *testing.T) {
		result := NullPropagation(Outline{Type: RecursiveSentinelIteratorType})
		require.Equal(t, RecursiveSentinelIteratorType, result.Type)
	})
}

func TestReorderMutation(t *testing.T) {
	t.Run("reorders children correctly", func(t *testing.T) {
		child0 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("document:doc0#viewer@user:alice")}}}
		child1 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("document:doc1#viewer@user:bob")}}}
		child2 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("document:doc2#viewer@user:charlie")}}}

		outline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{child0, child1, child2},
		}

		// Reorder to [2, 0, 1]
		mutation := ReorderMutation([]int{2, 0, 1})
		result := mutation(outline)

		require.Equal(t, UnionIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 3)
		require.True(t, result.SubOutlines[0].Equals(child2))
		require.True(t, result.SubOutlines[1].Equals(child0))
		require.True(t, result.SubOutlines[2].Equals(child1))
	})

	t.Run("no-op when order length mismatches", func(t *testing.T) {
		child0 := Outline{Type: FixedIteratorType}
		child1 := Outline{Type: FixedIteratorType}

		outline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{child0, child1},
		}

		// Wrong length order
		mutation := ReorderMutation([]int{1, 0, 2})
		result := mutation(outline)

		// Should return unchanged
		require.True(t, result.Equals(outline))
	})

	t.Run("identity reordering", func(t *testing.T) {
		child0 := Outline{Type: FixedIteratorType}
		child1 := Outline{Type: FixedIteratorType}

		outline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{child0, child1},
		}

		// Identity permutation
		mutation := ReorderMutation([]int{0, 1})
		result := mutation(outline)

		require.True(t, result.Equals(outline))
	})

	t.Run("empty outline", func(t *testing.T) {
		outline := Outline{
			Type:        UnionIteratorType,
			SubOutlines: []Outline{},
		}

		mutation := ReorderMutation([]int{})
		result := mutation(outline)

		require.True(t, result.Equals(outline))
	})
}

func TestRotateArrowMutation(t *testing.T) {
	t.Run("rotate left: (A->B)->C to A->(B->C)", func(t *testing.T) {
		// Build A, B, C
		a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:a#viewer@user:alice")}}}
		b := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:b#viewer@user:bob")}}}
		c := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:c#viewer@user:charlie")}}}

		// Build A->B
		ab := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{a, b},
		}

		// Build (A->B)->C
		abThenC := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{ab, c},
		}

		// Apply rotation
		mutation := RotateArrowMutation(true)
		result := mutation(abThenC)

		// Verify structure: A->(B->C)
		require.Equal(t, ArrowIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)

		// First child should be A
		require.True(t, result.SubOutlines[0].Equals(a))

		// Second child should be B->C
		bc := result.SubOutlines[1]
		require.Equal(t, ArrowIteratorType, bc.Type)
		require.Len(t, bc.SubOutlines, 2)
		require.True(t, bc.SubOutlines[0].Equals(b))
		require.True(t, bc.SubOutlines[1].Equals(c))
	})

	t.Run("rotate right: A->(B->C) to (A->B)->C", func(t *testing.T) {
		// Build A, B, C
		a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:a#viewer@user:alice")}}}
		b := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:b#viewer@user:bob")}}}
		c := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{*MustPathFromString("doc:c#viewer@user:charlie")}}}

		// Build B->C
		bc := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{b, c},
		}

		// Build A->(B->C)
		aThenBC := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{a, bc},
		}

		// Apply rotation
		mutation := RotateArrowMutation(false)
		result := mutation(aThenBC)

		// Verify structure: (A->B)->C
		require.Equal(t, ArrowIteratorType, result.Type)
		require.Len(t, result.SubOutlines, 2)

		// First child should be A->B
		ab := result.SubOutlines[0]
		require.Equal(t, ArrowIteratorType, ab.Type)
		require.Len(t, ab.SubOutlines, 2)
		require.True(t, ab.SubOutlines[0].Equals(a))
		require.True(t, ab.SubOutlines[1].Equals(b))

		// Second child should be C
		require.True(t, result.SubOutlines[1].Equals(c))
	})

	t.Run("no-op when not an arrow", func(t *testing.T) {
		outline := Outline{
			Type: UnionIteratorType,
			SubOutlines: []Outline{
				{Type: FixedIteratorType},
				{Type: FixedIteratorType},
			},
		}

		mutation := RotateArrowMutation(true)
		result := mutation(outline)

		require.True(t, result.Equals(outline))
	})

	t.Run("no-op when left child not arrow (rotate left)", func(t *testing.T) {
		a := Outline{Type: FixedIteratorType}
		b := Outline{Type: FixedIteratorType}

		outline := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{a, b},
		}

		mutation := RotateArrowMutation(true)
		result := mutation(outline)

		require.True(t, result.Equals(outline))
	})

	t.Run("no-op when right child not arrow (rotate right)", func(t *testing.T) {
		a := Outline{Type: FixedIteratorType}
		b := Outline{Type: FixedIteratorType}

		outline := Outline{
			Type:        ArrowIteratorType,
			SubOutlines: []Outline{a, b},
		}

		mutation := RotateArrowMutation(false)
		result := mutation(outline)

		require.True(t, result.Equals(outline))
	})

	t.Run("preserves Args fields", func(t *testing.T) {
		a := Outline{Type: FixedIteratorType}
		b := Outline{Type: FixedIteratorType}
		c := Outline{Type: FixedIteratorType}

		ab := Outline{
			Type:        ArrowIteratorType,
			Args:        &IteratorArgs{RelationName: "inner"},
			SubOutlines: []Outline{a, b},
		}

		abThenC := Outline{
			Type:        ArrowIteratorType,
			Args:        &IteratorArgs{RelationName: "outer"},
			SubOutlines: []Outline{ab, c},
		}

		mutation := RotateArrowMutation(true)
		result := mutation(abThenC)

		// Root should preserve outer Args
		require.NotNil(t, result.Args)
		require.Equal(t, "outer", result.Args.RelationName)

		// Inner arrow should also have outer Args (from parent)
		innerArrow := result.SubOutlines[1]
		require.NotNil(t, innerArrow.Args)
		require.Equal(t, "outer", innerArrow.Args.RelationName)
	})
}

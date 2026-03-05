package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReorderMutation(t *testing.T) {
	t.Parallel()

	t.Run("reorders children correctly", func(t *testing.T) {
		t.Parallel()

		child0 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc0#viewer@user:alice")}}}
		child1 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc1#viewer@user:bob")}}}
		child2 := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("document:doc2#viewer@user:charlie")}}}

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
		t.Parallel()

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
		t.Parallel()

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
		t.Parallel()

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
	t.Parallel()

	t.Run("rotate left: (A->B)->C to A->(B->C)", func(t *testing.T) {
		t.Parallel()

		// Build A, B, C
		a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:a#viewer@user:alice")}}}
		b := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:b#viewer@user:bob")}}}
		c := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:c#viewer@user:charlie")}}}

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
		t.Parallel()

		// Build A, B, C
		a := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:a#viewer@user:alice")}}}
		b := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:b#viewer@user:bob")}}}
		c := Outline{Type: FixedIteratorType, Args: &IteratorArgs{FixedPaths: []Path{MustPathFromString("doc:c#viewer@user:charlie")}}}

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
		t.Parallel()

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
		t.Parallel()

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
		t.Parallel()

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
		t.Parallel()

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

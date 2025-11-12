package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// newNonEmptyFixedIterator creates a FixedIterator with at least one path for testing
func newNonEmptyFixedIterator() *FixedIterator {
	return NewFixedIterator(Path{
		Resource: Object{ObjectType: "doc", ObjectID: "test"},
		Relation: "viewer",
		Subject: ObjectAndRelation{
			ObjectType: "user",
			ObjectID:   "alice",
			Relation:   "...",
		},
	})
}

func TestWrapOptimizer(t *testing.T) {
	t.Parallel()

	t.Run("matches correct type", func(t *testing.T) {
		t.Parallel()

		// Create a typed optimizer that only works on Union
		typedOptimizer := func(u *Union) (Iterator, bool, error) {
			if len(u.subIts) == 1 {
				return u.subIts[0], true, nil
			}
			return u, false, nil
		}

		// Wrap it and use in ApplyOptimizations
		wrapped := WrapOptimizer[*Union](typedOptimizer)

		// Test with a Union - should match and optimize
		fixed := newNonEmptyFixedIterator()
		union := NewUnion(fixed)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{wrapped})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("does not match wrong type", func(t *testing.T) {
		t.Parallel()

		// Create a typed optimizer that only works on Union
		typedOptimizer := func(u *Union) (Iterator, bool, error) {
			return u, true, nil // Would return true if called
		}

		// Wrap it and use in ApplyOptimizations
		wrapped := WrapOptimizer[*Union](typedOptimizer)

		// Test with an Intersection - should not match
		intersection := NewIntersection()
		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{wrapped})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})
}

func TestCollapseSingletonUnionAndIntersection(t *testing.T) {
	t.Parallel()

	t.Run("collapses singleton union", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		union := NewUnion(fixed)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("collapses singleton intersection", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		intersection := NewIntersection(fixed)

		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("does not collapse multi-element union", func(t *testing.T) {
		t.Parallel()

		union := NewUnion(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not collapse multi-element intersection", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersection(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})

	t.Run("does not collapse other iterator types", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		result, changed, err := ApplyOptimizations(fixed, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, fixed, result)
	})
}

func TestRemoveNullIterators(t *testing.T) {
	t.Parallel()

	t.Run("removes empty fixed from union", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		union := NewUnion(fixed, empty)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should remove the empty iterator, leaving a singleton union, which then gets collapsed
		require.Equal(t, fixed, result)
	})

	t.Run("removes multiple empty fixed from union", func(t *testing.T) {
		t.Parallel()

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		empty1 := NewEmptyFixedIterator()
		empty2 := NewEmptyFixedIterator()
		union := NewUnion(fixed1, empty1, fixed2, empty2)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should remove both empty iterators, leaving a union with 2 elements
		resultUnion, ok := result.(*Union)
		require.True(t, ok)
		require.Len(t, resultUnion.subIts, 2)
		require.Equal(t, fixed1, resultUnion.subIts[0])
		require.Equal(t, fixed2, resultUnion.subIts[1])
	})

	t.Run("replaces intersection with empty if it contains empty and fixed", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		intersection := NewIntersection(fixed, empty)

		result, changed, err := ApplyOptimizations(intersection, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should return an empty fixed iterator
		resultFixed, ok := result.(*FixedIterator)
		require.True(t, ok)
		require.Len(t, resultFixed.paths, 0)
	})

	t.Run("does not change union without empty iterators", func(t *testing.T) {
		t.Parallel()

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		union := NewUnion(fixed1, fixed2)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not change intersection without empty iterators", func(t *testing.T) {
		t.Parallel()

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		intersection := NewIntersection(fixed1, fixed2)

		result, changed, err := ApplyOptimizations(intersection, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})

	t.Run("returns empty when all union subiterators are empty", func(t *testing.T) {
		t.Parallel()

		empty1 := NewEmptyFixedIterator()
		empty2 := NewEmptyFixedIterator()
		empty3 := NewEmptyFixedIterator()
		union := NewUnion(empty1, empty2, empty3)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{RemoveNullIterators})
		require.NoError(t, err)
		require.True(t, changed)

		// Should return an empty fixed iterator
		resultFixed, ok := result.(*FixedIterator)
		require.True(t, ok)
		require.Len(t, resultFixed.paths, 0)
	})
}

func TestApplyOptimizations(t *testing.T) {
	t.Parallel()

	t.Run("applies optimization to nested iterators", func(t *testing.T) {
		t.Parallel()

		// Create a union with a nested singleton union
		fixed := newNonEmptyFixedIterator()
		innerUnion := NewUnion(fixed)
		outerUnion := NewUnion(innerUnion, newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(outerUnion, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// The outer union should still be a union (has 2 elements)
		// but the inner singleton union should be collapsed
		resultUnion, ok := result.(*Union)
		require.True(t, ok)
		require.Len(t, resultUnion.subIts, 2)
		require.Equal(t, fixed, resultUnion.subIts[0])
	})

	t.Run("chains multiple optimizations", func(t *testing.T) {
		t.Parallel()

		// Create a union with a singleton union inside
		fixed := newNonEmptyFixedIterator()
		innerUnion := NewUnion(fixed)
		outerUnion := NewUnion(innerUnion)

		result, changed, err := ApplyOptimizations(outerUnion, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)
		// After optimizations:
		// 1. Inner union collapsed: outerUnion has [fixed]
		// 2. Outer union collapsed: returns fixed
		require.Equal(t, fixed, result)
	})

	t.Run("returns unchanged when no optimizations apply", func(t *testing.T) {
		t.Parallel()

		union := NewUnion(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("applies multiple optimizations in sequence", func(t *testing.T) {
		t.Parallel()

		// Create two different optimizers
		unionOptimizer := func(it Iterator) (Iterator, bool, error) {
			if u, ok := it.(*Union); ok && len(u.subIts) == 1 {
				return u.subIts[0], true, nil
			}
			return it, false, nil
		}

		intersectionOptimizer := func(it Iterator) (Iterator, bool, error) {
			if i, ok := it.(*Intersection); ok && len(i.subIts) == 1 {
				return i.subIts[0], true, nil
			}
			return it, false, nil
		}

		// Test that both are applied
		fixed := newNonEmptyFixedIterator()
		intersection := NewIntersection(fixed)
		union := NewUnion(intersection)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{unionOptimizer, intersectionOptimizer})
		require.NoError(t, err)
		require.True(t, changed)
		// Both the union and intersection should be collapsed
		require.Equal(t, fixed, result)
	})

	t.Run("handles empty optimizer list", func(t *testing.T) {
		t.Parallel()

		union := NewUnion(newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("optimizer order independence - removes empty then collapses singleton", func(t *testing.T) {
		t.Parallel()

		// Create a union with an empty iterator and one non-empty iterator
		// After removing the empty, we have a singleton union that should be collapsed
		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		union := NewUnion(fixed, empty)

		// Test with RemoveNullIterators first, then CollapseSingletonUnionAndIntersection
		result1, changed1, err := ApplyOptimizations(union, []OptimizerFunc{
			RemoveNullIterators,
			CollapseSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		// Test with CollapseSingletonUnionAndIntersection first, then RemoveNullIterators
		result2, changed2, err := ApplyOptimizations(union, []OptimizerFunc{
			CollapseSingletonUnionAndIntersection,
			RemoveNullIterators,
		})
		require.NoError(t, err)
		require.True(t, changed2)

		// Both orders should produce the same final result: the fixed iterator
		require.Equal(t, fixed, result1)
		require.Equal(t, fixed, result2)
		require.Equal(t, result1, result2)
	})

	t.Run("optimizer order independence - nested structure", func(t *testing.T) {
		t.Parallel()

		// Create a more complex nested structure:
		// Union[Intersection[Fixed, Empty], Fixed]
		// Expected result: Fixed (the second one)
		// After RemoveNullIterators: Union[Empty, Fixed] -> Union[Fixed] -> Fixed
		// OR after processing intersection first: Union[Empty, Fixed] -> Union[Fixed] -> Fixed

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		innerIntersection := NewIntersection(fixed1, empty)
		outerUnion := NewUnion(innerIntersection, fixed2)

		result1, changed1, err := ApplyOptimizations(outerUnion, []OptimizerFunc{
			RemoveNullIterators,
			CollapseSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		result2, changed2, err := ApplyOptimizations(outerUnion, []OptimizerFunc{
			CollapseSingletonUnionAndIntersection,
			RemoveNullIterators,
		})
		require.NoError(t, err)
		require.True(t, changed2)

		// Both should result in the same structure
		// The intersection with empty should become empty, leaving Union[Empty, Fixed2]
		// Then removing empty gives Union[Fixed2], which collapses to Fixed2
		require.Equal(t, result1, result2)
		require.Equal(t, fixed2, result1)
		require.Equal(t, fixed2, result2)
	})

	t.Run("optimizer order independence - union with all empty", func(t *testing.T) {
		t.Parallel()

		// Union[Empty, Empty] should become Empty regardless of order
		empty1 := NewEmptyFixedIterator()
		empty2 := NewEmptyFixedIterator()
		union := NewUnion(empty1, empty2)

		result1, changed1, err := ApplyOptimizations(union, []OptimizerFunc{
			RemoveNullIterators,
			CollapseSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		result2, changed2, err := ApplyOptimizations(union, []OptimizerFunc{
			CollapseSingletonUnionAndIntersection,
			RemoveNullIterators,
		})
		require.NoError(t, err)
		require.True(t, changed2)

		// Both should result in an empty fixed iterator
		// After removing all empties, we get Union[], which then gets optimized
		// to an empty fixed iterator by CollapseSingletonUnionAndIntersection
		require.Equal(t, result1, result2)
		_, ok := result1.(*FixedIterator)
		require.True(t, ok, "result1 should be a FixedIterator (empty), got %T", result1)
	})
}

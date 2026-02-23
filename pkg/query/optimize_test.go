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
		typedOptimizer := func(u *UnionIterator) (Iterator, bool, error) {
			if len(u.subIts) == 1 {
				return u.subIts[0], true, nil
			}
			return u, false, nil
		}

		// Wrap it and use in ApplyOptimizations
		wrapped := WrapOptimizer[*UnionIterator](typedOptimizer)

		// Test with a Union - should match and optimize
		fixed := newNonEmptyFixedIterator()
		union := NewUnionIterator(fixed)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{wrapped})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("does not match wrong type", func(t *testing.T) {
		t.Parallel()

		// Create a typed optimizer that only works on Union
		typedOptimizer := func(u *UnionIterator) (Iterator, bool, error) {
			return u, true, nil // Would return true if called
		}

		// Wrap it and use in ApplyOptimizations
		wrapped := WrapOptimizer[*UnionIterator](typedOptimizer)

		// Test with an Intersection - should not match
		intersection := NewIntersectionIterator()
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
		union := NewUnionIterator(fixed)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("collapses singleton intersection", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		intersection := NewIntersectionIterator(fixed)

		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("does not collapse multi-element union", func(t *testing.T) {
		t.Parallel()

		union := NewUnionIterator(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{CollapseSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not collapse multi-element intersection", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersectionIterator(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

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
		union := NewUnionIterator(fixed, empty)

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
		union := NewUnionIterator(fixed1, empty1, fixed2, empty2)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should remove both empty iterators, leaving a union with 2 elements
		require.IsType(t, &UnionIterator{}, result)
		resultUnion := result.(*UnionIterator)
		require.Len(t, resultUnion.subIts, 2)
		require.Equal(t, fixed1, resultUnion.subIts[0])
		require.Equal(t, fixed2, resultUnion.subIts[1])
	})

	t.Run("replaces intersection with empty if it contains empty and fixed", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		intersection := NewIntersectionIterator(fixed, empty)

		result, changed, err := ApplyOptimizations(intersection, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should return an empty fixed iterator
		require.IsType(t, &FixedIterator{}, result)
		resultFixed := result.(*FixedIterator)
		require.Empty(t, resultFixed.paths)
	})

	t.Run("does not change union without empty iterators", func(t *testing.T) {
		t.Parallel()

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		union := NewUnionIterator(fixed1, fixed2)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not change intersection without empty iterators", func(t *testing.T) {
		t.Parallel()

		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		intersection := NewIntersectionIterator(fixed1, fixed2)

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
		union := NewUnionIterator(empty1, empty2, empty3)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{RemoveNullIterators})
		require.NoError(t, err)
		require.True(t, changed)

		// Should return an empty fixed iterator
		require.IsType(t, &FixedIterator{}, result)
		resultFixed := result.(*FixedIterator)
		require.Empty(t, resultFixed.paths)
	})
}

func TestApplyOptimizations(t *testing.T) {
	t.Parallel()

	t.Run("applies optimization to nested iterators", func(t *testing.T) {
		t.Parallel()

		// Create a union with a nested singleton union
		fixed := newNonEmptyFixedIterator()
		innerUnion := NewUnionIterator(fixed)
		outerUnion := NewUnionIterator(innerUnion, newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(outerUnion, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// The outer union should still be a union (has 2 elements)
		// but the inner singleton union should be collapsed
		require.IsType(t, &UnionIterator{}, result)
		resultUnion := result.(*UnionIterator)
		require.Len(t, resultUnion.subIts, 2)
		require.Equal(t, fixed, resultUnion.subIts[0])
	})

	t.Run("chains multiple optimizations", func(t *testing.T) {
		t.Parallel()

		// Create a union with a singleton union inside
		fixed := newNonEmptyFixedIterator()
		innerUnion := NewUnionIterator(fixed)
		outerUnion := NewUnionIterator(innerUnion)

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

		union := NewUnionIterator(newNonEmptyFixedIterator(), newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("applies multiple optimizations in sequence", func(t *testing.T) {
		t.Parallel()

		// Create two different optimizers
		unionOptimizer := func(it Iterator) (Iterator, bool, error) {
			if u, ok := it.(*UnionIterator); ok && len(u.subIts) == 1 {
				return u.subIts[0], true, nil
			}
			return it, false, nil
		}

		intersectionOptimizer := func(it Iterator) (Iterator, bool, error) {
			if i, ok := it.(*IntersectionIterator); ok && len(i.subIts) == 1 {
				return i.subIts[0], true, nil
			}
			return it, false, nil
		}

		// Test that both are applied
		fixed := newNonEmptyFixedIterator()
		intersection := NewIntersectionIterator(fixed)
		union := NewUnionIterator(intersection)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{unionOptimizer, intersectionOptimizer})
		require.NoError(t, err)
		require.True(t, changed)
		// Both the union and intersection should be collapsed
		require.Equal(t, fixed, result)
	})

	t.Run("handles empty optimizer list", func(t *testing.T) {
		t.Parallel()

		union := NewUnionIterator(newNonEmptyFixedIterator())

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
		union := NewUnionIterator(fixed, empty)

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
		innerIntersection := NewIntersectionIterator(fixed1, empty)
		outerUnion := NewUnionIterator(innerIntersection, fixed2)

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
		union := NewUnionIterator(empty1, empty2)

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
		require.IsType(t, &FixedIterator{}, result1, "result1 should be a FixedIterator (empty)")
		require.IsType(t, &FixedIterator{}, result2, "result2 should be a FixedIterator (empty)")
		// Check that both are empty
		fixed1 := result1.(*FixedIterator)
		fixed2 := result2.(*FixedIterator)
		require.Empty(t, fixed1.paths)
		require.Empty(t, fixed2.paths)
	})
}

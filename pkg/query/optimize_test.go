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
		union := NewUnion()
		fixed := newNonEmptyFixedIterator()
		union.addSubIterator(fixed)

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

func TestElideSingletonUnionAndIntersection(t *testing.T) {
	t.Parallel()

	t.Run("elides singleton union", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		fixed := newNonEmptyFixedIterator()
		union.addSubIterator(fixed)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{ElideSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("elides singleton intersection", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersection()
		fixed := newNonEmptyFixedIterator()
		intersection.addSubIterator(fixed)

		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{ElideSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("does not elide multi-element union", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		union.addSubIterator(newNonEmptyFixedIterator())
		union.addSubIterator(newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{ElideSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not elide multi-element intersection", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersection()
		intersection.addSubIterator(newNonEmptyFixedIterator())
		intersection.addSubIterator(newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(intersection, []OptimizerFunc{ElideSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})

	t.Run("does not elide other iterator types", func(t *testing.T) {
		t.Parallel()

		fixed := newNonEmptyFixedIterator()
		result, changed, err := ApplyOptimizations(fixed, []OptimizerFunc{ElideSingletonUnionAndIntersection})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, fixed, result)
	})
}

func TestRemoveNullIterators(t *testing.T) {
	t.Parallel()

	t.Run("removes empty fixed from union", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		union.addSubIterator(fixed)
		union.addSubIterator(empty)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// Should remove the empty iterator, leaving a singleton union, which then gets elided
		require.Equal(t, fixed, result)
	})

	t.Run("removes multiple empty fixed from union", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		empty1 := NewEmptyFixedIterator()
		empty2 := NewEmptyFixedIterator()
		union.addSubIterator(fixed1)
		union.addSubIterator(empty1)
		union.addSubIterator(fixed2)
		union.addSubIterator(empty2)

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

	t.Run("replaces intersection with empty if it contains empty fixed", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersection()
		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		intersection.addSubIterator(fixed)
		intersection.addSubIterator(empty)

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

		union := NewUnion()
		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		union.addSubIterator(fixed1)
		union.addSubIterator(fixed2)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("does not change intersection without empty iterators", func(t *testing.T) {
		t.Parallel()

		intersection := NewIntersection()
		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		intersection.addSubIterator(fixed1)
		intersection.addSubIterator(fixed2)

		result, changed, err := ApplyOptimizations(intersection, StaticOptimizations)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, intersection, result)
	})
}

func TestApplyOptimizations(t *testing.T) {
	t.Parallel()

	t.Run("applies optimization to iterator", func(t *testing.T) {
		t.Parallel()

		// Create a singleton union
		union := NewUnion()
		fixed := newNonEmptyFixedIterator()
		union.addSubIterator(fixed)

		result, changed, err := ApplyOptimizations(union, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, fixed, result)
	})

	t.Run("applies optimization to nested iterators", func(t *testing.T) {
		t.Parallel()

		// Create a union with a nested singleton union
		outerUnion := NewUnion()
		innerUnion := NewUnion()
		fixed := newNonEmptyFixedIterator()
		innerUnion.addSubIterator(fixed)
		outerUnion.addSubIterator(innerUnion)
		outerUnion.addSubIterator(newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(outerUnion, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)

		// The outer union should still be a union (has 2 elements)
		// but the inner singleton union should be elided
		resultUnion, ok := result.(*Union)
		require.True(t, ok)
		require.Len(t, resultUnion.subIts, 2)
		require.Equal(t, fixed, resultUnion.subIts[0])
	})

	t.Run("chains multiple optimizations", func(t *testing.T) {
		t.Parallel()

		// Create a union with a singleton union inside
		outerUnion := NewUnion()
		innerUnion := NewUnion()
		fixed := newNonEmptyFixedIterator()
		innerUnion.addSubIterator(fixed)
		outerUnion.addSubIterator(innerUnion)

		result, changed, err := ApplyOptimizations(outerUnion, StaticOptimizations)
		require.NoError(t, err)
		require.True(t, changed)
		// After optimizations:
		// 1. Inner union elided: outerUnion has [fixed]
		// 2. Outer union elided: returns fixed
		require.Equal(t, fixed, result)
	})

	t.Run("returns unchanged when no optimizations apply", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		union.addSubIterator(newNonEmptyFixedIterator())
		union.addSubIterator(newNonEmptyFixedIterator())

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
		union := NewUnion()
		intersection := NewIntersection()
		fixed := newNonEmptyFixedIterator()
		intersection.addSubIterator(fixed)
		union.addSubIterator(intersection)

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{unionOptimizer, intersectionOptimizer})
		require.NoError(t, err)
		require.True(t, changed)
		// Both the union and intersection should be elided
		require.Equal(t, fixed, result)
	})

	t.Run("handles empty optimizer list", func(t *testing.T) {
		t.Parallel()

		union := NewUnion()
		union.addSubIterator(newNonEmptyFixedIterator())

		result, changed, err := ApplyOptimizations(union, []OptimizerFunc{})
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, union, result)
	})

	t.Run("optimizer order independence - removes empty then elides singleton", func(t *testing.T) {
		t.Parallel()

		// Create a union with an empty iterator and one non-empty iterator
		// After removing the empty, we have a singleton union that should be elided
		union1 := NewUnion()
		fixed := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		union1.addSubIterator(fixed)
		union1.addSubIterator(empty)

		// Test with RemoveNullIterators first, then ElideSingletonUnionAndIntersection
		result1, changed1, err := ApplyOptimizations(union1, []OptimizerFunc{
			RemoveNullIterators,
			ElideSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		// Create the same structure again
		union2 := NewUnion()
		union2.addSubIterator(fixed)
		union2.addSubIterator(empty)

		// Test with ElideSingletonUnionAndIntersection first, then RemoveNullIterators
		result2, changed2, err := ApplyOptimizations(union2, []OptimizerFunc{
			ElideSingletonUnionAndIntersection,
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

		outerUnion1 := NewUnion()
		innerIntersection1 := NewIntersection()
		fixed1 := newNonEmptyFixedIterator()
		fixed2 := newNonEmptyFixedIterator()
		empty := NewEmptyFixedIterator()
		innerIntersection1.addSubIterator(fixed1)
		innerIntersection1.addSubIterator(empty)
		outerUnion1.addSubIterator(innerIntersection1)
		outerUnion1.addSubIterator(fixed2)

		result1, changed1, err := ApplyOptimizations(outerUnion1, []OptimizerFunc{
			RemoveNullIterators,
			ElideSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		// Create the same structure again
		outerUnion2 := NewUnion()
		innerIntersection2 := NewIntersection()
		innerIntersection2.addSubIterator(fixed1)
		innerIntersection2.addSubIterator(empty)
		outerUnion2.addSubIterator(innerIntersection2)
		outerUnion2.addSubIterator(fixed2)

		result2, changed2, err := ApplyOptimizations(outerUnion2, []OptimizerFunc{
			ElideSingletonUnionAndIntersection,
			RemoveNullIterators,
		})
		require.NoError(t, err)
		require.True(t, changed2)

		// Both should result in the same structure
		// The intersection with empty should become empty, leaving Union[Empty, Fixed2]
		// Then removing empty gives Union[Fixed2], which elides to Fixed2
		require.Equal(t, result1, result2)
		require.Equal(t, fixed2, result1)
		require.Equal(t, fixed2, result2)
	})

	t.Run("optimizer order independence - union with all empty", func(t *testing.T) {
		t.Parallel()

		// Union[Empty, Empty] should become Empty regardless of order
		union1 := NewUnion()
		empty1 := NewEmptyFixedIterator()
		empty2 := NewEmptyFixedIterator()
		union1.addSubIterator(empty1)
		union1.addSubIterator(empty2)

		result1, changed1, err := ApplyOptimizations(union1, []OptimizerFunc{
			RemoveNullIterators,
			ElideSingletonUnionAndIntersection,
		})
		require.NoError(t, err)
		require.True(t, changed1)

		// Create the same structure again
		union2 := NewUnion()
		union2.addSubIterator(empty1)
		union2.addSubIterator(empty2)

		result2, changed2, err := ApplyOptimizations(union2, []OptimizerFunc{
			ElideSingletonUnionAndIntersection,
			RemoveNullIterators,
		})
		require.NoError(t, err)
		require.True(t, changed2)

		// Both should result in an empty union (no subiterators)
		// After removing all empties, we get Union[] which has 0 elements
		require.Equal(t, result1, result2)
		resultUnion1, ok := result1.(*Union)
		require.True(t, ok)
		require.Len(t, resultUnion1.subIts, 0)
	})
}

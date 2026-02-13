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

package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWalkBasic(t *testing.T) {
	// Create a simple tree: union with two fixed iterators
	path1 := MustPathFromString("document:doc1#view@user:alice")
	fixedIter1 := NewFixedIterator(path1)
	fixedIter2 := NewEmptyFixedIterator()

	union := NewUnion()
	union.addSubIterator(fixedIter1)
	union.addSubIterator(fixedIter2)

	// Walk the tree and count the nodes
	nodeCount := 0
	result, err := Walk(union, func(it Iterator) (Iterator, error) {
		nodeCount++
		return it, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, nodeCount) // 2 fixed iterators + 1 union
}

func TestWalkNilRoot(t *testing.T) {
	// Walk with nil root should return nil
	result, err := Walk(nil, func(it Iterator) (Iterator, error) {
		t.Fatal("callback should not be called for nil root")
		return it, nil
	})

	require.NoError(t, err)
	require.Nil(t, result)
}

func TestWalkWithTransformation(t *testing.T) {
	// Create a tree with sentinels
	sentinel1 := NewRecursiveSentinel("folder", "view", false)
	sentinel2 := NewRecursiveSentinel("document", "edit", false)

	union := NewUnion()
	union.addSubIterator(sentinel1)
	union.addSubIterator(sentinel2)

	// Walk and replace all sentinels with empty fixed iterators
	result, err := Walk(union, func(it Iterator) (Iterator, error) {
		if _, isSentinel := it.(*RecursiveSentinel); isSentinel {
			return NewEmptyFixedIterator(), nil
		}
		return it, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify sentinels were replaced
	resultUnion := result.(*Union)
	require.Len(t, resultUnion.Subiterators(), 2)

	for _, sub := range resultUnion.Subiterators() {
		_, isFixed := sub.(*FixedIterator)
		require.True(t, isFixed, "All children should be FixedIterator")
	}
}

func TestWalkCallbackError(t *testing.T) {
	// Create a simple tree
	fixedIter := NewEmptyFixedIterator()
	union := NewUnion()
	union.addSubIterator(fixedIter)

	// Walk with a callback that returns an error
	expectedErr := fmt.Errorf("callback error")
	result, err := Walk(union, func(it Iterator) (Iterator, error) {
		if _, isUnion := it.(*Union); isUnion {
			return nil, expectedErr
		}
		return it, nil
	})

	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, result)
}

func TestWalkRecursiveCallbackError(t *testing.T) {
	// Create a nested tree to test error propagation from recursive Walk calls
	fixedIter1 := NewEmptyFixedIterator()
	fixedIter2 := NewEmptyFixedIterator()

	innerUnion := NewUnion()
	innerUnion.addSubIterator(fixedIter1)

	outerUnion := NewUnion()
	outerUnion.addSubIterator(innerUnion)
	outerUnion.addSubIterator(fixedIter2)

	// Walk with a callback that errors on the inner fixed iterator
	expectedErr := fmt.Errorf("recursive callback error")
	callCount := 0
	result, err := Walk(outerUnion, func(it Iterator) (Iterator, error) {
		callCount++
		// Error on the first fixed iterator we encounter
		if _, isFixed := it.(*FixedIterator); isFixed && callCount == 1 {
			return nil, expectedErr
		}
		return it, nil
	})

	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	require.Nil(t, result)
	require.Equal(t, 1, callCount) // Should stop after first error
}

func TestWalkDeepTree(t *testing.T) {
	// Create a deeper tree: Arrow(Union(Fixed, Fixed), Fixed)
	path1 := MustPathFromString("document:doc1#view@user:alice")
	path2 := MustPathFromString("document:doc2#view@user:bob")

	fixedIter1 := NewFixedIterator(path1)
	fixedIter2 := NewFixedIterator(path2)
	fixedIter3 := NewEmptyFixedIterator()

	union := NewUnion()
	union.addSubIterator(fixedIter1)
	union.addSubIterator(fixedIter2)

	arrow := NewArrow(union, fixedIter3)

	// Walk and collect all iterator types
	var iteratorTypes []string
	result, err := Walk(arrow, func(it Iterator) (Iterator, error) {
		iteratorTypes = append(iteratorTypes, it.Explain().Name)
		return it, nil
	})

	require.NoError(t, err)
	require.NotNil(t, result)

	// Should visit in bottom-up order: fixed1, fixed2, union, fixed3, arrow
	require.Len(t, iteratorTypes, 5)
	require.Equal(t, "Fixed", iteratorTypes[0])
	require.Equal(t, "Fixed", iteratorTypes[1])
	require.Equal(t, "Union", iteratorTypes[2])
	require.Equal(t, "Fixed", iteratorTypes[3])
	require.Equal(t, "Arrow", iteratorTypes[4])
}

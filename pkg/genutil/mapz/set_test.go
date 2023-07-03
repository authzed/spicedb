package mapz

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetOperations(t *testing.T) {
	// Create a set and ensure it is empty.
	set := NewSet[string]()
	require.True(t, set.IsEmpty())

	// Add some items to the set.
	require.True(t, set.Add("hi"))
	require.True(t, set.Add("hello"))
	require.False(t, set.Add("hello"))
	require.True(t, set.Add("heyo"))

	// Ensure the items are in the set.
	require.False(t, set.IsEmpty())
	require.True(t, set.Has("hi"))
	require.True(t, set.Has("hello"))
	require.True(t, set.Has("heyo"))

	require.False(t, set.Has("hola"))
	require.False(t, set.Has("hiya"))

	slice := set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, slice, []string{"hello", "heyo", "hi"})

	// Remove some items.
	require.True(t, set.Remove("hi"))
	require.False(t, set.Remove("hi"))

	require.False(t, set.Has("hi"))
	require.True(t, set.Has("hello"))
	require.True(t, set.Has("heyo"))

	require.False(t, set.Has("hola"))
	require.False(t, set.Has("hiya"))

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, slice, []string{"hello", "heyo"})

	// Extend the set with a slice of values
	set.Extend([]string{"1", "2", "3"})

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, slice, []string{"1", "2", "3", "hello", "heyo"})

	// Create another set and remove its items.
	otherSet := NewSet[string]()
	otherSet.Extend([]string{"1", "2", "3"})

	set.RemoveAll(otherSet)

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, slice, []string{"hello", "heyo"})

	// Create a third set and perform intersection difference.
	thirdSet := NewSet[string]()
	thirdSet.Extend([]string{"hello", "hi"})

	set.IntersectionDifference(thirdSet)

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, slice, []string{"hello"})
}

func TestSetIntersect(t *testing.T) {
	// Create a set and ensure it is empty.
	set := NewSet[string]()
	require.True(t, set.IsEmpty())

	// Add some items to the set.
	require.True(t, set.Add("1"))
	require.True(t, set.Add("2"))
	require.True(t, set.Add("3"))
	require.True(t, set.Add("4"))

	// Subtract some items.
	updated := set.Intersect(NewSet[string]("1", "2", "3", "5"))
	updatedSlice := updated.AsSlice()
	sort.Strings(updatedSlice)
	require.Equal(t, []string{"1", "2", "3"}, updatedSlice)

	slice := set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"1", "2", "3", "4"}, slice)
}

func TestSetSubtract(t *testing.T) {
	// Create a set and ensure it is empty.
	set := NewSet[string]()
	require.True(t, set.IsEmpty())

	// Add some items to the set.
	require.True(t, set.Add("1"))
	require.True(t, set.Add("2"))
	require.True(t, set.Add("3"))
	require.True(t, set.Add("4"))

	// Subtract some items.
	updated := set.Subtract(NewSet[string]("1", "2", "3", "5"))
	require.Equal(t, []string{"4"}, updated.AsSlice())

	slice := set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"1", "2", "3", "4"}, slice)
}

func TestEqual(t *testing.T) {
	require.True(t, NewSet[string]().Equal(NewSet[string]()))
	require.True(t, NewSet[string]("2").Equal(NewSet[string]("2")))
	require.False(t, NewSet[string]("1", "2").Equal(NewSet[string]("1", "3")))
}

func TestSetIntersectionDifference(t *testing.T) {
	tcs := []struct {
		first    []int
		second   []int
		expected []int
	}{
		{
			[]int{1, 2, 3, 4, 5},
			[]int{1, 2, 3, 4, 5},
			[]int{1, 2, 3, 4, 5},
		},
		{
			[]int{1, 3, 5, 7, 9},
			[]int{2, 4, 6, 8, 10},
			nil,
		},
		{
			[]int{1, 2, 3, 4, 5},
			[]int{6, 5, 4, 3},
			[]int{3, 4, 5},
		},
	}

	for index, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%d", index), func(t *testing.T) {
			firstSet := NewSet[int]()
			firstSet.Extend(tc.first)

			secondSet := NewSet[int]()
			secondSet.Extend(tc.second)

			firstSet.IntersectionDifference(secondSet)
			slice := firstSet.AsSlice()
			sort.Ints(slice)
			require.Equal(t, tc.expected, slice)
		})
	}
}

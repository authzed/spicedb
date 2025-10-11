package mapz

import (
	"fmt"
	"slices"
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
	require.Equal(t, []string{"hello", "heyo", "hi"}, slice)

	// Delete some items.
	set.Delete("hi")
	set.Delete("hi")

	require.False(t, set.Has("hi"))
	require.True(t, set.Has("hello"))
	require.True(t, set.Has("heyo"))

	require.False(t, set.Has("hola"))
	require.False(t, set.Has("hiya"))

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"hello", "heyo"}, slice)

	// Extend the set with a slice of values
	set.Extend([]string{"1", "2", "3"})

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"1", "2", "3", "hello", "heyo"}, slice)

	// Create another set and remove its items.
	otherSet := NewSet[string]()
	otherSet.Extend([]string{"1", "2", "3"})

	set.RemoveAll(otherSet)

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"hello", "heyo"}, slice)

	// Create a third set and perform intersection difference.
	thirdSet := NewSet[string]()
	thirdSet.Extend([]string{"hello", "hi"})

	set.IntersectionDifference(thirdSet)

	slice = set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"hello"}, slice)
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
	updated := set.Intersect(NewSet("1", "2", "3", "5"))
	updatedSlice := updated.AsSlice()
	sort.Strings(updatedSlice)
	require.Equal(t, []string{"1", "2", "3"}, updatedSlice)

	slice := set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"1", "2", "3", "4"}, slice)

	// Perform in reverse.
	updated = NewSet("1", "2", "3", "5").Intersect(set)
	updatedSlice = updated.AsSlice()
	sort.Strings(updatedSlice)
	require.Equal(t, []string{"1", "2", "3"}, updatedSlice)
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
	updated := set.Subtract(NewSet("1", "2", "3", "5"))
	require.Equal(t, []string{"4"}, updated.AsSlice())

	slice := set.AsSlice()
	sort.Strings(slice)
	require.Equal(t, []string{"1", "2", "3", "4"}, slice)
}

func TestEqual(t *testing.T) {
	require.True(t, NewSet[string]().Equal(NewSet[string]()))
	require.True(t, NewSet("2").Equal(NewSet("2")))
	require.False(t, NewSet("1", "2").Equal(NewSet("1", "3")))
}

func TestUnion(t *testing.T) {
	u1 := NewSet("1", "2").Union(NewSet("2", "3")).AsSlice()
	sort.Strings(u1)

	u2 := NewSet("2", "3").Union(NewSet("1", "2")).AsSlice()
	sort.Strings(u2)

	require.Equal(t, []string{"1", "2", "3"}, u1)
	require.Equal(t, []string{"1", "2", "3"}, u2)
}

func TestMerge(t *testing.T) {
	u1 := NewSet("1", "2")
	u2 := NewSet("2", "3")

	u1.Merge(u2)

	slice := u1.AsSlice()
	sort.Strings(slice)

	require.Equal(t, []string{"1", "2", "3"}, slice)

	// Try the reverse.
	u1 = NewSet("1", "2")
	u2 = NewSet("2", "3")

	u2.Merge(u1)

	slice = u2.AsSlice()
	sort.Strings(slice)

	require.Equal(t, []string{"1", "2", "3"}, slice)
}

func TestSetDifference(t *testing.T) {
	tests := []struct {
		name     string
		original []int
		others   [][]int
		expected []int
	}{
		{
			name:     "empty set difference with empty set",
			original: []int{},
			others:   [][]int{{}},
			expected: []int{},
		},
		{
			name:     "empty set difference with non-empty set",
			original: []int{},
			others:   [][]int{{1, 2, 3}},
			expected: []int{},
		},
		{
			name:     "non-empty set difference with empty set",
			original: []int{1, 2, 3},
			others:   [][]int{{}},
			expected: []int{1, 2, 3},
		},
		{
			name:     "identical sets difference",
			original: []int{1, 2, 3},
			others:   [][]int{{1, 2, 3}},
			expected: []int{},
		},
		{
			name:     "completely disjoint sets",
			original: []int{1, 2, 3},
			others:   [][]int{{4, 5, 6}},
			expected: []int{1, 2, 3},
		},
		{
			name:     "partial overlap - some elements removed",
			original: []int{1, 2, 3, 4, 5},
			others:   [][]int{{2, 4, 6}},
			expected: []int{1, 3, 5},
		},
		{
			name:     "subset removal",
			original: []int{1, 2, 3, 4, 5},
			others:   [][]int{{2, 3}},
			expected: []int{1, 4, 5},
		},
		{
			name:     "superset removal - original is subset",
			original: []int{2, 3},
			others:   [][]int{{1, 2, 3, 4, 5}},
			expected: []int{},
		},
		{
			name:     "single element sets",
			original: []int{1},
			others:   [][]int{{1}},
			expected: []int{},
		},
		{
			name:     "single element different",
			original: []int{1},
			others:   [][]int{{2}},
			expected: []int{1},
		},
		{
			name:     "multiple other sets - all disjoint",
			original: []int{1, 2, 3, 4, 5},
			others:   [][]int{{6, 7}, {8, 9}, {10, 11}},
			expected: []int{1, 2, 3, 4, 5},
		},
		{
			name:     "multiple other sets - with overlaps",
			original: []int{1, 2, 3, 4, 5, 6, 7, 8},
			others:   [][]int{{1, 2}, {3, 4}, {5, 6}},
			expected: []int{7, 8},
		},
		{
			name:     "multiple other sets - complete removal",
			original: []int{1, 2, 3},
			others:   [][]int{{1}, {2}, {3}},
			expected: []int{},
		},
		{
			name:     "multiple other sets - overlapping removals",
			original: []int{1, 2, 3, 4, 5},
			others:   [][]int{{1, 2, 6}, {2, 3, 7}, {3, 4, 8}},
			expected: []int{5},
		},
		{
			name:     "no other sets provided",
			original: []int{1, 2, 3},
			others:   [][]int{},
			expected: []int{1, 2, 3},
		},
		{
			name:     "duplicate elements in original (shouldn't happen in real set, but testing robustness)",
			original: []int{1, 2, 3},
			others:   [][]int{{1}},
			expected: []int{2, 3},
		},
		{
			name:     "large set difference",
			original: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			others:   [][]int{{2, 4, 6, 8, 10, 12, 14}},
			expected: []int{1, 3, 5, 7, 9, 11, 13, 15},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := NewSet(tt.original...)
			others := make([]*Set[int], len(tt.others))
			for i, otherSlice := range tt.others {
				others[i] = NewSet(otherSlice...)
			}

			result := original.Difference(others...)

			resultSlice := result.AsSlice()
			if resultSlice == nil {
				resultSlice = []int{}
			}
			slices.Sort(resultSlice)

			expectedSorted := make([]int, len(tt.expected))
			copy(expectedSorted, tt.expected)
			slices.Sort(expectedSorted)

			require.Equal(t, expectedSorted, resultSlice)
		})
	}
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

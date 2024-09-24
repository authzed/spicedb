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

	// Perform in reverse.
	updated = NewSet[string]("1", "2", "3", "5").Intersect(set)
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

func TestUnion(t *testing.T) {
	u1 := NewSet[string]("1", "2").Union(NewSet[string]("2", "3")).AsSlice()
	sort.Strings(u1)

	u2 := NewSet[string]("2", "3").Union(NewSet[string]("1", "2")).AsSlice()
	sort.Strings(u2)

	require.Equal(t, []string{"1", "2", "3"}, u1)
	require.Equal(t, []string{"1", "2", "3"}, u2)
}

func TestMerge(t *testing.T) {
	u1 := NewSet[string]("1", "2")
	u2 := NewSet[string]("2", "3")

	u1.Merge(u2)

	slice := u1.AsSlice()
	sort.Strings(slice)

	require.Equal(t, []string{"1", "2", "3"}, slice)

	// Try the reverse.
	u1 = NewSet[string]("1", "2")
	u2 = NewSet[string]("2", "3")

	u2.Merge(u1)

	slice = u2.AsSlice()
	sort.Strings(slice)

	require.Equal(t, []string{"1", "2", "3"}, slice)
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

func BenchmarkAdd(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
}

func BenchmarkInsert(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Insert(i)
	}
}

func BenchmarkCopy(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Copy()
	}
}

func BenchmarkHas(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Has(i)
	}
}

func BenchmarkDelete(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Delete(i)
	}
}

func BenchmarkIntersect(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Intersect(other)
	}
}

func BenchmarkSubtract(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Subtract(other)
	}
}

func BenchmarkAsSlice(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.AsSlice()
	}
}

func BenchmarkEqual(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Equal(other)
	}
}

func BenchmarkExtendFromSlice(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Extend(other.AsSlice())
	}
}

func BenchmarkMerge(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Merge(other)
	}
}

func BenchmarkUnion(b *testing.B) {
	set := NewSet[int]()
	for i := 0; i < b.N; i++ {
		set.Add(i)
	}
	other := NewSet[int]()
	for i := 0; i < b.N; i++ {
		other.Add(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set.Union(other)
	}
}

package mapz

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultimapOperations(t *testing.T) {
	mm := NewMultiMap[string, int]()
	require.Equal(t, 0, mm.Len())
	require.True(t, mm.IsEmpty())

	// Add some values to the map.
	mm.Add("odd", 1)
	mm.Add("odd", 3)
	mm.Add("odd", 5)

	require.Equal(t, 1, mm.Len())
	require.False(t, mm.IsEmpty())

	require.True(t, mm.Has("odd"))
	found, ok := mm.Get("odd")
	require.True(t, ok)
	require.Equal(t, []int{1, 3, 5}, found)

	require.False(t, mm.Has("even"))
	found, ok = mm.Get("even")
	require.False(t, ok)
	require.Equal(t, []int{}, found)

	require.Equal(t, []string{"odd"}, mm.Keys())

	// Add some more values.
	mm.Add("even", 2)
	mm.Add("even", 4)

	require.Equal(t, 2, mm.Len())
	require.False(t, mm.IsEmpty())

	require.True(t, mm.Has("even"))
	found, ok = mm.Get("even")
	require.True(t, ok)
	require.Equal(t, []int{2, 4}, found)

	foundKeys := mm.Keys()
	sort.Strings(foundKeys)

	require.Equal(t, []string{"even", "odd"}, foundKeys)

	// Remove a key.
	mm.RemoveKey("odd")

	require.Equal(t, 1, mm.Len())
	require.False(t, mm.IsEmpty())

	foundKeys = mm.Keys()
	sort.Strings(foundKeys)
	require.Equal(t, []string{"even"}, foundKeys)

	require.False(t, mm.Has("odd"))
	found, ok = mm.Get("odd")
	require.False(t, ok)
	require.Equal(t, []int{}, found)

	// Remove an unknown key.
	mm.RemoveKey("unknown")
	require.Equal(t, 1, mm.Len())
	require.False(t, mm.IsEmpty())

	// Remove the last key.
	mm.RemoveKey("even")
	require.Equal(t, 0, mm.Len())
	require.True(t, mm.IsEmpty())
}

func TestMultimapReadOnly(t *testing.T) {
	mm := NewMultiMap[string, int]()
	require.Equal(t, 0, mm.Len())
	require.True(t, mm.IsEmpty())

	// Add some values to the map.
	mm.Add("odd", 1)
	mm.Add("odd", 3)
	mm.Add("odd", 5)

	// Make a read-only copy.
	ro := mm.AsReadOnly()

	// Add some values to the original map.
	mm.Add("even", 2)
	mm.Add("zero", 0)

	// Make sure the read-only map was not modified.
	require.Equal(t, 3, mm.Len())
	require.Equal(t, 1, ro.Len())

	require.True(t, mm.Has("even"))
	require.False(t, ro.Has("even"))
}

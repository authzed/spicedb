package mapz

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicCountingMap(t *testing.T) {
	cmap := NewCountingMultiMap[string, string]()

	require.False(t, cmap.Add("foo", "1"))
	require.False(t, cmap.Add("foo", "2"))
	require.False(t, cmap.Add("bar", "1"))

	require.True(t, cmap.Add("foo", "1"))

	cmap.Remove("foo", "1")

	require.False(t, cmap.Add("foo", "1"))
	require.True(t, cmap.Add("foo", "2"))
}

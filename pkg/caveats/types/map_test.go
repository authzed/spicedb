package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapSubtree(t *testing.T) {
	tcs := []struct {
		map1    map[string]any
		map2    map[string]any
		subtree bool
	}{
		{
			map[string]any{"a": 1},
			map[string]any{"a": 1},
			true,
		},
		{
			map[string]any{"a": 1, "b": 2},
			map[string]any{"a": 1},
			false,
		},
		{
			map[string]any{"a": 1},
			map[string]any{"a": 1, "b": 1},
			true,
		},
		{
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			map[string]any{"a": 1},
			false,
		},
		{
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			true,
		},
		{
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			map[string]any{"a": 1, "b": map[string]any{"a": 1, "b": 1}},
			true,
		},
		{
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			map[string]any{"a": 1, "b": map[string]any{"a": "1", "b": 1}},
			false,
		},
		{
			map[string]any{"a": 1, "b": map[string]any{"a": 1}},
			map[string]any{"a": 1, "b": map[string]any{"a": 1, "b": map[string]any{}}},
			true,
		},
	}
	for _, tt := range tcs {
		tt := tt
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.subtree, subtree(tt.map1, tt.map2))
		})
	}
}

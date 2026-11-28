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
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.subtree, subtree(tt.map1, tt.map2))
		})
	}
}

// TestMapSubtreeWithListValues covers caveat context values that are slices.
// Comparing slices with the `!=` operator panics ("comparing uncomparable
// type"), so these cases guard that subtree handles them by value instead.
func TestMapSubtreeWithListValues(t *testing.T) {
	tcs := []struct {
		name    string
		map1    map[string]any
		map2    map[string]any
		subtree bool
	}{
		{
			"equal list values",
			map[string]any{"ips": []any{"10.0.0.1", "10.0.0.2"}},
			map[string]any{"ips": []any{"10.0.0.1", "10.0.0.2"}},
			true,
		},
		{
			"different list values",
			map[string]any{"ips": []any{"10.0.0.1"}},
			map[string]any{"ips": []any{"10.0.0.2"}},
			false,
		},
		{
			"different list lengths",
			map[string]any{"ips": []any{"10.0.0.1"}},
			map[string]any{"ips": []any{"10.0.0.1", "10.0.0.2"}},
			false,
		},
		{
			"list inside a nested map",
			map[string]any{"net": map[string]any{"ips": []any{"10.0.0.1"}}},
			map[string]any{"net": map[string]any{"ips": []any{"10.0.0.1"}, "extra": 1}},
			true,
		},
		{
			"list missing in map2",
			map[string]any{"ips": []any{"10.0.0.1"}},
			map[string]any{"other": 1},
			false,
		},
	}
	for _, tt := range tcs {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.subtree, subtree(tt.map1, tt.map2))
		})
	}
}

package crdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOverlapKeyAddition(t *testing.T) {
	cases := []struct {
		name       string
		keyer      overlapKeyer
		namespaces []string
		expected   keySet
	}{
		{
			name:       "none",
			keyer:      noOverlapKeyer,
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected:   map[string]struct{}{},
		},
		{
			name:       "static",
			keyer:      appendStaticKey("test"),
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected:   map[string]struct{}{"test": {}},
		},
		{
			name:       "prefix with default",
			keyer:      prefixKeyer,
			namespaces: []string{"a", "a/b", "c", "a/b/c"},
			expected: map[string]struct{}{
				defaultOverlapKey: {},
				"a":               {},
			},
		},
		{
			name:       "prefix no default",
			keyer:      prefixKeyer,
			namespaces: []string{"a/b", "a/b/c"},
			expected: map[string]struct{}{
				"a": {},
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			set := newKeySet()
			for _, n := range tt.namespaces {
				tt.keyer.addKey(set, n)
			}
			require.EqualValues(t, tt.expected, set)
		})
	}
}

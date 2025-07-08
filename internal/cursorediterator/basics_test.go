package cursorediterator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmpty(t *testing.T) {
	testCases := []struct {
		name   string
		cursor Cursor
	}{
		{
			name:   "empty cursor",
			cursor: Cursor{},
		},
		{
			name:   "non-empty cursor",
			cursor: Cursor{"value1", "value2"},
		},
		{
			name:   "nil cursor",
			cursor: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			emptySeq := Empty[string](t.Context(), tc.cursor)

			items, errs := collectAll(emptySeq)

			require.Len(t, items, 0)
			require.Len(t, errs, 0)
		})
	}
}

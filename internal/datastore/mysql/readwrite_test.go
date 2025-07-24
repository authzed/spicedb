package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStructpbWrapperScan(t *testing.T) {
	tests := []struct {
		name     string
		initial  structpbWrapper
		val      any
		expected structpbWrapper
		wantErr  bool
	}{
		{
			name:     "nil value",
			initial:  structpbWrapper{"key": "value"},
			val:      nil,
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "valid JSON",
			initial:  structpbWrapper{"old": "data"},
			val:      []byte(`{"new":"data","count":42}`),
			expected: structpbWrapper{"new": "data", "count": float64(42)},
			wantErr:  false,
		},
		{
			name:     "invalid type",
			initial:  structpbWrapper{"key": "value"},
			val:      "not a byte array",
			expected: structpbWrapper{"key": "value"}, // Should remain unchanged
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			cc := tt.initial
			err := cc.Scan(tt.val)

			if tt.wantErr {
				require.Error(err)
			} else {
				require.NoError(err)
			}
			require.Equal(tt.expected, cc)
		})
	}
}

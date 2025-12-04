package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMustStruct(t *testing.T) {
	t.Parallel()

	cc := TransactionMetadata{"key": "value"}

	ss := cc.MustStruct()
	require.NotNil(t, ss)
}

func TestScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		initial  TransactionMetadata
		val      any
		expected TransactionMetadata
		wantErr  bool
	}{
		{
			name:     "nil value",
			initial:  TransactionMetadata{"key": "value"},
			val:      nil,
			expected: nil,
			wantErr:  false,
		},
		{
			name:     "valid JSON",
			initial:  TransactionMetadata{"old": "data"},
			val:      []byte(`{"new":"data","count":42}`),
			expected: TransactionMetadata{"new": "data", "count": float64(42)},
			wantErr:  false,
		},
		{
			name:     "invalid type",
			initial:  TransactionMetadata{"key": "value"},
			val:      "not a byte array",
			expected: TransactionMetadata{"key": "value"}, // Should remain unchanged
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := tt.initial
			err := cc.Scan(tt.val)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, cc)
		})
	}
}

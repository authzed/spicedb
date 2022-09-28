package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePercent(t *testing.T) {
	table := []struct {
		percent     string
		freeMem     uint64
		expected    uint64
		expectedErr error
	}{
		{"100%", 1000, 1000, nil},
		{"0%", 1000, 0, nil},
		{"50%", 1000, 500, nil},
		{"100%", 0, 0, nil},
		{"1000%", 1000, 0, errOverHundredPercent},
	}

	for _, tt := range table {
		v, err := parsePercent(tt.percent, tt.freeMem)
		if tt.expectedErr == nil {
			require.Nil(t, err)
		} else {
			require.Equal(t, tt.expectedErr, err)
		}
		require.Equal(t, tt.expected, v)
	}
}

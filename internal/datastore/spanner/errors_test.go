package spanner

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsMissingTableError(t *testing.T) {
	tcs := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"table not found", status.Error(codes.NotFound, "Table not found: caveat"), true},
		{"lowercase table not found", status.Error(codes.NotFound, "table not found: caveat"), true},
		{"wrapped table not found", fmt.Errorf("outer: %w", status.Error(codes.NotFound, "Table not found: namespace_config")), true},
		{"row not found", status.Error(codes.NotFound, "Row not found"), false},
		{"other code", status.Error(codes.Internal, "Table not found: caveat"), false},
		{"non-grpc error", errors.New("Table not found: caveat"), false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsMissingTableError(tc.err))
		})
	}
}

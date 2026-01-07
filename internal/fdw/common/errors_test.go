package common

import (
	"errors"
	"testing"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewUnsupportedError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		baseErr        error
		expectedPrefix string
		expectedCode   codes.Code
	}{
		{
			name:           "basic error",
			baseErr:        errors.New("feature not available"),
			expectedPrefix: "unsupported feature or query type:",
			expectedCode:   codes.FeatureNotSupported,
		},
		{
			name:           "wrapped error",
			baseErr:        errors.New("SELECT DISTINCT not supported"),
			expectedPrefix: "unsupported feature or query type:",
			expectedCode:   codes.FeatureNotSupported,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := NewUnsupportedError(tc.baseErr)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.expectedPrefix)
			require.ErrorIs(t, err, tc.baseErr)

			// Verify error is properly wrapped with psql codes
			code := psqlerr.GetCode(err)
			require.Equal(t, tc.expectedCode, code)
		})
	}
}

func TestNewSemanticsError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		baseErr        error
		expectedPrefix string
		expectedCode   codes.Code
	}{
		{
			name:           "basic semantic error",
			baseErr:        errors.New("invalid schema definition"),
			expectedPrefix: "semantic error:",
			expectedCode:   codes.InvalidParameterValue,
		},
		{
			name:           "type mismatch error",
			baseErr:        errors.New("column type mismatch"),
			expectedPrefix: "semantic error:",
			expectedCode:   codes.InvalidParameterValue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := NewSemanticsError(tc.baseErr)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.expectedPrefix)
			require.ErrorIs(t, err, tc.baseErr)

			// Verify error is properly wrapped with psql codes
			code := psqlerr.GetCode(err)
			require.Equal(t, tc.expectedCode, code)
		})
	}
}

func TestNewQueryError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		baseErr        error
		expectedPrefix string
		expectedCode   codes.Code
	}{
		{
			name:           "basic query error",
			baseErr:        errors.New("failed to execute query"),
			expectedPrefix: "query error:",
			expectedCode:   codes.InvalidParameterValue,
		},
		{
			name:           "connection error",
			baseErr:        errors.New("connection lost"),
			expectedPrefix: "query error:",
			expectedCode:   codes.InvalidParameterValue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := NewQueryError(tc.baseErr)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.expectedPrefix)
			require.ErrorIs(t, err, tc.baseErr)

			// Verify error is properly wrapped with psql codes
			code := psqlerr.GetCode(err)
			require.Equal(t, tc.expectedCode, code)
		})
	}
}

func TestConvertError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		inputErr    error
		expectWraps bool
	}{
		{
			name:        "grpc InvalidArgument",
			inputErr:    status.Error(grpccodes.InvalidArgument, "invalid argument"),
			expectWraps: true,
		},
		{
			name:        "grpc FailedPrecondition",
			inputErr:    status.Error(grpccodes.FailedPrecondition, "precondition failed"),
			expectWraps: true,
		},
		{
			name:        "grpc NotFound - pass through",
			inputErr:    status.Error(grpccodes.NotFound, "not found"),
			expectWraps: false,
		},
		{
			name:        "grpc Internal - pass through",
			inputErr:    status.Error(grpccodes.Internal, "internal error"),
			expectWraps: false,
		},
		{
			name:        "non-grpc error - pass through",
			inputErr:    errors.New("regular error"),
			expectWraps: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ConvertError(tc.inputErr)
			require.Error(t, err)

			if tc.expectWraps {
				// Should be wrapped as a query error
				require.ErrorContains(t, err, "query error:")
			} else {
				// Should pass through unchanged
				require.Equal(t, tc.inputErr, err)
			}
		})
	}
}

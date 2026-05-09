package grpcutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequireStatus asserts that an error is a gRPC error and returns the expected
// status code.
func RequireStatus(t *testing.T, expected codes.Code, err error) {
	require.Error(t, err)
	errStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, expected, errStatus.Code())
}

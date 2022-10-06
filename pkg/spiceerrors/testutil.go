package spiceerrors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// RequireReason asserts that an error is a gRPC error and returns the expected
// reason in the ErrorInfo.
// TODO(jschorr): Move into grpcutil.
func RequireReason(t testing.TB, reason v1.ErrorReason, err error, expectedMetadataKeys ...string) {
	require.Error(t, err)
	withStatus, ok := status.FromError(err)
	require.True(t, ok)
	require.True(t, len(withStatus.Details()) > 0)

	info := withStatus.Details()[0].(*errdetails.ErrorInfo)
	require.Equal(t, v1.ErrorReason_name[int32(reason)], info.GetReason())

	for _, expectedKey := range expectedMetadataKeys {
		require.Contains(t, info.Metadata, expectedKey)
	}
}

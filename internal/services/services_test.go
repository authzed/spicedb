package services

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func requireGRPCStatus(expectedCode codes.Code, err error, require *require.Assertions) {
	require.Error(err)
	asStatus, ok := status.FromError(err)
	require.True(ok)
	require.Equal(expectedCode, asStatus.Code())
}

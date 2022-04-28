package auth

import (
	"context"
	"fmt"
	"testing"

	"github.com/authzed/grpcutil"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestMultiplePresharedKeys(t *testing.T) {
	f := RequirePresharedKey([]string{"one", "two"})
	_, err := f(contextWithBearerToken("one"))
	require.NoError(t, err)
	_, err = f(contextWithBearerToken("two"))
	require.NoError(t, err)
}

func TestWrongKey(t *testing.T) {
	f := RequirePresharedKey([]string{"one", "two"})
	_, err := f(contextWithBearerToken("three"))
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.PermissionDenied, err)
}

func TestMissingKey(t *testing.T) {
	f := RequirePresharedKey([]string{"one", "two"})
	_, err := f(contextWithBearerToken(""))
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unauthenticated, err)
}

func TestAuthorizationHeader(t *testing.T) {
	f := RequirePresharedKey([]string{"one", "two"})
	_, err := f(context.Background())
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unauthenticated, err)
}

func contextWithBearerToken(token string) context.Context {
	md := metadata.Pairs("authorization", fmt.Sprintf("bearer %s", token))
	return metautils.NiceMD(md).ToIncoming(context.Background())
}

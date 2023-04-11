package auth

import (
	"context"
	"testing"

	"github.com/authzed/grpcutil"
	metautils "github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestPresharedKeys(t *testing.T) {
	testcases := []struct {
		name           string
		presharedkeys  []string
		withMetadata   bool
		authzHeader    string
		expectedStatus codes.Code
	}{
		{"valid request with the first key", []string{"one", "two"}, true, "bearer one", codes.OK},
		{"valid request with the second key", []string{"one", "two"}, true, "bearer two", codes.OK},
		{"denied due to unknown key", []string{"one", "two"}, true, "bearer three", codes.PermissionDenied},
		{"unauthenticated due to missing key", []string{"one", "two"}, true, "bearer ", codes.Unauthenticated},
		{"unauthenticated due to empty header", []string{"one", "two"}, true, "", codes.Unauthenticated},
		{"unauthenticated due to missing metadata", []string{"one", "two"}, false, "", codes.Unauthenticated},
	}

	for _, testcase := range testcases {
		testcase := testcase
		t.Run(testcase.name, func(t *testing.T) {
			f := MustRequirePresharedKey(testcase.presharedkeys)
			ctx := context.Background()
			if testcase.withMetadata {
				ctx = withTokenMetadata(testcase.authzHeader)
			}
			_, err := f(ctx)
			if testcase.expectedStatus != codes.OK {
				require.Error(t, err)
				grpcutil.RequireStatus(t, testcase.expectedStatus, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func withTokenMetadata(authzHeader string) context.Context {
	md := metadata.Pairs("authorization", authzHeader)
	return metautils.MD(md).ToIncoming(context.Background())
}

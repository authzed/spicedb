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

func TestPresharedKeys(t *testing.T) {
	testcases := []struct {
		name           string
		presharedkeys  []string
		withMetadata   bool
		token          string
		expectedStatus codes.Code
	}{
		{"valid request with the first key", []string{"one", "two"}, true, "one", codes.OK},
		{"valid request with the second key", []string{"one", "two"}, true, "two", codes.OK},
		{"denied due to unknown key", []string{"one", "two"}, true, "three", codes.PermissionDenied},
		{"unauthenticated due to missing key", []string{"one", "two"}, true, "", codes.Unauthenticated},
		{"unauthenticated due to missing metadata", []string{"one", "two"}, false, "", codes.Unauthenticated},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			f := RequirePresharedKey(testcase.presharedkeys)
			ctx := context.Background()
			if testcase.withMetadata {
				ctx = withTokenMetadata(testcase.token)
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

func withTokenMetadata(token string) context.Context {
	md := metadata.Pairs("authorization", fmt.Sprintf("bearer %s", token))
	return metautils.NiceMD(md).ToIncoming(context.Background())
}

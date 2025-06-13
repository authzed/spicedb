package auth

import (
	"context"
	"fmt"
	"testing"

	metautils "github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/grpcutil"
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
			ctx := t.Context()
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

func TestMustRequirePresharedKeyPanics(t *testing.T) {
	t.Run("panics with empty slice", func(t *testing.T) {
		require.Panics(t, func() {
			MustRequirePresharedKey([]string{})
		})
	})

	t.Run("panics with empty key", func(t *testing.T) {
		require.Panics(t, func() {
			MustRequirePresharedKey([]string{"valid", ""})
		})
	})

	t.Run("panics with nil slice", func(t *testing.T) {
		require.Panics(t, func() {
			MustRequirePresharedKey(nil)
		})
	})
}

func TestPresharedKeyEdgeCases(t *testing.T) {
	testcases := []struct {
		name           string
		presharedkeys  []string
		token          string
		expectedStatus codes.Code
	}{
		{"single character key match", []string{"a"}, "a", codes.OK},
		{"unicode key match", []string{"🔑"}, "🔑", codes.OK},
		{"very long key match", []string{"very-long-preshared-key-that-is-quite-lengthy-and-should-still-work"}, "very-long-preshared-key-that-is-quite-lengthy-and-should-still-work", codes.OK},
		{"case sensitive mismatch", []string{"Key"}, "key", codes.PermissionDenied},
		{"partial match denied", []string{"longkey"}, "long", codes.PermissionDenied},
		{"timing attack protection", []string{"secret"}, "secre", codes.PermissionDenied},
	}

	for _, testcase := range testcases {
		testcase := testcase
		t.Run(testcase.name, func(t *testing.T) {
			f := MustRequirePresharedKey(testcase.presharedkeys)
			ctx := withTokenMetadata("bearer " + testcase.token)
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

func TestPresharedKeyMultipleKeys(t *testing.T) {
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	f := MustRequirePresharedKey(keys)

	// Test that all keys work
	for i, key := range keys {
		t.Run(fmt.Sprintf("key_%d_works", i+1), func(t *testing.T) {
			ctx := withTokenMetadata("bearer " + key)
			_, err := f(ctx)
			require.NoError(t, err)
		})
	}

	// Test invalid key still fails
	t.Run("invalid_key_fails", func(t *testing.T) {
		ctx := withTokenMetadata("bearer invalid")
		_, err := f(ctx)
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.PermissionDenied, err)
	})
}

func withTokenMetadata(authzHeader string) context.Context {
	md := metadata.Pairs("authorization", authzHeader)
	return metautils.MD(md).ToIncoming(context.Background())
}

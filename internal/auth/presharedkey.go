package auth

import (
	"context"
	"crypto/subtle"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	errInvalidPresharedKey = "invalid preshared key: %s"
	errMissingPresharedKey = "missing preshared key"
)

var errInvalidToken = "invalid token"

// MustRequirePresharedKey requires that gRPC requests have a Bearer Token value
// equivalent to one of the provided preshared key(s).
func MustRequirePresharedKey(presharedKeys []string) grpcauth.AuthFunc {
	if len(presharedKeys) == 0 {
		panic("RequirePresharedKey was given an empty preshared keys slice")
	}

	for _, presharedKey := range presharedKeys {
		if len(presharedKey) == 0 {
			panic("RequirePresharedKey was given an empty preshared key")
		}
	}

	return func(ctx context.Context) (context.Context, error) {
		token, err := grpcauth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, errInvalidPresharedKey, err.Error())
		}

		if token == "" {
			return nil, status.Errorf(codes.Unauthenticated, errMissingPresharedKey)
		}

		for _, presharedKey := range presharedKeys {
			if match := subtle.ConstantTimeCompare([]byte(presharedKey), []byte(token)); match == 1 {
				return ctx, nil
			}
		}

		return nil, status.Errorf(codes.PermissionDenied, errInvalidPresharedKey, errInvalidToken)
	}
}

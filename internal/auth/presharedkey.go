package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
)

const errInvalidPresharedKey = "invalid preshared key: %w"

var errInvalidToken = errors.New("invalid token")

// RequirePresharedKey requires that gRPC requests have a Bearer Token value
// equivalent to one of the provided preshared key(s).
func RequirePresharedKey(presharedKeys []string) grpcauth.AuthFunc {
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
			return nil, fmt.Errorf(errInvalidPresharedKey, err)
		}

		for _, presharedKey := range presharedKeys {
			if match := subtle.ConstantTimeCompare([]byte(presharedKey), []byte(token)); match == 1 {
				return ctx, nil
			}
		}

		return nil, fmt.Errorf(errInvalidPresharedKey, errInvalidToken)
	}
}

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
// equivalant to the provided preshared key.
func RequirePresharedKey(presharedKey string) grpcauth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		token, err := grpcauth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return nil, fmt.Errorf(errInvalidPresharedKey, err)
		}

		if match := subtle.ConstantTimeCompare([]byte(presharedKey), []byte(token)); match != 1 {
			return nil, fmt.Errorf(errInvalidPresharedKey, errInvalidToken)
		}

		return ctx, nil
	}
}

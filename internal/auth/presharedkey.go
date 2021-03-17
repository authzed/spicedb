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

func RequirePresharedKey(presharedKey string) grpcauth.AuthFunc {
	return (&requirePresharedKey{[]byte(presharedKey)}).checkPresharedKey
}

type requirePresharedKey struct {
	expectedPSK []byte
}

func (rpsk *requirePresharedKey) checkPresharedKey(
	ctx context.Context,
) (context.Context, error) {
	token, err := grpcauth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, fmt.Errorf(errInvalidPresharedKey, err)
	}

	if match := subtle.ConstantTimeCompare(rpsk.expectedPSK, []byte(token)); match != 1 {
		return nil, fmt.Errorf(errInvalidPresharedKey, errInvalidToken)
	}

	return ctx, nil
}

type NoAuthRequired struct{}

func (noauth NoAuthRequired) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	return ctx, nil
}

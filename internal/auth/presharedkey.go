package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/metadata"
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

// PresharedKeyAnnotator copies the Authorization header from an HTTP request
// and sets it on the gRPC context.
func PresharedKeyAnnotator(ctx context.Context, r *http.Request) metadata.MD {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.New(map[string]string{})
	}
	md.Append("authorization", r.Header.Get("Authorization"))
	return md
}

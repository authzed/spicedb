package grpcutil

import "context"

// IgnoreAuthMix implements the ServiceAuthFuncOverride interface to ignore any
// auth requirements set by github.com/grpc-ecosystem/go-grpc-middleware/auth.
type IgnoreAuthMixin struct{}

func (m IgnoreAuthMixin) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	return ctx, nil
}

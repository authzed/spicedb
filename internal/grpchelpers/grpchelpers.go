package grpchelpers

import (
	"context"

	"google.golang.org/grpc"
)

// DialAndWait creates a new client connection to the target and blocks until the connection is ready.
func DialAndWait(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// TODO: move to NewClient
	opts = append(opts, grpc.WithBlock())         // nolint: staticcheck
	return grpc.DialContext(ctx, target, opts...) // nolint: staticcheck
}

// Dial creates a new client connection to the target.
func Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// TODO: move to NewClient
	return grpc.DialContext(ctx, target, opts...) // nolint: staticcheck
}

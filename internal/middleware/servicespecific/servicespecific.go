package servicespecific

import (
	"context"

	"google.golang.org/grpc"
)

// ExtraUnaryInterceptor is an interface for a service which has its own bundled
// unary interceptors that must be run.
type ExtraUnaryInterceptor interface {
	UnaryInterceptor() grpc.UnaryServerInterceptor
}

// ExtraStreamInterceptor is an interface for a service which has its own bundled
// stream interceptors that must be run.
type ExtraStreamInterceptor interface {
	StreamInterceptor() grpc.StreamServerInterceptor
}

// UnaryServerInterceptor returns a new unary server interceptor that runs bundled interceptors.
func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if hasExtraInterceptor, ok := info.Server.(ExtraUnaryInterceptor); ok {
		interceptor := hasExtraInterceptor.UnaryInterceptor()
		return interceptor(ctx, req, info, handler)
	}

	return handler(ctx, req)
}

// StreamServerInterceptor returns a new stream server interceptor that runs bundled interceptors.
func StreamServerInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if hasExtraInterceptor, ok := srv.(ExtraStreamInterceptor); ok {
		interceptor := hasExtraInterceptor.StreamInterceptor()
		return interceptor(srv, stream, info, handler)
	}

	return handler(srv, stream)
}

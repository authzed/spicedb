package injector

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
)

type Key any

// ContextWithValue injects a value in the context identified by the provided key
func ContextWithValue[T any](ctx context.Context, key Key, value T) context.Context {
	return context.WithValue(ctx, key, value)
}

// FromContext returns a value in the context specified by the provided key.
func FromContext[T any](ctx context.Context, key Key) T {
	var value T
	if v := ctx.Value(key); v != nil {
		value = v.(T)
	}
	return value
}

// UnaryServerInterceptor returns a new unary server interceptor that injects
// state in the context identified by a specific key
func UnaryServerInterceptor[T any](key Key, value T) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(ContextWithValue[T](ctx, key, value), req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that injects
// state in the context identified by a specific key
func StreamServerInterceptor[T any](key Key, value T) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithValue[T](wrapped.WrappedContext, key, value)
		return handler(srv, wrapped)
	}
}

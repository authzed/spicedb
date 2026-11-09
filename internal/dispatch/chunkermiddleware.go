package dispatch

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a unary server interceptor that adds chunker to the request context.
// A nil chunker selects DefaultChunker.
func UnaryServerInterceptor(chunker Chunker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ContextWithChunker(ctx, chunker), req)
	}
}

// StreamServerInterceptor returns a stream server interceptor that adds chunker to the stream context.
// A nil chunker selects DefaultChunker.
func StreamServerInterceptor(chunker Chunker) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithChunker(wrapped.WrappedContext, chunker)
		return handler(srv, wrapped)
	}
}

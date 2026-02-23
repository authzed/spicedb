package readonly

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// UnaryServerInterceptor returns a new unary server interceptor that sets the datalayer to readonly
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := datalayer.SetInContext(ctx, datalayer.NewReadonlyDataLayer(datalayer.MustFromContext(ctx))); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that sets the datalayer to readonly
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		if err := datalayer.SetInContext(wrapped.WrappedContext, datalayer.NewReadonlyDataLayer(datalayer.MustFromContext(stream.Context()))); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}

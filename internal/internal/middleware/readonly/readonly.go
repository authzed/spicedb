package readonly

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/proxy"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

// UnaryServerInterceptor returns a new unary server interceptor that sets the datastore to readonly
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := datastoremw.SetInContext(ctx, proxy.NewReadonlyDatastore(datastoremw.MustFromContext(ctx))); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor  that sets the datastore to readonly
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		if err := datastoremw.SetInContext(wrapped.WrappedContext, proxy.NewReadonlyDatastore(datastoremw.MustFromContext(stream.Context()))); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}

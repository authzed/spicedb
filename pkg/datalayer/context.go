package datalayer

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/authzed/ctxkey"
)

var datalayerKey = ctxkey.NewBoxedWithDefault[DataLayer](nil)

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the datalayer.
func ContextWithHandle(ctx context.Context) context.Context {
	return datalayerKey.SetBox(ctx)
}

// FromContext reads the DataLayer out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) DataLayer {
	return datalayerKey.Value(ctx)
}

// MustFromContext reads the DataLayer out of a context.Context and panics if it does not exist.
func MustFromContext(ctx context.Context) DataLayer {
	dl := FromContext(ctx)
	if dl == nil {
		panic("datalayer middleware did not inject datalayer")
	}
	return dl
}

// SetInContext sets the DataLayer in the given context.
func SetInContext(ctx context.Context, dl DataLayer) error {
	datalayerKey.Set(ctx, dl)
	return nil
}

// ContextWithDataLayer adds the handle and DataLayer in one step.
func ContextWithDataLayer(ctx context.Context, dl DataLayer) context.Context {
	ctx = datalayerKey.SetBox(ctx)
	datalayerKey.Set(ctx, dl)
	return ctx
}

// UnaryServerInterceptor returns a new unary server interceptor that adds the
// DataLayer to the context.
func UnaryServerInterceptor(dl DataLayer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx := ContextWithHandle(ctx)
		if err := SetInContext(newCtx, dl); err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that adds the
// DataLayer to the context.
func StreamServerInterceptor(dl DataLayer) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithHandle(wrapped.WrappedContext)
		if err := SetInContext(wrapped.WrappedContext, dl); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}

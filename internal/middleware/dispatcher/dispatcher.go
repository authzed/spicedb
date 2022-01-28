package dispatcher

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
)

type ctxKeyType struct{}

var dispatcherKey ctxKeyType = struct{}{}

type dispatchHandle struct {
	dispatcher dispatch.Dispatcher
}

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the dispatcher
func ContextWithHandle(ctx context.Context) context.Context {
	return context.WithValue(ctx, dispatcherKey, &dispatchHandle{})
}

// FromContext reads the selected dispatcher out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) dispatch.Dispatcher {
	if c := ctx.Value(dispatcherKey); c != nil {
		handle := c.(*dispatchHandle)
		return handle.dispatcher
	}
	return nil
}

// MustFromContext reads the selected dispatcher out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) dispatch.Dispatcher {
	dispatcher := FromContext(ctx)
	if dispatcher == nil {
		panic("dispatcher middleware did not inject dispatcher")
	}

	return dispatcher
}

// SetInContext adds a dispatcher to the given context
func SetInContext(ctx context.Context, dispatcher dispatch.Dispatcher) error {
	handle := ctx.Value(dispatcherKey)
	if handle == nil {
		return nil
	}
	handle.(*dispatchHandle).dispatcher = dispatcher
	return nil
}

// UnaryServerInterceptor returns a new unary server interceptor that adds the
// dispatcher to the context
func UnaryServerInterceptor(dispatcher dispatch.Dispatcher) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx := ContextWithHandle(ctx)
		if err := SetInContext(newCtx, dispatcher); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that adds the
// dispatcher to the context
func StreamServerInterceptor(dispatcher dispatch.Dispatcher) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithHandle(wrapped.WrappedContext)
		if err := SetInContext(wrapped.WrappedContext, dispatcher); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}

package dispatcher

import (
	"context"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb/internal/dispatch"
)

type dispatchHandle struct {
	dispatcher dispatch.Dispatcher
}

var dispatcherKey = ctxkey.New[*dispatchHandle]()

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the dispatcher
func ContextWithHandle(ctx context.Context) context.Context {
	return dispatcherKey.Set(ctx, &dispatchHandle{})
}

// FromContext reads the selected dispatcher out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) dispatch.Dispatcher {
	if handle, ok := dispatcherKey.Value(ctx); ok {
		return handle.dispatcher
	}
	return nil
}

// SetInContext adds a dispatcher to the given context
func SetInContext(ctx context.Context, dispatcher dispatch.Dispatcher) error {
	handle, ok := dispatcherKey.Value(ctx)
	if !ok {
		return nil
	}
	handle.dispatcher = dispatcher
	return nil
}

// UnaryServerInterceptor returns a new unary server interceptor that adds the
// dispatcher to the context
func UnaryServerInterceptor(dispatcher dispatch.Dispatcher) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithHandle(wrapped.WrappedContext)
		if err := SetInContext(wrapped.WrappedContext, dispatcher); err != nil {
			return err
		}
		return handler(srv, wrapped)
	}
}

package interceptorwrapper

import (
	"context"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

var tracer = otel.Tracer("spicedb/internal/middleware")

// WrapUnaryServerInterceptorWithSpans returns a new interceptor that wraps the given interceptor
// with a span, measuring the duration of the interceptor's pre-handler logic.
func WrapUnaryServerInterceptorWithSpans(
	inner grpc.UnaryServerInterceptor,
	spanName string,
) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, span := tracer.Start(ctx, spanName)
		// NOTE: this shim is what lets us measure how long the interceptor is doing work.
		// It's the handler that we pass to the wrapped interceptor, so `span.End()` will be
		// called when the handler itself is called.
		shimHandler := func(ctx context.Context, req any) (any, error) {
			span.End()
			return handler(ctx, req)
		}
		resp, err := inner(ctx, req, info, shimHandler)
		if span.IsRecording() {
			span.End()
		}
		return resp, err
	}
}

package interceptorwrapper

import (
	"context"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
)

// WrapUnaryServerInterceptorWithSpans returns a new interceptor that wraps the given interceptor
// with a span, measuring the duration of the interceptor's pre-handler logic.
func WrapUnaryServerInterceptorWithSpans(
	inner grpc.UnaryServerInterceptor,
	tracerName, spanName string,
) grpc.UnaryServerInterceptor {
	t := otel.Tracer(tracerName)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, span := t.Start(ctx, spanName)
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

// WrapStreamServerInterceptorWithSpans returns a new interceptor that wraps the given interceptor
// with a span, measuring the duration of the interceptor's pre-handler logic.
func WrapStreamServerInterceptorWithSpans(
	inner grpc.StreamServerInterceptor,
	tracerName, spanName string,
) grpc.StreamServerInterceptor {
	t := otel.Tracer(tracerName)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, span := t.Start(ss.Context(), spanName)
		wrappedStream := &contextStream{ServerStream: ss, ctx: ctx}
		shimHandler := func(srv any, stream grpc.ServerStream) error {
			span.End()
			return handler(srv, stream)
		}
		err := inner(srv, wrappedStream, info, shimHandler)
		if span.IsRecording() {
			span.End()
		}
		return err
	}
}

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *contextStream) Context() context.Context { return s.ctx }

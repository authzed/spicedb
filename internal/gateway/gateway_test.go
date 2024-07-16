package gateway

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/testutil"
)

func TestOtelForwarding(t *testing.T) {
	// Set the global propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create some test IDs
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.Nil(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.Nil(t, err)

	// Create a context with the test data
	inCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	// Create a new http request and inject the context into the headers
	r, err := http.NewRequest(http.MethodPost, "/v1/schema/read", nil)
	require.Nil(t, err)
	otel.GetTextMapPropagator().Inject(inCtx, propagation.HeaderCarrier(r.Header))

	// Run the annotator with a new context to ensure no existing tracing data
	outCtx := context.Background()
	md := OtelAnnotator(outCtx, r)

	// Assert the context was injected into the gRPC context.
	_, spanCtx := otelgrpc.Extract(outCtx, &md, defaultOtelOpts...)
	require.True(t, spanCtx.HasTraceID())
	require.Equal(t, traceID, spanCtx.TraceID())
}

func TestCloseConnections(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	gatewayHandler, err := NewHandler(context.Background(), "192.0.2.0:4321", "")
	require.NoError(t, err)
	// 4 conns for permission+schema+watch+experimental services, 1 for health check
	require.Len(t, gatewayHandler.closers, 5)

	// if connections are not closed, goleak would detect it
	require.NoError(t, gatewayHandler.Close())
}

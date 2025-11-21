package gateway

import (
	"net/http"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/pkg/testutil"
)

func TestOtelForwarding(t *testing.T) {
	// Set the global propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create some test IDs
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	// Create a context with the test data
	inCtx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	// Create a new http request and inject the context into the headers
	r, err := http.NewRequest(http.MethodPost, "/v1/schema/read", nil)
	require.NoError(t, err)
	otel.GetTextMapPropagator().Inject(inCtx, propagation.HeaderCarrier(r.Header))

	// Run the annotator with a new context to ensure no existing tracing data
	outCtx := t.Context()
	md := OtelAnnotator(outCtx, r)

	// Assert the context was injected into the gRPC context.
	_, spanCtx := otelgrpc.Extract(outCtx, &md, defaultOtelOpts...) // nolint:staticcheck
	require.True(t, spanCtx.HasTraceID())
	require.Equal(t, traceID, spanCtx.TraceID())
}

func TestCloseConnections(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	gatewayHandler, err := NewHandler(t.Context(), "192.0.2.0:4321", "")
	require.NoError(t, err)
	// 4 conns for permission+schema+watch+experimental services, 1 for health check
	require.Len(t, gatewayHandler.closers, 5)

	// if connections are not closed, goleak would detect it
	require.NoError(t, gatewayHandler.Close())
}

func TestCustomIncomingHeaderMatcher(t *testing.T) {
	// Test x-request-id is passed through without grpcgateway- prefix
	key, ok := customIncomingHeaderMatcher("x-request-id")
	require.True(t, ok)
	require.Equal(t, "x-request-id", key)

	// Test case insensitivity
	key, ok = customIncomingHeaderMatcher("X-Request-ID")
	require.True(t, ok)
	require.Equal(t, "x-request-id", key)

	key, ok = customIncomingHeaderMatcher("X-REQUEST-ID")
	require.True(t, ok)
	require.Equal(t, "x-request-id", key)

	// Test other headers use default behaviour
	key, ok = customIncomingHeaderMatcher("authorization")
	require.True(t, ok)
	require.Equal(t, "grpcgateway-Authorization", key)
}

func TestForwardRequestIDTrailer(t *testing.T) {
	t.Run("with standard x-request-id key", func(t *testing.T) {
		md := runtime.ServerMetadata{
			TrailerMD: metadata.Pairs("x-request-id", "test-id-123"),
		}
		ctx := runtime.NewServerMetadataContext(t.Context(), md)

		recorder := &headerRecorder{header: http.Header{}}
		err := forwardRequestIDTrailer(ctx, recorder, nil)
		require.NoError(t, err)
		require.Equal(t, "test-id-123", recorder.Header().Get("X-Request-Id"))
	})

	t.Run("with legacy io.spicedb.respmeta.requestid key", func(t *testing.T) {
		md := runtime.ServerMetadata{
			TrailerMD: metadata.Pairs("io.spicedb.respmeta.requestid", "legacy-id-456"),
		}
		ctx := runtime.NewServerMetadataContext(t.Context(), md)

		recorder := &headerRecorder{header: http.Header{}}
		err := forwardRequestIDTrailer(ctx, recorder, nil)
		require.NoError(t, err)
		require.Equal(t, "legacy-id-456", recorder.Header().Get("X-Request-Id"))
	})

	t.Run("prefers standard key over legacy", func(t *testing.T) {
		md := runtime.ServerMetadata{
			TrailerMD: metadata.Join(
				metadata.Pairs("x-request-id", "standard-id"),
				metadata.Pairs("io.spicedb.respmeta.requestid", "legacy-id"),
			),
		}
		ctx := runtime.NewServerMetadataContext(t.Context(), md)

		recorder := &headerRecorder{header: http.Header{}}
		err := forwardRequestIDTrailer(ctx, recorder, nil)
		require.NoError(t, err)
		require.Equal(t, "standard-id", recorder.Header().Get("X-Request-Id"))
	})

	t.Run("without metadata", func(t *testing.T) {
		recorder := &headerRecorder{header: http.Header{}}
		err := forwardRequestIDTrailer(t.Context(), recorder, nil)
		require.NoError(t, err)
		require.Empty(t, recorder.Header().Get("X-Request-Id"))
	})

	t.Run("without request ID in metadata", func(t *testing.T) {
		md := runtime.ServerMetadata{
			TrailerMD: metadata.Pairs("some-other-header", "value"),
		}
		ctx := runtime.NewServerMetadataContext(t.Context(), md)

		recorder := &headerRecorder{header: http.Header{}}
		err := forwardRequestIDTrailer(ctx, recorder, nil)
		require.NoError(t, err)
		require.Empty(t, recorder.Header().Get("X-Request-Id"))
	})
}

// headerRecorder is a test helper that implements http.ResponseWriter
type headerRecorder struct {
	header http.Header
}

func (h *headerRecorder) Header() http.Header {
	return h.header
}

func (h *headerRecorder) Write([]byte) (int, error) {
	return 0, nil
}

func (h *headerRecorder) WriteHeader(statusCode int) {}

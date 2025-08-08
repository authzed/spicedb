package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

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
	inCtx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	}))

	// Create a new http request and inject the context into the headers
	r, err := http.NewRequest(http.MethodPost, "/v1/schema/read", nil)
	require.Nil(t, err)
	otel.GetTextMapPropagator().Inject(inCtx, propagation.HeaderCarrier(r.Header))

	// Run the annotator with a new context to ensure no existing tracing data
	outCtx := t.Context()
	md := OtelAnnotator(outCtx, r)

	// Assert the context was injected into the gRPC context.
	_, spanCtx := otelgrpc.Extract(outCtx, &md, defaultOtelOpts...)
	require.True(t, spanCtx.HasTraceID())
	require.Equal(t, traceID, spanCtx.TraceID())
}

func TestCloseConnections(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	gatewayHandler, err := NewHandler(t.Context(), "192.0.2.0:4321", "", false)
	require.NoError(t, err)
	// 4 conns for permission+schema+watch+experimental services, 1 for health check
	require.Len(t, gatewayHandler.closers, 5)

	// if connections are not closed, goleak would detect it
	require.NoError(t, gatewayHandler.Close())
}

func TestGatewayHealthCheckTracing(t *testing.T) {
	tests := []struct {
		name                      string
		disableHealthCheckTracing bool
		description               string
	}{
		{
			name:                      "health check tracing disabled",
			disableHealthCheckTracing: true,
			description:               "gateway should skip tracing for health checks when flag is true",
		},
		{
			name:                      "health check tracing enabled",
			disableHealthCheckTracing: false,
			description:               "gateway should trace health checks when flag is false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			// Since we can't use a real gRPC server, we need to mock one
			mockServer := setupMockGRPCServer(t, ctx)
			defer mockServer.Stop()

			defaultProvider := otel.GetTracerProvider()
			defer otel.SetTracerProvider(defaultProvider)

			provider := sdktrace.NewTracerProvider(
				sdktrace.WithSampler(sdktrace.AlwaysSample()),
			)
			spanrecorder := tracetest.NewSpanRecorder()
			provider.RegisterSpanProcessor(spanrecorder)
			otel.SetTracerProvider(provider)

			gatewayHandler, err := NewHandler(ctx, mockServer.Addr(), "", tt.disableHealthCheckTracing)
			require.NoError(t, err)
			defer gatewayHandler.Close()

			// Test health check endpoint
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			w := httptest.NewRecorder()
			gatewayHandler.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			healthSpans := spanrecorder.Ended()
			spanrecorder.Reset()

			// Test regular endpoint
			req = httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
			w = httptest.NewRecorder()
			gatewayHandler.ServeHTTP(w, req)
			require.Equal(t, http.StatusOK, w.Code)

			regularSpans := spanrecorder.Ended()

			if tt.disableHealthCheckTracing {
				require.Len(t, healthSpans, 0, "Health check should not create spans when tracing is disabled")

				healthFound := false
				for _, span := range healthSpans {
					if span.Name() == "grpc.health.v1.Health/Check" {
						healthFound = true
						break
					}
				}
				require.False(t, healthFound, "Unexpectedly found health check span when tracing is disabled")

				gatewayFound := false
				for _, span := range healthSpans {
					if span.Name() == "gateway" {
						gatewayFound = true
						break
					}
				}
				require.False(t, gatewayFound, "Unexpectedly found gateway span for health check when tracing is disabled")
			} else {
				require.True(t, len(healthSpans) > 0, "Health check should create spans when tracing is enabled")

				healthFound := false
				for _, span := range healthSpans {
					if span.Name() == "grpc.health.v1.Health/Check" {
						healthFound = true
						break
					}
				}
				require.True(t, healthFound, "Expected to find health check span when tracing is enabled")
			}

			require.True(t, len(regularSpans) > 0, "Regular endpoints should create spans")
			regularFound := false
			for _, span := range regularSpans {
				if span.Name() == "gateway" {
					regularFound = true
					break
				}
			}
			require.True(t, regularFound, "Expected to find gateway span for regular endpoint")
		})
	}
}

type mockGRPCServer struct {
	server   *grpc.Server
	listener net.Listener
	addr     string
}

func (m *mockGRPCServer) Addr() string {
	return m.addr
}

func (m *mockGRPCServer) Stop() {
	if m.server != nil {
		m.server.Stop()
	}
	if m.listener != nil {
		m.listener.Close()
	}
}

func setupMockGRPCServer(t *testing.T, ctx context.Context) *mockGRPCServer {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Mock gRPC server error: %v", err)
		}
	}()

	return &mockGRPCServer{
		server:   server,
		listener: listener,
		addr:     listener.Addr().String(),
	}
}

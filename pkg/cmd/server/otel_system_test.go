//go:build system

// pkg/cmd/server/otel_system_test.go
//
// System tests use an in-process OTLP gRPC collector to verify end-to-end
// span delivery. They require no external services.
package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// In-process OTLP collector
// ---------------------------------------------------------------------------

// inProcessCollector implements the OTLP TraceService gRPC server and records
// all received ResourceSpans so tests can assert on span delivery.
type inProcessCollector struct {
	collectortrace.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracev1.ResourceSpans
}

func (c *inProcessCollector) Export(
	_ context.Context,
	req *collectortrace.ExportTraceServiceRequest,
) (*collectortrace.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, req.ResourceSpans...)
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// ReceivedSpanCount returns the total number of individual spans received.
func (c *inProcessCollector) ReceivedSpanCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, rs := range c.spans {
		for _, scope := range rs.ScopeSpans {
			n += len(scope.Spans)
		}
	}
	return n
}

// startCollector starts an in-process OTLP gRPC collector on a random port.
// Returns the listener address, the collector, and a cleanup function.
func startCollector(t *testing.T) (string, *inProcessCollector, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to start in-process collector listener")

	collector := &inProcessCollector{}
	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	collectortrace.RegisterTraceServiceServer(srv, collector)

	go func() { _ = srv.Serve(lis) }()

	return lis.Addr().String(), collector, func() { srv.GracefulStop() }
}

// ---------------------------------------------------------------------------
// System tests
// ---------------------------------------------------------------------------

// TestOTelSystem_SpansDeliveredToCollector starts an in-process OTLP
// collector, initializes the OTel provider pointing at it, creates a test
// span, force-flushes, and verifies the collector received at least one span.
func TestOTelSystem_SpansDeliveredToCollector(t *testing.T) {
	addr, collector, cleanup := startCollector(t)
	defer cleanup()

	cmd := &cobra.Command{Use: "test"}
	RegisterOTelFlags(cmd)
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("otel-provider", "otlpgrpc"))
	require.NoError(t, cmd.Flags().Set("otel-endpoint", addr))
	require.NoError(t, cmd.Flags().Set("otel-insecure", "true"))
	require.NoError(t, cmd.Flags().Set("otel-service-name", "spicedb-system-test"))
	require.NoError(t, cmd.Flags().Set("otel-sample-ratio", "1.0"))

	provider, err := InitOTelProvider(cmd)
	require.NoError(t, err)
	require.NotNil(t, provider)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ShutdownOTelProvider(ctx, provider)
	})

	tracer := otel.Tracer("system-test")
	_, span := tracer.Start(context.Background(), "test-span")
	span.End()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()
	require.NoError(t, provider.ForceFlush(flushCtx))

	require.Eventually(t,
		func() bool { return collector.ReceivedSpanCount() >= 1 },
		5*time.Second, 100*time.Millisecond,
		"expected at least 1 span to be received by the in-process collector",
	)
}

// TestOTelSystem_SpansNotDroppedOnShutdown verifies that spans buffered in
// the BatchSpanProcessor are flushed before the provider shuts down, fixing
// the data-loss scenario described in issue #3095.
func TestOTelSystem_SpansNotDroppedOnShutdown(t *testing.T) {
	const spanCount = 5

	addr, collector, cleanup := startCollector(t)
	defer cleanup()

	cmd := &cobra.Command{Use: "test"}
	RegisterOTelFlags(cmd)
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("otel-provider", "otlpgrpc"))
	require.NoError(t, cmd.Flags().Set("otel-endpoint", addr))
	require.NoError(t, cmd.Flags().Set("otel-insecure", "true"))
	require.NoError(t, cmd.Flags().Set("otel-sample-ratio", "1.0"))

	provider, err := InitOTelProvider(cmd)
	require.NoError(t, err)
	require.NotNil(t, provider)

	tracer := otel.Tracer("shutdown-test")
	for i := 0; i < spanCount; i++ {
		_, span := tracer.Start(context.Background(), fmt.Sprintf("span-%d", i))
		span.End()
	}

	// Shut down immediately — spans must be flushed before exit.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	require.NoError(t, ShutdownOTelProvider(shutCtx, provider),
		"ShutdownOTelProvider must not error")

	assert.GreaterOrEqual(t, collector.ReceivedSpanCount(), spanCount,
		"all buffered spans must be delivered before Shutdown returns")
}

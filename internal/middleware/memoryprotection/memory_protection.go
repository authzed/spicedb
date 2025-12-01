package memoryprotection

import (
	"context"
	"reflect"
	"strconv"
	"strings"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
)

// RequestsProcessed tracks requests that were processed by this middleware.
var RequestsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "memory_middleware",
	Name:      "requests_processed_total",
	Help:      "Total requests processed by the memory protection middleware (flag --memory-protection-enabled)",
}, []string{"endpoint", "accepted"})

type MemoryProtectionMiddleware struct {
	currentMemoryUsageProvider MemoryUsageProvider
}

func New(usageProvider MemoryUsageProvider, name string) *MemoryProtectionMiddleware {
	am := MemoryProtectionMiddleware{
		currentMemoryUsageProvider: usageProvider,
	}

	log.Info().
		Str("name", name).
		Str("provider", reflect.TypeOf(usageProvider).String()).
		Msg("memory protection middleware initialized")

	return &am
}

// UnaryServerInterceptor returns a unary server interceptor that rejects incoming requests is memory usage is too high
func (am *MemoryProtectionMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := am.checkAdmission(info.FullMethod); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a stream server interceptor that rejects incoming requests is memory usage is too high
func (am *MemoryProtectionMiddleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := am.checkAdmission(info.FullMethod); err != nil {
			return err
		}

		wrapped := middleware.WrapServerStream(stream)
		return handler(srv, wrapped)
	}
}

// checkAdmission returns an error if the request should be denied because memory usage is too high.
func (am *MemoryProtectionMiddleware) checkAdmission(method string) error {
	accept := true
	defer func() {
		am.recordMetric(method, accept)
	}()

	if am.currentMemoryUsageProvider.IsMemLimitReached() {
		accept = false
		return status.Error(codes.ResourceExhausted, "server rejected the request because memory usage is too high")
	}

	return nil
}

// recordMetric updates the RequestsProcessed metric and returns the endpoint type for the input method.
func (am *MemoryProtectionMiddleware) recordMetric(fullMethod string, accepted bool) string {
	endpointType := "api"
	if strings.HasPrefix(fullMethod, "/dispatch.v1.DispatchService") {
		endpointType = "dispatch"
	}

	acceptedStr := strconv.FormatBool(accepted)

	RequestsProcessed.WithLabelValues(endpointType, acceptedStr).Inc()
	return endpointType
}

package memoryprotection

import (
	"context"
	"runtime/debug"
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

// Config holds configuration for the memory protection middleware
type Config struct {
	// ThresholdPercent is the memory usage threshold for requests, in the range [0,1]
	// If zero or negative, this middleware has no effect.
	ThresholdPercent float64
}

// DefaultConfig returns reasonable default configuration for API requests
func DefaultConfig() Config {
	return Config{
		ThresholdPercent: 0.90,
	}
}

// DefaultDispatchConfig returns reasonable default configuration for dispatch requests
func DefaultDispatchConfig() Config {
	return Config{
		ThresholdPercent: 0.95,
	}
}

// MemoryLimitProvider gets and sets the limit of memory usage.
// In production, use DefaultMemoryLimitProvider.
// For testing, use HardCodedMemoryLimitProvider.
type MemoryLimitProvider interface {
	GetInBytes() int64
	SetInBytes(int64)
}

var (
	_ MemoryLimitProvider = (*DefaultMemoryLimitProvider)(nil)
	_ MemoryLimitProvider = (*HardCodedMemoryLimitProvider)(nil)
)

type DefaultMemoryLimitProvider struct{}

func (p *DefaultMemoryLimitProvider) GetInBytes() int64 {
	// SetMemoryLimit returns the previously set memory limit.
	// A negative input does not adjust the limit, and allows for retrieval of the currently set memory limit
	return debug.SetMemoryLimit(-1)
}

func (p *DefaultMemoryLimitProvider) SetInBytes(limit int64) {
	debug.SetMemoryLimit(limit)
}

type HardCodedMemoryLimitProvider struct {
	Hardcodedlimit int64
}

func (p *HardCodedMemoryLimitProvider) GetInBytes() int64 {
	return p.Hardcodedlimit
}

func (p *HardCodedMemoryLimitProvider) SetInBytes(limit int64) {
	p.Hardcodedlimit = limit
}

type MemoryProtectionMiddleware struct {
	config  Config
	sampler MemorySampler
}

// New creates a new memory admission middleware with the given sampler, which is assumed to have been started already.
func New(config Config, sampler MemorySampler, name string) *MemoryProtectionMiddleware {
	am := MemoryProtectionMiddleware{
		config:  config,
		sampler: sampler,
	}

	if am.disabled() {
		log.Warn().Str("name", name).Msg("memory protection middleware disabled")
		return &am
	}

	log.Info().
		Str("name", name).
		Float64("threshold_percent", config.ThresholdPercent).
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
	if am.disabled() {
		return nil
	}

	accept := true
	defer func() {
		am.recordMetric(method, accept)
	}()

	if am.sampler.GetMemoryUsagePercent() >= am.config.ThresholdPercent {
		accept = false
		return status.Error(codes.ResourceExhausted, "server rejected the request because memory usage is above configured threshold")
	}

	return nil
}

func (am *MemoryProtectionMiddleware) disabled() bool {
	return am.config.ThresholdPercent <= 0
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

package memoryprotection

import (
	"context"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"time"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/authzed/spicedb/internal/logging"
)

var (
	// RejectedRequestsCounter tracks requests rejected due to memory pressure
	RejectedRequestsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_overload_rejected_requests_total",
		Help:      "Total requests rejected due to memory pressure",
	}, []string{"endpoint"})

	// MemoryUsageGauge tracks current memory usage percentage
	MemoryUsageGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_usage_percent",
		Help:      "Current memory usage as percentage of GOMEMLIMIT",
	})
)

// Config holds configuration for the memory protection middleware
type Config struct {
	// ThresholdPercent is the memory usage threshold for requests (0-100)
	ThresholdPercent int
	// SampleIntervalSeconds controls how often memory usage is sampled
	SampleIntervalSeconds int
}

// DefaultConfig returns reasonable default configuration for API requests
func DefaultConfig() Config {
	return Config{
		ThresholdPercent:      90,
		SampleIntervalSeconds: 1,
	}
}

// DefaultDispatchConfig returns reasonable default configuration for dispatch requests
func DefaultDispatchConfig() Config {
	return Config{
		ThresholdPercent:      95,
		SampleIntervalSeconds: 1,
	}
}

// AdmissionMiddleware implements memory-based admission control
type AdmissionMiddleware struct {
	config          Config
	memoryLimit     int64
	lastMemoryUsage atomic.Int64
	metricsSamples  []metrics.Sample
	ctx             context.Context
}

// New creates a new memory protection middleware with the given context
func New(ctx context.Context, config Config) *AdmissionMiddleware {
	// Use the provided context directly
	mwCtx := ctx

	// Get the current GOMEMLIMIT
	memoryLimit := debug.SetMemoryLimit(-1)
	if memoryLimit < 0 {
		// If no limit is set, we can't provide memory protection
		log.Info().Msg("GOMEMLIMIT not set, memory protection disabled")
		return &AdmissionMiddleware{
			config:      config,
			memoryLimit: -1, // Disabled
			ctx:         mwCtx,
		}
	}

	// Check if memory protection is disabled via config
	if config.ThresholdPercent <= 0 {
		log.Info().Msg("memory protection disabled via configuration")
		return &AdmissionMiddleware{
			config:      config,
			memoryLimit: -1, // Disabled
			ctx:         mwCtx,
		}
	}

	am := &AdmissionMiddleware{
		config:      config,
		memoryLimit: memoryLimit,
		metricsSamples: []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		},
		ctx: mwCtx,
	}

	// Initialize with current memory usage
	if err := am.sampleMemory(); err != nil {
		log.Warn().Err(err).Msg("failed to get initial memory sample")
	}

	// Start background sampling with context
	am.startBackgroundSampling()

	log.Info().
		Int64("memory_limit_bytes", memoryLimit).
		Int("threshold_percent", config.ThresholdPercent).
		Int("sample_interval_seconds", config.SampleIntervalSeconds).
		Msg("memory protection middleware initialized with background sampling")

	return am
}

// UnaryServerInterceptor returns a unary server interceptor that implements admission control
func (am *AdmissionMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if am.memoryLimit < 0 {
			// Memory protection is disabled
			return handler(ctx, req)
		}

		if err := am.checkAdmission(info.FullMethod); err != nil {
			am.recordRejection(info.FullMethod)
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a stream server interceptor that implements admission control
func (am *AdmissionMiddleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if am.memoryLimit < 0 {
			// Memory protection is disabled
			return handler(srv, stream)
		}

		if err := am.checkAdmission(info.FullMethod); err != nil {
			am.recordRejection(info.FullMethod)
			return err
		}

		wrapped := middleware.WrapServerStream(stream)
		return handler(srv, wrapped)
	}
}

// checkAdmission determines if a request should be admitted based on current memory usage
func (am *AdmissionMiddleware) checkAdmission(fullMethod string) error {
	memoryUsage := am.getCurrentMemoryUsage()

	usagePercent := float64(memoryUsage) / float64(am.memoryLimit) * 100

	// Metrics gauge is updated in background sampling

	if usagePercent > float64(am.config.ThresholdPercent) {
		log.Warn().
			Float64("memory_usage_percent", usagePercent).
			Int("threshold_percent", am.config.ThresholdPercent).
			Str("method", fullMethod).
			Msg("rejecting request due to memory pressure")

		return status.Errorf(codes.ResourceExhausted,
			"server is experiencing memory pressure (%.1f%% usage, threshold: %d%%)",
			usagePercent, am.config.ThresholdPercent)
	}

	return nil
}

// startBackgroundSampling starts a background goroutine that samples memory usage periodically
func (am *AdmissionMiddleware) startBackgroundSampling() {
	interval := time.Duration(am.config.SampleIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		defer log.Debug().Msg("memory protection background sampling stopped")

		log.Debug().
			Dur("interval", interval).
			Msg("memory protection background sampling started")

		for {
			select {
			case <-ticker.C:
				if err := am.sampleMemory(); err != nil {
					log.Warn().Err(err).Msg("background memory sampling failed")
				}
			case <-am.ctx.Done():
				return
			}
		}
	}()
}

// sampleMemory samples the current memory usage and updates the cached value
func (am *AdmissionMiddleware) sampleMemory() error {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Interface("panic", r).Msg("memory sampling panicked")
		}
	}()

	metrics.Read(am.metricsSamples)
	newUsage := int64(am.metricsSamples[0].Value.Uint64())
	am.lastMemoryUsage.Store(newUsage)

	// Update metrics gauge
	if am.memoryLimit > 0 {
		usagePercent := float64(newUsage) / float64(am.memoryLimit) * 100
		MemoryUsageGauge.Set(usagePercent)
	}

	return nil
}

// getCurrentMemoryUsage returns the cached memory usage in bytes
func (am *AdmissionMiddleware) getCurrentMemoryUsage() int64 {
	return am.lastMemoryUsage.Load()
}

// recordRejection records metrics for rejected requests
func (am *AdmissionMiddleware) recordRejection(fullMethod string) {
	endpointType := "api"
	if strings.HasPrefix(fullMethod, "/dispatch.v1.DispatchService") {
		endpointType = "dispatch"
	}

	RejectedRequestsCounter.WithLabelValues(endpointType).Inc()
}

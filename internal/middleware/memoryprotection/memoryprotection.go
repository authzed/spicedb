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
		Subsystem: "memory_admission",
		Name:      "rejected_requests_total",
		Help:      "Total requests rejected due to memory pressure",
	}, []string{"endpoint"})

	// MemoryUsageGauge tracks current memory usage percentage
	MemoryUsageGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "memory_admission",
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

// MemoryLimitProvider gets and sets the limit of memory usage.
// In production, use DefaultMemoryLimitProvider.
// For testing, use HardCodedMemoryLimitProvider.
type MemoryLimitProvider interface {
	Get() int64
	Set(int64)
}

var (
	_ MemoryLimitProvider = (*DefaultMemoryLimitProvider)(nil)
	_ MemoryLimitProvider = (*HardCodedMemoryLimitProvider)(nil)
)

type DefaultMemoryLimitProvider struct{}

func (p *DefaultMemoryLimitProvider) Get() int64 {
	// SetMemoryLimit returns the previously set memory limit.
	// A negative input does not adjust the limit, and allows for retrieval of the currently set memory limit
	return debug.SetMemoryLimit(-1)
}

func (p *DefaultMemoryLimitProvider) Set(limit int64) {
	debug.SetMemoryLimit(limit)
}

type HardCodedMemoryLimitProvider struct {
	Hardcodedlimit int64
}

func (p *HardCodedMemoryLimitProvider) Get() int64 {
	return p.Hardcodedlimit
}

func (p *HardCodedMemoryLimitProvider) Set(limit int64) {
	p.Hardcodedlimit = limit
}

type MemoryAdmissionMiddleware struct {
	config         Config
	memoryLimit    int64 // -1 means no limit
	metricsSamples []metrics.Sample
	ctx            context.Context // to stop the background process

	lastMemorySampleInBytes   *atomic.Uint64 // atomic because it's written inside a goroutine but can be read from anywhere
	timestampLastMemorySample *atomic.Pointer[time.Time]
}

// New creates a new memory admission middleware with the given context.
// Whe the context is cancelled, this middleware stops its background processing.
func New(ctx context.Context, config Config, limitProvider MemoryLimitProvider) MemoryAdmissionMiddleware {
	am := MemoryAdmissionMiddleware{
		config:                    config,
		lastMemorySampleInBytes:   &atomic.Uint64{},
		timestampLastMemorySample: &atomic.Pointer[time.Time]{},
		memoryLimit:               -1, // disabled initially
		ctx:                       ctx,
	}

	// Get the current GOMEMLIMIT
	memoryLimit := limitProvider.Get()
	if memoryLimit < 0 {
		// If no limit is set, we can't provide memory protection
		log.Info().Msg("GOMEMLIMIT not set, memory protection disabled")
		return am
	}

	if config.ThresholdPercent <= 0 {
		log.Info().Msg("memory protection disabled via configuration")
		return am
	}

	am.memoryLimit = memoryLimit
	am.metricsSamples = []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
	}

	// Initialize with current memory usage
	am.sampleMemory()

	// Start background sampling with context
	am.startBackgroundSampling()

	log.Info().
		Int64("memory_limit_bytes", memoryLimit).
		Int("threshold_percent", config.ThresholdPercent).
		Int("sample_interval_seconds", config.SampleIntervalSeconds).
		Msg("memory protection middleware initialized with background sampling")

	return am
}

// UnaryServerInterceptor returns a unary server interceptor that rejects incoming requests is memory usage is too high
func (am *MemoryAdmissionMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
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

// StreamServerInterceptor returns a stream server interceptor that rejects incoming requests is memory usage is too high
func (am *MemoryAdmissionMiddleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
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
func (am *MemoryAdmissionMiddleware) checkAdmission(fullMethod string) error {
	memoryUsage := am.getLastMemorySampleInBytes()

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
func (am *MemoryAdmissionMiddleware) startBackgroundSampling() {
	interval := time.Duration(am.config.SampleIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		// TODO this code might start running before the logger is setup, therefore we have a data race
		//defer log.Debug().Msg("memory protection background sampling stopped")
		//
		//log.Debug().
		//	Dur("interval", interval).
		//	Msg("memory protection background sampling started")

		for {
			select {
			case <-ticker.C:
				am.sampleMemory()
			case <-am.ctx.Done():
				return
			}
		}
	}()
}

// sampleMemory samples the current memory usage and updates the cached value
func (am *MemoryAdmissionMiddleware) sampleMemory() {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Interface("panic", r).Msg("memory sampling panicked")
		}
	}()

	now := time.Now()
	metrics.Read(am.metricsSamples)
	newUsage := am.metricsSamples[0].Value.Uint64()
	am.lastMemorySampleInBytes.Store(newUsage)
	am.timestampLastMemorySample.Store(&now)

	// Update metrics gauge
	if am.memoryLimit > 0 {
		usagePercent := float64(newUsage) / float64(am.memoryLimit) * 100
		MemoryUsageGauge.Set(usagePercent)
	}
}

func (am *MemoryAdmissionMiddleware) getLastMemorySampleInBytes() uint64 {
	return am.lastMemorySampleInBytes.Load()
}

func (am *MemoryAdmissionMiddleware) getTimestampLastMemorySample() *time.Time {
	return am.timestampLastMemorySample.Load()
}

// recordRejection records metrics for rejected requests
func (am *MemoryAdmissionMiddleware) recordRejection(fullMethod string) {
	endpointType := "api"
	if strings.HasPrefix(fullMethod, "/dispatch.v1.DispatchService") {
		endpointType = "dispatch"
	}

	RejectedRequestsCounter.WithLabelValues(endpointType).Inc()
}

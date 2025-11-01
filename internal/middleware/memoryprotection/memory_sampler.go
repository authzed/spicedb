//go:generate go run go.uber.org/mock/mockgen -source memory_sampler.go -destination ./mocks/mock_memory_sampler.go -package mocks MemorySampler
package memoryprotection

import (
	"runtime/metrics"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/authzed/spicedb/internal/logging"
)

const DefaultSampleIntervalSeconds = 1

var MemoryUsageGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: "spicedb",
	Subsystem: "memory_middleware",
	Name:      "memory_usage_percent",
	Help:      "Current memory usage as percentage of GOMEMLIMIT",
})

// MemorySampler provides memory usage sampling capabilities.
type MemorySampler interface {
	// GetMemoryUsagePercent returns the most recent memory usage (0-100)
	GetMemoryUsagePercent() float64
	// GetTimestampLastMemorySample returns the timestamp of the most recent memory sample
	GetTimestampLastMemorySample() time.Time
	// GetIntervalSeconds returns how often the sampler runs
	GetIntervalSeconds() int
	// Close clears all resources.
	Close()
}

type NoOpSampler struct{}

func (n NoOpSampler) GetIntervalSeconds() int {
	return 0
}

func (n NoOpSampler) GetMemoryUsagePercent() float64 {
	return 0
}

func (n NoOpSampler) GetTimestampLastMemorySample() time.Time {
	return time.Now()
}

func (n NoOpSampler) Close() {
}

var (
	_ MemorySampler = (*NoOpSampler)(nil)
	_ MemorySampler = (*MemorySamplerOnInterval)(nil)
)

// MemorySamplerOnInterval provides shared memory sampling for multiple middleware instances.
// It runs a single background goroutine to sample memory usage periodically, avoiding
// duplicate sampling when multiple middleware instances are created.
type MemorySamplerOnInterval struct {
	sampleIntervalSeconds int
	memoryLimitInBytes    uint64
	metricsSamples        []metrics.Sample
	stop                  chan struct{}

	usageLock                 *sync.RWMutex
	lastMemorySampleInBytes   uint64    // GUARDED_BY(usageLock)
	timestampLastMemorySample time.Time // GUARDED_BY(usageLock)
}

func (s *MemorySamplerOnInterval) GetIntervalSeconds() int {
	return s.sampleIntervalSeconds
}

// NewMemorySampler sets the current usage immediately and starts a goroutine to fetch memory usage on an interval.
// The goroutine stops when Close is called.
func NewMemorySampler(sampleIntervalSeconds int, limitProvider MemoryLimitProvider) *MemorySamplerOnInterval {
	if sampleIntervalSeconds <= 0 {
		log.Warn().Msgf("memory sampler sample interval cannot be zero or negative; using default value of %d seconds", DefaultSampleIntervalSeconds)
		sampleIntervalSeconds = DefaultSampleIntervalSeconds
	}

	sampler := &MemorySamplerOnInterval{
		stop:                      make(chan struct{}),
		sampleIntervalSeconds:     sampleIntervalSeconds,
		memoryLimitInBytes:        uint64(limitProvider.GetInBytes()), //nolint:gosec
		usageLock:                 &sync.RWMutex{},
		timestampLastMemorySample: time.Now(),
		lastMemorySampleInBytes:   0,
		metricsSamples: []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		},
	}

	// Initialize with current memory usage
	sampler.sampleMemory()

	sampler.startBackgroundSampling()

	log.Info().
		Int("sample_interval_seconds", sampleIntervalSeconds).
		Str("limit", humanize.Bytes(sampler.memoryLimitInBytes)).
		Msg("shared memory sampler initialized with background sampling")

	return sampler
}

// GetLastMemorySampleInBytes returns the most recent memory sample in bytes
func (s *MemorySamplerOnInterval) GetMemoryUsagePercent() float64 {
	s.usageLock.RLock()
	defer s.usageLock.RUnlock()
	if s.memoryLimitInBytes <= 0 {
		return 0
	}
	return ((float64(s.lastMemorySampleInBytes) / float64(s.memoryLimitInBytes)) * 100)
}

// GetTimestampLastMemorySample returns the timestamp of the most recent memory sample
func (s *MemorySamplerOnInterval) GetTimestampLastMemorySample() time.Time {
	s.usageLock.RLock()
	defer s.usageLock.RUnlock()
	return s.timestampLastMemorySample
}

// startBackgroundSampling starts a background goroutine that samples memory usage on an interval
func (s *MemorySamplerOnInterval) startBackgroundSampling() {
	interval := time.Duration(s.sampleIntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)

	go func() {
		defer ticker.Stop()
		// NOTE: this code might start running before the logger is setup, therefore we cannot log anything here
		// or we will trigger a data race

		for {
			select {
			case <-ticker.C:
				s.sampleMemory()
			case <-s.stop:
				return
			}
		}
	}()
}

// sampleMemory samples the current memory usage and timestamp and updates the gauge
func (s *MemorySamplerOnInterval) sampleMemory() {
	metrics.Read(s.metricsSamples)
	if len(s.metricsSamples) == 0 {
		log.Warn().Msg("could not get current memory usage, no metrics available")
		return
	}
	newUsage := s.metricsSamples[0].Value.Uint64()

	s.usageLock.Lock()
	s.lastMemorySampleInBytes = newUsage
	s.timestampLastMemorySample = time.Now()
	s.usageLock.Unlock()

	MemoryUsageGauge.Set(s.GetMemoryUsagePercent())
}

func (s *MemorySamplerOnInterval) Close() {
	s.stop <- struct{}{}
	log.Info().Msg("stopped memory sampler")
}

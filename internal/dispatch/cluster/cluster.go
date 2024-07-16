package cluster

import (
	"time"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	metricsEnabled        bool
	prometheusSubsystem   string
	cache                 cache.Cache[keys.DispatchCacheKey, any]
	concurrencyLimits     graph.ConcurrencyLimits
	remoteDispatchTimeout time.Duration
	dispatchChunkSize     uint16
}

// MetricsEnabled enables issuing prometheus metrics
func MetricsEnabled(enabled bool) Option {
	return func(state *optionState) {
		state.metricsEnabled = enabled
	}
}

// PrometheusSubsystem sets the subsystem name for the prometheus metrics
func PrometheusSubsystem(name string) Option {
	return func(state *optionState) {
		state.prometheusSubsystem = name
	}
}

// Cache sets the cache for the remote dispatcher.
func Cache(c cache.Cache[keys.DispatchCacheKey, any]) Option {
	return func(state *optionState) {
		state.cache = c
	}
}

// ConcurrencyLimits sets the max number of goroutines per operation
func ConcurrencyLimits(limits graph.ConcurrencyLimits) Option {
	return func(state *optionState) {
		state.concurrencyLimits = limits
	}
}

// DispatchChunkSize sets the maximum number of items to be dispatched in a single dispatch request
func DispatchChunkSize(dispatchChunkSize uint16) Option {
	return func(state *optionState) {
		state.dispatchChunkSize = dispatchChunkSize
	}
}

// RemoteDispatchTimeout sets the maximum timeout for a remote dispatch.
// Defaults to 60s (as defined in the remote dispatcher).
func RemoteDispatchTimeout(remoteDispatchTimeout time.Duration) Option {
	return func(state *optionState) {
		state.remoteDispatchTimeout = remoteDispatchTimeout
	}
}

// NewClusterDispatcher takes a dispatcher (such as one created by
// combined.NewDispatcher) and returns a cluster dispatcher suitable for use as
// the dispatcher for the dispatch grpc server.
func NewClusterDispatcher(dispatch dispatch.Dispatcher, options ...Option) (dispatch.Dispatcher, error) {
	var opts optionState
	for _, fn := range options {
		fn(&opts)
	}

	chunkSize := opts.dispatchChunkSize
	if chunkSize == 0 {
		chunkSize = 100
		log.Warn().Msgf("ClusterDispatcher: dispatchChunkSize not set, defaulting to %d", chunkSize)
	}
	clusterDispatch := graph.NewDispatcher(dispatch, opts.concurrencyLimits, opts.dispatchChunkSize)

	if opts.prometheusSubsystem == "" {
		opts.prometheusSubsystem = "dispatch"
	}

	cachingClusterDispatch, err := caching.NewCachingDispatcher(opts.cache, opts.metricsEnabled, opts.prometheusSubsystem, &keys.CanonicalKeyHandler{})
	if err != nil {
		return nil, err
	}
	cachingClusterDispatch.SetDelegate(clusterDispatch)
	return cachingClusterDispatch, nil
}

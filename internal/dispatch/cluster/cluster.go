package cluster

import (
	"time"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/cache"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	metricsEnabled        bool
	prometheusSubsystem   string
	cache                 cache.Cache
	concurrencyLimits     graph.ConcurrencyLimits
	remoteDispatchTimeout time.Duration
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
func Cache(c cache.Cache) Option {
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

	clusterDispatch := graph.NewDispatcher(dispatch, opts.concurrencyLimits)

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

package cluster

import (
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/cache"
)

const defaultConcurrencyLimit = 50

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	prometheusSubsystem string
	cacheConfig         *cache.Config
	concurrencyLimit    uint16
}

// PrometheusSubsystem sets the subsystem name for the prometheus metrics
func PrometheusSubsystem(name string) Option {
	return func(state *optionState) {
		state.prometheusSubsystem = name
	}
}

// CacheConfig sets the configuration for the local dispatcher's cache.
func CacheConfig(config *cache.Config) Option {
	return func(state *optionState) {
		state.cacheConfig = config
	}
}

// ConcurrencyLimit sets the max number of goroutines per operation
func ConcurrencyLimit(limit uint16) Option {
	return func(state *optionState) {
		state.concurrencyLimit = limit
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

	var concurrencyLimit uint16 = defaultConcurrencyLimit
	if opts.concurrencyLimit != 0 {
		concurrencyLimit = opts.concurrencyLimit
	}

	clusterDispatch := graph.NewDispatcher(dispatch, concurrencyLimit)

	if opts.prometheusSubsystem == "" {
		opts.prometheusSubsystem = "dispatch"
	}

	cachingClusterDispatch, err := caching.NewCachingDispatcher(opts.cacheConfig, opts.prometheusSubsystem, &keys.CanonicalKeyHandler{})
	if err != nil {
		return nil, err
	}
	cachingClusterDispatch.SetDelegate(clusterDispatch)
	return cachingClusterDispatch, nil
}

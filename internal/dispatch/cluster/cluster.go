package cluster

import (
	"github.com/dgraph-io/ristretto"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	prometheusSubsystem string
	cacheConfig         *ristretto.Config
}

// PrometheusSubsystem sets the subsystem name for the prometheus metrics
func PrometheusSubsystem(name string) Option {
	return func(state *optionState) {
		state.prometheusSubsystem = name
	}
}

// CacheConfig sets the configuration for the local dispatcher's cache.
func CacheConfig(config *ristretto.Config) Option {
	return func(state *optionState) {
		state.cacheConfig = config
	}
}

// NewClusterDispatcher takes a dispatcher (such as one created by
// combined.NewDispatcher) and returns a cluster dispatcher suitable for use as
// the dispatcher for the dispatch grpc server.
func NewClusterDispatcher(dispatch dispatch.Dispatcher, options ...Option) (dispatch.Dispatcher, error) {
	clusterDispatch := graph.NewDispatcher(dispatch)
	var opts optionState
	for _, fn := range options {
		fn(&opts)
	}

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

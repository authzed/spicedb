package cluster

import (
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/cache"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	prometheusSubsystem string
	cache               cache.Cache
	concurrencyLimits   graph.ConcurrencyLimits
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

// ConcurrencyLimit sets the max number of goroutines per operation
func ConcurrencyLimits(limits graph.ConcurrencyLimits) Option {
	return func(state *optionState) {
		state.concurrencyLimits = limits
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

	cachingClusterDispatch, err := caching.NewCachingDispatcher(opts.cache, opts.prometheusSubsystem, &keys.CanonicalKeyHandler{})
	if err != nil {
		return nil, err
	}
	cachingClusterDispatch.SetDelegate(clusterDispatch)
	return cachingClusterDispatch, nil
}

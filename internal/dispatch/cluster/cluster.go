package cluster

import (
	"fmt"
	"time"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	metricsEnabled               bool
	prometheusSubsystem          string
	cache                        cache.Cache[keys.DispatchCacheKey, any]
	concurrencyLimits            graph.ConcurrencyLimits
	remoteDispatchTimeout        time.Duration
	dispatchChunkSize            uint16
	caveatTypeSet                *caveattypes.TypeSet
	relationshipChunkCacheConfig *cache.Config
	relationshipChunkCache       cache.Cache[cache.StringKey, any]
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

// RelationshipChunkCacheConfig sets the cache config for LR3 relationship chunks.
func RelationshipChunkCacheConfig(config *cache.Config) Option {
	return func(state *optionState) {
		state.relationshipChunkCacheConfig = config
	}
}

// RelationshipChunkCache sets the cache for LR3 relationship chunks.
func RelationshipChunkCache(cache cache.Cache[cache.StringKey, any]) Option {
	return func(state *optionState) {
		state.relationshipChunkCache = cache
	}
}

// CaveatTypeSet sets the type set to use for caveats. If not specified, the default
// type set is used.
func CaveatTypeSet(caveatTypeSet *caveattypes.TypeSet) Option {
	return func(state *optionState) {
		state.caveatTypeSet = caveatTypeSet
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

	cts := caveattypes.TypeSetOrDefault(opts.caveatTypeSet)

	// Use provided cache or create one from config
	var relationshipChunkCache cache.Cache[cache.StringKey, any]
	if opts.relationshipChunkCache != nil {
		relationshipChunkCache = opts.relationshipChunkCache
	} else {
		// Default RelationshipChunkCacheConfig if not provided
		relationshipChunkCacheConfig := opts.relationshipChunkCacheConfig
		if relationshipChunkCacheConfig == nil {
			relationshipChunkCacheConfig = &cache.Config{
				NumCounters: 1e4,     // 10k
				MaxCost:     1 << 20, // 1MB
				DefaultTTL:  30 * time.Second,
			}
		}

		// Create cache from config
		var err error
		relationshipChunkCache, err = cache.NewStandardCache[cache.StringKey, any](relationshipChunkCacheConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create relationship chunk cache: %w", err)
		}
	}

	params := graph.DispatcherParameters{
		ConcurrencyLimits:      opts.concurrencyLimits,
		TypeSet:                cts,
		DispatchChunkSize:      opts.dispatchChunkSize,
		RelationshipChunkCache: relationshipChunkCache,
	}
	clusterDispatch, err := graph.NewDispatcher(dispatch, params)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster dispatcher: %w", err)
	}

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

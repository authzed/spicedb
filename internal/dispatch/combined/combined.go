package combined

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/dispatch/remote"
	"github.com/authzed/spicedb/internal/dispatch/singleflight"
	"github.com/authzed/spicedb/internal/grpchelpers"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	metricsEnabled                               bool
	prometheusSubsystem                          string
	upstreamAddr                                 string
	upstreamCAPath                               string
	grpcPresharedKey                             string
	grpcDialOpts                                 []grpc.DialOption
	cache                                        cache.Cache[keys.DispatchCacheKey, any]
	concurrencyLimits                            graph.ConcurrencyLimits
	remoteDispatchTimeout                        time.Duration
	secondaryUpstreamAddrs                       map[string]string
	secondaryUpstreamExprs                       map[string]string
	secondaryUpstreamMaximumPrimaryHedgingDelays map[string]string
	dispatchChunkSize                            uint16
	startingPrimaryHedgingDelay                  time.Duration
	caveatTypeSet                                *caveattypes.TypeSet
	relationshipChunkCacheConfig                 *cache.Config
	relationshipChunkCache                       cache.Cache[cache.StringKey, any]
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

// UpstreamAddr sets the optional cluster dispatching upstream address.
func UpstreamAddr(addr string) Option {
	return func(state *optionState) {
		state.upstreamAddr = addr
	}
}

// UpstreamCAPath sets the optional cluster dispatching upstream certificate
// authority.
func UpstreamCAPath(path string) Option {
	return func(state *optionState) {
		state.upstreamCAPath = path
	}
}

// SecondaryUpstreamAddrs sets a named map of upstream addresses for secondary
// dispatching.
func SecondaryUpstreamAddrs(addrs map[string]string) Option {
	return func(state *optionState) {
		state.secondaryUpstreamAddrs = addrs
	}
}

// SecondaryUpstreamExprs sets a named map from dispatch type to the associated
// CEL expression to run to determine which secondary dispatch addresses (if any)
// to use for that incoming request.
func SecondaryUpstreamExprs(addrs map[string]string) Option {
	return func(state *optionState) {
		state.secondaryUpstreamExprs = addrs
	}
}

// SecondaryMaximumPrimaryHedgingDelays sets a named map from dispatch type to the
// maximum primary hedging delay to use for that dispatch type. This is used to
// determine how long to delay a primary dispatch when invoking the secondary dispatch.
// The default is 5ms.
func SecondaryMaximumPrimaryHedgingDelays(delays map[string]string) Option {
	return func(state *optionState) {
		state.secondaryUpstreamMaximumPrimaryHedgingDelays = delays
	}
}

// GrpcPresharedKey sets the preshared key used to authenticate for optional
// cluster dispatching.
func GrpcPresharedKey(key string) Option {
	return func(state *optionState) {
		state.grpcPresharedKey = key
	}
}

// GrpcDialOpts sets the default DialOptions used for gRPC clients
// connecting to the optional cluster dispatching.
func GrpcDialOpts(opts ...grpc.DialOption) Option {
	return func(state *optionState) {
		state.grpcDialOpts = opts
	}
}

// Cache sets the cache for the dispatcher.
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

// RemoteDispatchTimeout sets the maximum timeout for a remote dispatch.
// Defaults to 60s (as defined in the remote dispatcher).
func RemoteDispatchTimeout(remoteDispatchTimeout time.Duration) Option {
	return func(state *optionState) {
		state.remoteDispatchTimeout = remoteDispatchTimeout
	}
}

// StartingPrimaryHedgingDelay sets the starting delay for primary hedging for a remote
// dispatch.
// Defaults to 0, which uses the default defined in the remote dispatcher.
func StartingPrimaryHedgingDelay(startingPrimaryHedgingDelay time.Duration) Option {
	return func(state *optionState) {
		state.startingPrimaryHedgingDelay = startingPrimaryHedgingDelay
	}
}

// CaveatTypeSet sets the type set to use for caveats. If not specified, the default
// type set is used.
func CaveatTypeSet(caveatTypeSet *caveattypes.TypeSet) Option {
	return func(state *optionState) {
		state.caveatTypeSet = caveatTypeSet
	}
}

// NewDispatcher initializes a Dispatcher that caches and redispatches
// optionally to the provided upstream.
func NewDispatcher(options ...Option) (dispatch.Dispatcher, error) {
	var opts optionState
	for _, fn := range options {
		fn(&opts)
	}
	log.Debug().Str("upstream", opts.upstreamAddr).Msg("configured combined dispatcher")

	if opts.prometheusSubsystem == "" {
		opts.prometheusSubsystem = "dispatch_client"
	}

	cachingRedispatch, err := caching.NewCachingDispatcher(opts.cache, opts.metricsEnabled, opts.prometheusSubsystem, &keys.CanonicalKeyHandler{})
	if err != nil {
		return nil, err
	}

	chunkSize := opts.dispatchChunkSize
	if chunkSize == 0 {
		chunkSize = 100
		log.Warn().Msgf("CombinedDispatcher: dispatchChunkSize not set, defaulting to %d", chunkSize)
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
		DispatchChunkSize:      chunkSize,
		RelationshipChunkCache: relationshipChunkCache,
	}
	redispatch, err := graph.NewDispatcher(cachingRedispatch, params)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatcher: %w", err)
	}
	redispatch = singleflight.New(redispatch, &keys.CanonicalKeyHandler{})

	// If an upstream is specified, create a cluster dispatcher.
	if opts.upstreamAddr != "" {
		if opts.upstreamCAPath != "" {
			customCertOpt, err := grpcutil.WithCustomCerts(grpcutil.VerifyCA, opts.upstreamCAPath)
			if err != nil {
				return nil, err
			}
			opts.grpcDialOpts = append(opts.grpcDialOpts, customCertOpt)
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpcutil.WithBearerToken(opts.grpcPresharedKey))
		} else {
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpcutil.WithInsecureBearerToken(opts.grpcPresharedKey))
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		// TODO(jschorr): this makes streaming RPCs a bit slower, so for now, we disable it. Perhaps long-term
		// we can enable it only for non-streaming rpcs.
		// opts.grpcDialOpts = append(opts.grpcDialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor("s2")))

		conn, err := grpchelpers.Dial(context.Background(), opts.upstreamAddr, opts.grpcDialOpts...)
		if err != nil {
			return nil, err
		}

		secondaryClients := make(map[string]remote.SecondaryDispatch, len(opts.secondaryUpstreamAddrs))
		for name, addr := range opts.secondaryUpstreamAddrs {
			secondaryConn, err := grpchelpers.Dial(context.Background(), addr, opts.grpcDialOpts...)
			if err != nil {
				return nil, err
			}

			maximumHedgingDelay := 5 * time.Millisecond
			if maximumHedgingDelayStr, ok := opts.secondaryUpstreamMaximumPrimaryHedgingDelays[name]; ok {
				mgd, err := time.ParseDuration(maximumHedgingDelayStr)
				if err != nil {
					return nil, fmt.Errorf("error parsing maximum primary hedging delay for secondary dispatch `%s`: %w", name, err)
				}
				if mgd <= 0 {
					return nil, fmt.Errorf("maximum primary hedging delay for secondary dispatch `%s` must be greater than 0", name)
				}
				maximumHedgingDelay = mgd
			}

			secondaryClients[name] = remote.SecondaryDispatch{
				Name:                       name,
				Client:                     v1.NewDispatchServiceClient(secondaryConn),
				MaximumPrimaryHedgingDelay: maximumHedgingDelay,
			}
		}

		secondaryExprs := make(map[string]*remote.DispatchExpr, len(opts.secondaryUpstreamExprs))
		for name, exprString := range opts.secondaryUpstreamExprs {
			parsed, err := remote.ParseDispatchExpression(name, exprString)
			if err != nil {
				return nil, fmt.Errorf("error parsing secondary dispatch expr `%s` for method `%s`: %w", exprString, name, err)
			}
			secondaryExprs[name] = parsed
		}

		re, err := remote.NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, remote.ClusterDispatcherConfig{
			KeyHandler:             &keys.CanonicalKeyHandler{},
			DispatchOverallTimeout: opts.remoteDispatchTimeout,
		}, secondaryClients, secondaryExprs, opts.startingPrimaryHedgingDelay)
		if err != nil {
			return nil, err
		}
		redispatch = singleflight.New(re, &keys.CanonicalKeyHandler{})
	}

	cachingRedispatch.SetDelegate(redispatch)

	return cachingRedispatch, nil
}

// Package combined implements a dispatcher that combines caching,
// redispatching and optional cluster dispatching.
package combined

import (
	"time"

	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/dispatch/remote"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	metricsEnabled        bool
	prometheusSubsystem   string
	upstreamAddr          string
	upstreamCAPath        string
	grpcPresharedKey      string
	grpcDialOpts          []grpc.DialOption
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

	redispatch := graph.NewDispatcher(cachingRedispatch, opts.concurrencyLimits)

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

		opts.grpcDialOpts = append(opts.grpcDialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor("s2")))

		conn, err := grpc.Dial(opts.upstreamAddr, opts.grpcDialOpts...)
		if err != nil {
			return nil, err
		}
		redispatch = remote.NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, remote.ClusterDispatcherConfig{
			KeyHandler:             &keys.CanonicalKeyHandler{},
			DispatchOverallTimeout: opts.remoteDispatchTimeout,
		})
	}

	cachingRedispatch.SetDelegate(redispatch)

	return cachingRedispatch, nil
}

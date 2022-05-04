// Package combined implements a dispatcher that combines caching,
// redispatching and optional cluster dispatching.
package combined

import (
	"os"

	"github.com/authzed/grpcutil"
	"github.com/dgraph-io/ristretto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/dispatch/remote"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// Option is a function-style option for configuring a combined Dispatcher.
type Option func(*optionState)

type optionState struct {
	prometheusSubsystem string
	upstreamAddr        string
	upstreamCAPath      string
	grpcPresharedKey    string
	grpcDialOpts        []grpc.DialOption
	cacheConfig         *ristretto.Config
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

// CacheConfig sets the configuration for the local dispatcher's cache.
func CacheConfig(config *ristretto.Config) Option {
	return func(state *optionState) {
		state.cacheConfig = config
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

	cachingRedispatch, err := caching.NewCachingDispatcher(opts.cacheConfig, opts.prometheusSubsystem, &keys.CanonicalKeyHandler{})
	if err != nil {
		return nil, err
	}

	redispatch := graph.NewDispatcher(cachingRedispatch)

	// If an upstream is specified, create a cluster dispatcher.
	if opts.upstreamAddr != "" {
		if opts.upstreamCAPath != "" {
			// Ensure that the CA path exists.
			if _, err := os.Stat(opts.upstreamCAPath); err != nil {
				return nil, err
			}
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpcutil.WithCustomCerts(opts.upstreamCAPath, grpcutil.VerifyCA))
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpcutil.WithBearerToken(opts.grpcPresharedKey))
		} else {
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpcutil.WithInsecureBearerToken(opts.grpcPresharedKey))
			opts.grpcDialOpts = append(opts.grpcDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		conn, err := grpc.Dial(opts.upstreamAddr, opts.grpcDialOpts...)
		if err != nil {
			return nil, err
		}
		redispatch = remote.NewClusterDispatcher(v1.NewDispatchServiceClient(conn), &keys.CanonicalKeyHandler{})
	}

	cachingRedispatch.SetDelegate(redispatch)

	return cachingRedispatch, nil
}

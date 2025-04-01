package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/authzed/consistent"
	"github.com/authzed/grpcutil"
	"github.com/cespare/xxhash/v2"
	"github.com/ecordell/optgen/helpers"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/cors"
	"github.com/rs/zerolog"
	"github.com/sean-/sysexits"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // enable gzip compression on all derivative servers

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/datastore/proxy/schemacaching"
	"github.com/authzed/spicedb/internal/dispatch"
	clusterdispatch "github.com/authzed/spicedb/internal/dispatch/cluster"
	combineddispatch "github.com/authzed/spicedb/internal/dispatch/combined"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/gateway"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/services"
	dispatchSvc "github.com/authzed/spicedb/internal/services/dispatch"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/cache"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ConsistentHashringBuilder is a balancer Builder that uses xxhash as the
// underlying hash for the ConsistentHashringBalancers it creates.
var ConsistentHashringBuilder = consistent.NewBuilder(xxhash.Sum64)

//go:generate go run github.com/ecordell/optgen -output zz_generated.options.go . Config
type Config struct {
	// API config
	GRPCServer             util.GRPCServerConfig `debugmap:"visible"`
	GRPCAuthFunc           grpc_auth.AuthFunc    `debugmap:"visible"`
	PresharedSecureKey     []string              `debugmap:"sensitive"`
	ShutdownGracePeriod    time.Duration         `debugmap:"visible"`
	DisableVersionResponse bool                  `debugmap:"visible"`
	ServerName             string                `debugmap:"visible"`

	// GRPC Gateway config
	HTTPGateway                    util.HTTPServerConfig `debugmap:"visible"`
	HTTPGatewayUpstreamAddr        string                `debugmap:"visible"`
	HTTPGatewayUpstreamTLSCertPath string                `debugmap:"visible"`
	HTTPGatewayCorsEnabled         bool                  `debugmap:"visible"`
	HTTPGatewayCorsAllowedOrigins  []string              `debugmap:"visible-format"`

	// Datastore
	DatastoreConfig datastorecfg.Config `debugmap:"visible"`
	Datastore       datastore.Datastore `debugmap:"visible"`

	// Datastore usage
	MaxCaveatContextSize       int `debugmap:"visible" default:"4096"`
	MaxRelationshipContextSize int `debugmap:"visible" default:"25_000"`

	// Namespace cache
	EnableExperimentalWatchableSchemaCache bool          `debugmap:"visible"`
	SchemaWatchHeartbeat                   time.Duration `debugmap:"visible"`
	NamespaceCacheConfig                   CacheConfig   `debugmap:"visible"`

	// Schema options
	SchemaPrefixesRequired bool `debugmap:"visible"`

	// Dispatch options
	DispatchServer                    util.GRPCServerConfig   `debugmap:"visible"`
	DispatchMaxDepth                  uint32                  `debugmap:"visible"`
	GlobalDispatchConcurrencyLimit    uint16                  `debugmap:"visible"`
	DispatchConcurrencyLimits         graph.ConcurrencyLimits `debugmap:"visible"`
	DispatchUpstreamAddr              string                  `debugmap:"visible"`
	DispatchUpstreamCAPath            string                  `debugmap:"visible"`
	DispatchUpstreamTimeout           time.Duration           `debugmap:"visible"`
	DispatchClientMetricsEnabled      bool                    `debugmap:"visible"`
	DispatchClientMetricsPrefix       string                  `debugmap:"visible"`
	DispatchClusterMetricsEnabled     bool                    `debugmap:"visible"`
	DispatchClusterMetricsPrefix      string                  `debugmap:"visible"`
	Dispatcher                        dispatch.Dispatcher     `debugmap:"visible"`
	DispatchHashringReplicationFactor uint16                  `debugmap:"visible"`
	DispatchHashringSpread            uint8                   `debugmap:"visible"`
	DispatchChunkSize                 uint16                  `debugmap:"visible" default:"100"`

	DispatchSecondaryUpstreamAddrs map[string]string `debugmap:"visible"`
	DispatchSecondaryUpstreamExprs map[string]string `debugmap:"visible"`
	DispatchPrimaryDelayForTesting time.Duration     `debugmap:"hidden"`

	DispatchCacheConfig        CacheConfig `debugmap:"visible"`
	ClusterDispatchCacheConfig CacheConfig `debugmap:"visible"`

	// API Behavior
	DisableV1SchemaAPI                       bool          `debugmap:"visible"`
	V1SchemaAdditiveOnly                     bool          `debugmap:"visible"`
	MaximumUpdatesPerWrite                   uint16        `debugmap:"visible"`
	MaximumPreconditionCount                 uint16        `debugmap:"visible"`
	MaxDatastoreReadPageSize                 uint64        `debugmap:"visible"`
	StreamingAPITimeout                      time.Duration `debugmap:"visible"`
	WatchHeartbeat                           time.Duration `debugmap:"visible"`
	MaxReadRelationshipsLimit                uint32        `debugmap:"visible"`
	MaxDeleteRelationshipsLimit              uint32        `debugmap:"visible"`
	MaxLookupResourcesLimit                  uint32        `debugmap:"visible"`
	MaxBulkExportRelationshipsLimit          uint32        `debugmap:"visible"`
	EnableExperimentalLookupResources        bool          `debugmap:"visible"`
	EnableExperimentalRelationshipExpiration bool          `debugmap:"visible"`
	EnableRevisionHeartbeat                  bool          `debugmap:"visible"`

	// Additional Services
	MetricsAPI util.HTTPServerConfig `debugmap:"visible"`

	// Middleware for grpc API
	UnaryMiddlewareModification     []MiddlewareModification[grpc.UnaryServerInterceptor]  `debugmap:"hidden"`
	StreamingMiddlewareModification []MiddlewareModification[grpc.StreamServerInterceptor] `debugmap:"hidden"`

	// Middleware for internal dispatch API
	DispatchUnaryMiddleware     []grpc.UnaryServerInterceptor  `debugmap:"hidden"`
	DispatchStreamingMiddleware []grpc.StreamServerInterceptor `debugmap:"hidden"`

	// Telemetry
	SilentlyDisableTelemetry bool          `debugmap:"visible"`
	TelemetryCAOverridePath  string        `debugmap:"visible"`
	TelemetryEndpoint        string        `debugmap:"visible"`
	TelemetryInterval        time.Duration `debugmap:"visible"`

	// Logs
	EnableRequestLogs  bool `debugmap:"visible"`
	EnableResponseLogs bool `debugmap:"visible"`

	// Metrics
	DisableGRPCLatencyHistogram bool `debugmap:"visible"`
}

type closeableStack struct {
	closers []func() error
}

func (c *closeableStack) AddWithError(closer func() error) {
	c.closers = append(c.closers, closer)
}

func (c *closeableStack) AddCloser(closer io.Closer) {
	if closer != nil {
		c.closers = append(c.closers, closer.Close)
	}
}

func (c *closeableStack) AddWithoutError(closer func()) {
	c.closers = append(c.closers, func() error {
		closer()
		return nil
	})
}

func (c *closeableStack) Close() error {
	var err error
	// closer in reverse order how it's expected in deferred funcs
	for i := len(c.closers) - 1; i >= 0; i-- {
		if closerErr := c.closers[i](); closerErr != nil {
			err = multierror.Append(err, closerErr)
		}
	}
	return err
}

func (c *closeableStack) CloseIfError(err error) error {
	if err != nil {
		return c.Close()
	}
	return nil
}

// Complete validates the config and fills out defaults.
// if there is no error, a completedServerConfig (with limited options for
// mutation) is returned.
func (c *Config) Complete(ctx context.Context) (RunnableServer, error) {
	closeables := closeableStack{}
	var err error
	defer func() {
		// if an error happens during the execution of Complete, all resources are cleaned up
		if closeableErr := closeables.CloseIfError(err); closeableErr != nil {
			log.Ctx(ctx).Err(closeableErr).Msg("failed to clean up resources on Config.Complete")
		}
	}()

	if len(c.PresharedSecureKey) < 1 && c.GRPCAuthFunc == nil {
		return nil, fmt.Errorf("a preshared key must be provided to authenticate API requests")
	}

	if c.GRPCAuthFunc == nil {
		log.Ctx(ctx).Trace().Int("preshared-keys-count", len(c.PresharedSecureKey)).Msg("using gRPC auth with preshared key(s)")
		for index, presharedKey := range c.PresharedSecureKey {
			if len(presharedKey) == 0 {
				return nil, fmt.Errorf("preshared key #%d is empty", index+1)
			}

			log.Ctx(ctx).Trace().Int("preshared-key-"+strconv.Itoa(index+1)+"-length", len(presharedKey)).Msg("preshared key configured")
		}

		c.GRPCAuthFunc = auth.MustRequirePresharedKey(c.PresharedSecureKey)
	} else {
		log.Ctx(ctx).Trace().Msg("using preconfigured auth function")
	}

	ds := c.Datastore
	if ds == nil {
		var err error
		c.supportOldAndNewReadReplicaConnectionPoolFlags()
		ds, err = datastorecfg.NewDatastore(context.Background(), c.DatastoreConfig.ToOption(),
			// Datastore's filter maximum ID count is set to the max size, since the number of elements to be dispatched
			// are at most the number of elements returned from a datastore query
			datastorecfg.WithFilterMaximumIDCount(c.DispatchChunkSize),
			datastorecfg.WithEnableExperimentalRelationshipExpiration(c.EnableExperimentalRelationshipExpiration),
			datastorecfg.WithEnableRevisionHeartbeat(c.EnableRevisionHeartbeat),
		)
		if err != nil {
			return nil, spiceerrors.NewTerminationErrorBuilder(fmt.Errorf("failed to create datastore: %w", err)).
				Component("datastore").
				ExitCode(sysexits.Config).
				Error()
		}
	}
	closeables.AddWithError(ds.Close)

	nscc, err := CompleteCache[cache.StringKey, schemacaching.CacheEntry](&c.NamespaceCacheConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create namespace cache: %w", err)
	}
	log.Ctx(ctx).Info().EmbedObject(nscc).Msg("configured namespace cache")

	cachingMode := schemacaching.JustInTimeCaching
	if c.EnableExperimentalWatchableSchemaCache {
		cachingMode = schemacaching.WatchIfSupported
	}

	ds = proxy.NewObservableDatastoreProxy(ds)
	ds = proxy.NewSingleflightDatastoreProxy(ds)
	ds = schemacaching.NewCachingDatastoreProxy(ds, nscc, c.DatastoreConfig.GCWindow, cachingMode, c.SchemaWatchHeartbeat)
	closeables.AddWithError(ds.Close)

	specificConcurrencyLimits := c.DispatchConcurrencyLimits
	concurrencyLimits := specificConcurrencyLimits.WithOverallDefaultLimit(c.GlobalDispatchConcurrencyLimit)

	dispatcher := c.Dispatcher
	if dispatcher == nil {
		cc, err := CompleteCache[keys.DispatchCacheKey, any](c.DispatchCacheConfig.WithRevisionParameters(
			c.DatastoreConfig.RevisionQuantization,
			c.DatastoreConfig.FollowerReadDelay,
			c.DatastoreConfig.MaxRevisionStalenessPercent,
		))
		if err != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", err)
		}
		closeables.AddWithoutError(cc.Close)
		log.Ctx(ctx).Info().EmbedObject(cc).Msg("configured dispatch cache")

		dispatchPresharedKey := ""
		if len(c.PresharedSecureKey) > 0 {
			dispatchPresharedKey = c.PresharedSecureKey[0]
		}

		hashringConfigJSON, err := (&consistent.BalancerConfig{
			ReplicationFactor: c.DispatchHashringReplicationFactor,
			Spread:            c.DispatchHashringSpread,
		}).ServiceConfigJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC hashring balancer config: %w", err)
		}

		dispatcher, err = combineddispatch.NewDispatcher(
			combineddispatch.UpstreamAddr(c.DispatchUpstreamAddr),
			combineddispatch.UpstreamCAPath(c.DispatchUpstreamCAPath),
			combineddispatch.SecondaryUpstreamAddrs(c.DispatchSecondaryUpstreamAddrs),
			combineddispatch.SecondaryUpstreamExprs(c.DispatchSecondaryUpstreamExprs),
			combineddispatch.GrpcPresharedKey(dispatchPresharedKey),
			combineddispatch.GrpcDialOpts(
				grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
				grpc.WithDefaultServiceConfig(hashringConfigJSON),
				grpc.WithChainUnaryInterceptor(
					requestid.UnaryClientInterceptor(),
				),
				grpc.WithChainStreamInterceptor(
					requestid.StreamClientInterceptor(),
				),
			),
			combineddispatch.MetricsEnabled(c.DispatchClientMetricsEnabled),
			combineddispatch.PrometheusSubsystem(c.DispatchClientMetricsPrefix),
			combineddispatch.Cache(cc),
			combineddispatch.ConcurrencyLimits(concurrencyLimits),
			combineddispatch.DispatchChunkSize(c.DispatchChunkSize),
			combineddispatch.StartingPrimaryHedgingDelay(c.DispatchPrimaryDelayForTesting),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create dispatcher: %w", err)
		}

		log.Ctx(ctx).Info().EmbedObject(concurrencyLimits).RawJSON("balancerconfig", []byte(hashringConfigJSON)).Msg("configured dispatcher")
	}
	closeables.AddWithError(dispatcher.Close)

	if len(c.DispatchUnaryMiddleware) == 0 && len(c.DispatchStreamingMiddleware) == 0 {
		if c.GRPCAuthFunc == nil {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, auth.MustRequirePresharedKey(c.PresharedSecureKey), ds, c.DisableGRPCLatencyHistogram)
		} else {
			c.DispatchUnaryMiddleware, c.DispatchStreamingMiddleware = DefaultDispatchMiddleware(log.Logger, c.GRPCAuthFunc, ds, c.DisableGRPCLatencyHistogram)
		}
	}

	var cachingClusterDispatch dispatch.Dispatcher
	if c.DispatchServer.Enabled {
		cdcc, err := CompleteCache[keys.DispatchCacheKey, any](c.ClusterDispatchCacheConfig.WithRevisionParameters(
			c.DatastoreConfig.RevisionQuantization,
			c.DatastoreConfig.FollowerReadDelay,
			c.DatastoreConfig.MaxRevisionStalenessPercent,
		))
		if err != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
		}
		log.Ctx(ctx).Info().EmbedObject(cdcc).Msg("configured cluster dispatch cache")
		closeables.AddWithoutError(cdcc.Close)

		cachingClusterDispatch, err = clusterdispatch.NewClusterDispatcher(
			dispatcher,
			clusterdispatch.MetricsEnabled(c.DispatchClusterMetricsEnabled),
			clusterdispatch.PrometheusSubsystem(c.DispatchClusterMetricsPrefix),
			clusterdispatch.Cache(cdcc),
			clusterdispatch.RemoteDispatchTimeout(c.DispatchUpstreamTimeout),
			clusterdispatch.ConcurrencyLimits(concurrencyLimits),
			clusterdispatch.DispatchChunkSize(c.DispatchChunkSize),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to configure cluster dispatch: %w", err)
		}
		closeables.AddWithError(cachingClusterDispatch.Close)
	}

	dispatchGrpcServer, err := c.DispatchServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			dispatchSvc.RegisterGrpcServices(server, cachingClusterDispatch)
		},
		grpc.ChainUnaryInterceptor(c.DispatchUnaryMiddleware...),
		grpc.ChainStreamInterceptor(c.DispatchStreamingMiddleware...),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatch gRPC server: %w", err)
	}
	closeables.AddWithoutError(dispatchGrpcServer.GracefulStop)

	datastoreFeatures, err := ds.Features(ctx)
	if err != nil {
		return nil, fmt.Errorf("error determining datastore features: %w", err)
	}

	v1SchemaServiceOption := services.V1SchemaServiceEnabled
	if c.DisableV1SchemaAPI {
		v1SchemaServiceOption = services.V1SchemaServiceDisabled
	} else if c.V1SchemaAdditiveOnly {
		v1SchemaServiceOption = services.V1SchemaServiceAdditiveOnly
	}

	watchServiceOption := services.WatchServiceEnabled
	if datastoreFeatures.Watch.Status != datastore.FeatureSupported {
		log.Ctx(ctx).Warn().Str("reason", datastoreFeatures.Watch.Reason).Msg("watch api disabled; underlying datastore does not support it")
		watchServiceOption = services.WatchServiceDisabled
	}

	serverName := c.ServerName
	if serverName == "" {
		serverName = "spicedb"
	}

	opts := MiddlewareOption{
		log.Logger,
		c.GRPCAuthFunc,
		!c.DisableVersionResponse,
		dispatcher,
		c.EnableRequestLogs,
		c.EnableResponseLogs,
		c.DisableGRPCLatencyHistogram,
		serverName,
		nil,
		nil,
	}
	opts = opts.WithDatastore(ds)

	defaultUnaryMiddlewareChain, err := DefaultUnaryMiddleware(opts)
	if err != nil {
		return nil, fmt.Errorf("error building default middlewares: %w", err)
	}

	defaultStreamingMiddlewareChain, err := DefaultStreamingMiddleware(opts)
	if err != nil {
		return nil, fmt.Errorf("error building default middlewares: %w", err)
	}

	sameMiddlewares := defaultUnaryMiddlewareChain.Names().Equal(defaultStreamingMiddlewareChain.Names())
	if !sameMiddlewares {
		return nil, fmt.Errorf("unary and streaming middlewares differ: %v / %v",
			defaultUnaryMiddlewareChain.Names().AsSlice(),
			defaultStreamingMiddlewareChain.Names().AsSlice(),
		)
	}

	unaryMiddleware, err := c.buildUnaryMiddleware(defaultUnaryMiddlewareChain)
	if err != nil {
		return nil, fmt.Errorf("error building unary middlewares: %w", err)
	}

	streamingMiddleware, err := c.buildStreamingMiddleware(defaultStreamingMiddlewareChain)
	if err != nil {
		return nil, fmt.Errorf("error building streaming middlewares: %w", err)
	}

	permSysConfig := v1svc.PermissionsServerConfig{
		MaxPreconditionsCount:           c.MaximumPreconditionCount,
		MaxUpdatesPerWrite:              c.MaximumUpdatesPerWrite,
		MaximumAPIDepth:                 c.DispatchMaxDepth,
		MaxCaveatContextSize:            c.MaxCaveatContextSize,
		MaxRelationshipContextSize:      c.MaxRelationshipContextSize,
		MaxDatastoreReadPageSize:        c.MaxDatastoreReadPageSize,
		StreamingAPITimeout:             c.StreamingAPITimeout,
		MaxReadRelationshipsLimit:       c.MaxReadRelationshipsLimit,
		MaxDeleteRelationshipsLimit:     c.MaxDeleteRelationshipsLimit,
		MaxLookupResourcesLimit:         c.MaxLookupResourcesLimit,
		MaxBulkExportRelationshipsLimit: c.MaxBulkExportRelationshipsLimit,
		DispatchChunkSize:               c.DispatchChunkSize,
		ExpiringRelationshipsEnabled:    c.EnableExperimentalRelationshipExpiration,
	}

	healthManager := health.NewHealthManager(dispatcher, ds)
	grpcServer, err := c.GRPCServer.Complete(zerolog.InfoLevel,
		func(server *grpc.Server) {
			services.RegisterGrpcServices(
				server,
				healthManager,
				dispatcher,
				v1SchemaServiceOption,
				watchServiceOption,
				permSysConfig,
				c.WatchHeartbeat,
			)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC server: %w", err)
	}
	closeables.AddWithoutError(grpcServer.GracefulStop)

	gatewayServer, gatewayCloser, err := c.initializeGateway(ctx)
	if err != nil {
		return nil, err
	}
	closeables.AddCloser(gatewayCloser)
	closeables.AddWithoutError(gatewayServer.Close)

	infoCollector, err := telemetry.SpiceDBClusterInfoCollector(ctx, "environment", c.DatastoreConfig.Engine, ds)
	if err != nil {
		log.Warn().Err(err).Msg("unable to initialize info collector")
	} else {
		if err := prometheus.Register(infoCollector); err != nil {
			log.Warn().Err(err).Msg("unable to initialize info collector")
		}
	}

	var telemetryRegistry *prometheus.Registry

	reporter := telemetry.DisabledReporter
	if c.SilentlyDisableTelemetry {
		reporter = telemetry.SilentlyDisabledReporter
	} else if c.TelemetryEndpoint != "" && c.DatastoreConfig.DisableStats {
		reporter = telemetry.DisabledReporter
	} else if c.TelemetryEndpoint != "" {
		log.Ctx(ctx).Debug().Msg("initializing telemetry collector")
		registry, err := telemetry.RegisterTelemetryCollector(c.DatastoreConfig.Engine, ds)
		if err != nil {
			log.Warn().Err(err).Msg("unable to initialize telemetry collector")
		} else {
			telemetryRegistry = registry
			reporter, err = telemetry.RemoteReporter(
				telemetryRegistry, c.TelemetryEndpoint, c.TelemetryCAOverridePath, c.TelemetryInterval,
			)
			if err != nil {
				log.Warn().Err(err).Msg("unable to initialize telemetry reporter")
				telemetryRegistry = nil
				reporter = telemetry.DisabledReporter
			}
		}
	}

	metricsServer, err := c.MetricsAPI.Complete(zerolog.InfoLevel, MetricsHandler(telemetryRegistry, c))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics server: %w", err)
	}
	closeables.AddWithoutError(metricsServer.Close)

	log.Ctx(ctx).Info().Fields(helpers.Flatten(c.DebugMap())).Msg("configuration")

	return &completedServerConfig{
		ds:                  ds,
		gRPCServer:          grpcServer,
		dispatchGRPCServer:  dispatchGrpcServer,
		gatewayServer:       gatewayServer,
		metricsServer:       metricsServer,
		unaryMiddleware:     unaryMiddleware,
		streamingMiddleware: streamingMiddleware,
		presharedKeys:       c.PresharedSecureKey,
		telemetryReporter:   reporter,
		healthManager:       healthManager,
		closeFunc:           closeables.Close,
	}, nil
}

func (c *Config) supportOldAndNewReadReplicaConnectionPoolFlags() {
	defaultReadConnPoolCfg := *datastorecfg.DefaultReadConnPool()
	if c.DatastoreConfig.ReadReplicaConnPool.MaxOpenConns == defaultReadConnPoolCfg.MaxOpenConns && c.DatastoreConfig.
		OldReadReplicaConnPool.MaxOpenConns != defaultReadConnPoolCfg.MaxOpenConns {
		c.DatastoreConfig.
			ReadReplicaConnPool.MaxOpenConns = c.DatastoreConfig.
			OldReadReplicaConnPool.MaxOpenConns
	}
	if c.DatastoreConfig.
		ReadReplicaConnPool.MinOpenConns == defaultReadConnPoolCfg.MinOpenConns && c.DatastoreConfig.
		OldReadReplicaConnPool.MinOpenConns != defaultReadConnPoolCfg.MinOpenConns {
		c.DatastoreConfig.
			ReadReplicaConnPool.MinOpenConns = c.DatastoreConfig.
			OldReadReplicaConnPool.MinOpenConns
	}
	if c.DatastoreConfig.
		ReadReplicaConnPool.MaxLifetime == defaultReadConnPoolCfg.MaxLifetime && c.DatastoreConfig.
		OldReadReplicaConnPool.MaxLifetime != defaultReadConnPoolCfg.MaxLifetime {
		c.DatastoreConfig.
			ReadReplicaConnPool.MaxLifetime = c.DatastoreConfig.
			OldReadReplicaConnPool.MaxLifetime
	}
	if c.DatastoreConfig.
		ReadReplicaConnPool.MaxLifetimeJitter == defaultReadConnPoolCfg.MaxLifetimeJitter && c.DatastoreConfig.
		OldReadReplicaConnPool.MaxLifetimeJitter != defaultReadConnPoolCfg.MaxLifetimeJitter {
		c.DatastoreConfig.
			ReadReplicaConnPool.MaxLifetimeJitter = c.DatastoreConfig.
			OldReadReplicaConnPool.MaxLifetimeJitter
	}
	if c.DatastoreConfig.
		ReadReplicaConnPool.MaxIdleTime == defaultReadConnPoolCfg.MaxIdleTime && c.DatastoreConfig.
		OldReadReplicaConnPool.MaxIdleTime != defaultReadConnPoolCfg.MaxIdleTime {
		c.DatastoreConfig.
			ReadReplicaConnPool.MaxIdleTime = c.DatastoreConfig.OldReadReplicaConnPool.MaxIdleTime
	}
	if c.DatastoreConfig.ReadReplicaConnPool.HealthCheckInterval == defaultReadConnPoolCfg.HealthCheckInterval &&
		c.DatastoreConfig.OldReadReplicaConnPool.HealthCheckInterval != defaultReadConnPoolCfg.HealthCheckInterval {
		c.DatastoreConfig.ReadReplicaConnPool.HealthCheckInterval = c.DatastoreConfig.OldReadReplicaConnPool.HealthCheckInterval
	}
}

func (c *Config) buildUnaryMiddleware(defaultMiddleware *MiddlewareChain[grpc.UnaryServerInterceptor]) ([]grpc.UnaryServerInterceptor, error) {
	chain := MiddlewareChain[grpc.UnaryServerInterceptor]{}
	if defaultMiddleware != nil {
		chain.chain = append(chain.chain, defaultMiddleware.chain...)
	}

	if err := chain.modify(c.UnaryMiddlewareModification...); err != nil {
		return nil, err
	}

	return chain.ToGRPCInterceptors(), nil
}

func (c *Config) buildStreamingMiddleware(defaultMiddleware *MiddlewareChain[grpc.StreamServerInterceptor]) ([]grpc.StreamServerInterceptor, error) {
	chain := MiddlewareChain[grpc.StreamServerInterceptor]{}
	if defaultMiddleware != nil {
		chain.chain = append(chain.chain, defaultMiddleware.chain...)
	}

	if err := chain.modify(c.StreamingMiddlewareModification...); err != nil {
		return nil, err
	}

	return chain.ToGRPCInterceptors(), nil
}

// initializeGateway Configures the gateway to serve HTTP
func (c *Config) initializeGateway(ctx context.Context) (util.RunnableHTTPServer, io.Closer, error) {
	if len(c.HTTPGatewayUpstreamAddr) == 0 {
		c.HTTPGatewayUpstreamAddr = c.GRPCServer.Address
	} else {
		log.Ctx(ctx).Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("Overriding REST gateway upstream")
	}

	if len(c.HTTPGatewayUpstreamTLSCertPath) == 0 {
		c.HTTPGatewayUpstreamTLSCertPath = c.GRPCServer.TLSCertPath
	} else {
		log.Ctx(ctx).Info().Str("cert-path", c.HTTPGatewayUpstreamTLSCertPath).Msg("Overriding REST gateway upstream TLS")
	}

	// If the requested network is a buffered one, then disable the HTTPGateway.
	if c.GRPCServer.Network == util.BufferedNetwork {
		c.HTTPGateway.HTTPEnabled = false
		gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed skipping rest gateway initialization: %w", err)
		}
		return gatewayServer, nil, nil
	}

	var gatewayHandler http.Handler
	closeableGatewayHandler, err := gateway.NewHandler(ctx, c.HTTPGatewayUpstreamAddr, c.HTTPGatewayUpstreamTLSCertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}
	gatewayHandler = closeableGatewayHandler

	if c.HTTPGatewayCorsEnabled {
		log.Ctx(ctx).Info().Strs("origins", c.HTTPGatewayCorsAllowedOrigins).Msg("Setting REST gateway CORS policy")
		gatewayHandler = cors.New(cors.Options{
			AllowedOrigins:   c.HTTPGatewayCorsAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   []string{"Authorization", "Content-Type"},
			Debug:            log.Debug().Enabled(),
		}).Handler(gatewayHandler)
	}

	if c.HTTPGateway.HTTPEnabled {
		log.Ctx(ctx).Info().Str("upstream", c.HTTPGatewayUpstreamAddr).Msg("starting REST gateway")
	}

	gatewayServer, err := c.HTTPGateway.Complete(zerolog.InfoLevel, gatewayHandler)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize rest gateway: %w", err)
	}
	return gatewayServer, closeableGatewayHandler, nil
}

// RunnableServer is a spicedb service set ready to run
type RunnableServer interface {
	Run(ctx context.Context) error
	GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error)
	DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error)
}

// completedServerConfig holds the full configuration to run a spicedb server,
// but is assumed have already been validated via `Complete()` on Config.
// It offers limited options for mutation before Run() starts the services.
type completedServerConfig struct {
	ds datastore.Datastore

	gRPCServer         util.RunnableGRPCServer
	dispatchGRPCServer util.RunnableGRPCServer
	gatewayServer      util.RunnableHTTPServer
	metricsServer      util.RunnableHTTPServer
	telemetryReporter  telemetry.Reporter
	healthManager      health.Manager

	unaryMiddleware     []grpc.UnaryServerInterceptor
	streamingMiddleware []grpc.StreamServerInterceptor
	presharedKeys       []string
	closeFunc           func() error
}

func (c *completedServerConfig) GRPCDialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if len(c.presharedKeys) == 0 {
		return c.gRPCServer.DialContext(ctx, opts...)
	}
	if c.gRPCServer.Insecure() {
		opts = append(opts, grpcutil.WithInsecureBearerToken(c.presharedKeys[0]))
	} else {
		opts = append(opts, grpcutil.WithBearerToken(c.presharedKeys[0]))
	}
	return c.gRPCServer.DialContext(ctx, opts...)
}

func (c *completedServerConfig) DispatchNetDialContext(ctx context.Context, s string) (net.Conn, error) {
	return c.dispatchGRPCServer.NetDialContext(ctx, s)
}

func (c *completedServerConfig) Run(ctx context.Context) error {
	log.Ctx(ctx).Info().Type("datastore", c.ds).Msg("running server")
	if startable := datastore.UnwrapAs[datastore.StartableDatastore](c.ds); startable != nil {
		log.Ctx(ctx).Info().Msg("Start-ing datastore")
		err := startable.Start(ctx)
		if err != nil {
			return err
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	stopOnCancelWithErr := func(stopFn func() error) func() error {
		return func() error {
			<-ctx.Done()
			return stopFn()
		}
	}

	grpcServer := c.gRPCServer.WithOpts(
		grpc.ChainUnaryInterceptor(c.unaryMiddleware...),
		grpc.ChainStreamInterceptor(c.streamingMiddleware...),
		grpc.StatsHandler(otelgrpc.NewServerHandler()))

	g.Go(c.healthManager.Checker(ctx))
	g.Go(grpcServer.Listen(ctx))
	g.Go(c.dispatchGRPCServer.Listen(ctx))
	g.Go(c.gatewayServer.ListenAndServe)
	g.Go(c.metricsServer.ListenAndServe)
	g.Go(func() error { return c.telemetryReporter(ctx) })

	g.Go(stopOnCancelWithErr(c.closeFunc))

	if err := g.Wait(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("error shutting down server")
		return err
	}

	return nil
}

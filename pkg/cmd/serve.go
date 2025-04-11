package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/runtime"
)

const PresharedKeyFlag = "grpc-preshared-key"

var (
	namespaceCacheDefaults = &server.CacheConfig{
		Name:                "namespace",
		Enabled:             true,
		Metrics:             true,
		NumCounters:         1_000,
		MaxCost:             "32MiB",
		CacheKindForTesting: "",
	}

	dispatchCacheDefaults = &server.CacheConfig{
		Name:                "dispatch",
		Enabled:             true,
		Metrics:             true,
		NumCounters:         10_000,
		MaxCost:             "30%",
		CacheKindForTesting: "",
	}

	dispatchClusterCacheDefaults = &server.CacheConfig{
		Name:                "cluster_dispatch",
		Enabled:             true,
		Metrics:             true,
		NumCounters:         100_000,
		MaxCost:             "70%",
		CacheKindForTesting: "",
	}
)

func BoldBlue(name string) string {
	return color.New(color.FgCyan, color.Bold).Sprint(name)
}

func RegisterServeFlags(cmd *cobra.Command, config *server.Config) error {
	// sets default values, but does not expose it as CLI arguments
	config.DispatchClusterMetricsEnabled = true
	config.DispatchClientMetricsEnabled = true

	nfs := cobrautil.NewNamedFlagSets(cmd)

	grpcFlagSet := nfs.FlagSet(BoldBlue("gRPC"))
	// Flags for logging
	grpcFlagSet.BoolVar(&config.EnableRequestLogs, "grpc-log-requests-enabled", false, "logs API request payloads")
	grpcFlagSet.BoolVar(&config.EnableResponseLogs, "grpc-log-responses-enabled", false, "logs API response payloads")

	// Flags for the gRPC API server
	util.RegisterGRPCServerFlags(grpcFlagSet, &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	grpcFlagSet.StringSliceVar(&config.PresharedSecureKey, PresharedKeyFlag, []string{}, "preshared key(s) to require for authenticated requests")
	grpcFlagSet.DurationVar(&config.ShutdownGracePeriod, "grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := cobra.MarkFlagRequired(grpcFlagSet, PresharedKeyFlag); err != nil {
		return fmt.Errorf("failed to mark flag as required: %w", err)
	}

	// Flags for HTTP gateway
	httpFlags := nfs.FlagSet(BoldBlue("HTTP"))
	util.RegisterHTTPServerFlags(httpFlags, &config.HTTPGateway, "http", "gateway", ":8443", false)
	httpFlags.StringVar(&config.HTTPGatewayUpstreamAddr, "http-upstream-override-addr", "", "Override the upstream to point to a different gRPC server")
	if err := httpFlags.MarkHidden("http-upstream-override-addr"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	httpFlags.StringVar(&config.HTTPGatewayUpstreamTLSCertPath, "http-upstream-override-tls-cert-path", "", "Override the upstream TLS certificate")
	if err := httpFlags.MarkHidden("http-upstream-override-tls-cert-path"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	httpFlags.BoolVar(&config.HTTPGatewayCorsEnabled, "http-cors-enabled", false, "DANGEROUS: Enable CORS on the http gateway")
	if err := httpFlags.MarkHidden("http-cors-enabled"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	httpFlags.StringSliceVar(&config.HTTPGatewayCorsAllowedOrigins, "http-cors-allowed-origins", []string{"*"}, "Set CORS allowed origins for http gateway, defaults to all origins")
	if err := httpFlags.MarkHidden("http-cors-allowed-origins"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	apiFlags := nfs.FlagSet(BoldBlue("SpiceDB API"))
	// Flags for configuring API behavior
	// In a future version these will probably be prefixed.
	apiFlags.Uint16Var(&config.MaximumPreconditionCount, "update-relationships-max-preconditions-per-call", 1000, "maximum number of preconditions allowed for WriteRelationships and DeleteRelationships calls")
	apiFlags.BoolVar(&config.DisableV1SchemaAPI, "disable-v1-schema-api", false, "disables the V1 schema API")
	if err := apiFlags.MarkHidden("disable-v1-schema-api"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	apiFlags.BoolVar(&config.DisableVersionResponse, "disable-version-response", false, "disables version response support in the API")
	apiFlags.Uint16Var(&config.MaximumUpdatesPerWrite, "write-relationships-max-updates-per-call", 1000, "maximum number of updates allowed for WriteRelationships calls")
	apiFlags.IntVar(&config.MaxCaveatContextSize, "max-caveat-context-size", 4096, "maximum allowed size of request caveat context in bytes. A value of zero or less means no limit")
	apiFlags.IntVar(&config.MaxRelationshipContextSize, "max-relationship-context-size", 25000, "maximum allowed size of the context to be stored in a relationship")
	apiFlags.DurationVar(&config.StreamingAPITimeout, "streaming-api-response-delay-timeout", 30*time.Second, "max duration time elapsed between messages sent by the server-side to the client (responses) before the stream times out")
	apiFlags.DurationVar(&config.WatchHeartbeat, "watch-api-heartbeat", 1*time.Second, "heartbeat time on the watch in the API. 0 means to default to the datastore's minimum.")
	apiFlags.Uint32Var(&config.MaxReadRelationshipsLimit, "max-read-relationships-limit", 1000, "maximum number of relationships that can be read in a single request")
	apiFlags.Uint32Var(&config.MaxDeleteRelationshipsLimit, "max-delete-relationships-limit", 1000, "maximum number of relationships that can be deleted in a single request")
	apiFlags.Uint32Var(&config.MaxLookupResourcesLimit, "max-lookup-resources-limit", 1000, "maximum number of resources that can be looked up in a single request")
	apiFlags.Uint32Var(&config.MaxBulkExportRelationshipsLimit, "max-bulk-export-relationships-limit", 10_000, "maximum number of relationships that can be exported in a single request")
	apiFlags.BoolVar(&config.EnableRevisionHeartbeat, "enable-revision-heartbeat", true, "enables support for revision heartbeat, used to create a synthetic revision on an interval defined by the quantization window (postgres only)")
	apiFlags.BoolVar(&config.EnableObjectStorage, "enable-object-storage", false, "enable object storage")

	datastoreFlags := nfs.FlagSet(BoldBlue("Datastore"))
	// Flags for the datastore
	if err := datastore.RegisterDatastoreFlags(datastoreFlags, &config.DatastoreConfig); err != nil {
		return err
	}
	datastoreFlags.Uint64Var(&config.MaxDatastoreReadPageSize, "max-datastore-read-page-size", 1_000, "limit on the maximum page size that we will load into memory from the datastore at one time")

	namespaceCacheFlags := nfs.FlagSet(BoldBlue("Namespace Cache"))
	// Flags for the namespace cache
	namespaceCacheFlags.Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")
	if err := namespaceCacheFlags.MarkHidden("ns-cache-expiration"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	namespaceCacheFlags.DurationVar(&config.SchemaWatchHeartbeat, "datastore-schema-watch-heartbeat", 1*time.Second, "heartbeat time on the schema watch in the datastore (if supported). 0 means to default to the datastore's minimum.")
	server.MustRegisterCacheFlags(namespaceCacheFlags, "ns-cache", &config.NamespaceCacheConfig, namespaceCacheDefaults)

	dispatchFlags := nfs.FlagSet(BoldBlue("Dispatch"))
	// Flags for configuring the dispatch server
	util.RegisterGRPCServerFlags(dispatchFlags, &config.DispatchServer, "dispatch-cluster", "dispatch", ":50053", false)
	server.MustRegisterCacheFlags(dispatchFlags, "dispatch-cache", &config.DispatchCacheConfig, dispatchCacheDefaults)
	server.MustRegisterCacheFlags(dispatchFlags, "dispatch-cluster-cache", &config.ClusterDispatchCacheConfig, dispatchClusterCacheDefaults)

	// Flags for configuring dispatch requests
	dispatchFlags.Uint16Var(&config.DispatchChunkSize, "dispatch-chunk-size", 100, "maximum number of object IDs in a dispatched request")
	dispatchFlags.Uint32Var(&config.DispatchMaxDepth, "dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	dispatchFlags.StringVar(&config.DispatchUpstreamAddr, "dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	dispatchFlags.StringVar(&config.DispatchUpstreamCAPath, "dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")
	dispatchFlags.DurationVar(&config.DispatchUpstreamTimeout, "dispatch-upstream-timeout", 60*time.Second, "maximum duration of a dispatch call an upstream cluster before it times out")

	dispatchFlags.Uint16Var(&config.GlobalDispatchConcurrencyLimit, "dispatch-concurrency-limit", 50, "maximum number of parallel goroutines to create for each request or subrequest")

	dispatchFlags.Uint16Var(&config.DispatchConcurrencyLimits.Check, "dispatch-check-permission-concurrency-limit", 0, "maximum number of parallel goroutines to create for each check request or subrequest. defaults to --dispatch-concurrency-limit")
	dispatchFlags.Uint16Var(&config.DispatchConcurrencyLimits.LookupResources, "dispatch-lookup-resources-concurrency-limit", 0, "maximum number of parallel goroutines to create for each lookup resources request or subrequest. defaults to --dispatch-concurrency-limit")
	dispatchFlags.Uint16Var(&config.DispatchConcurrencyLimits.LookupSubjects, "dispatch-lookup-subjects-concurrency-limit", 0, "maximum number of parallel goroutines to create for each lookup subjects request or subrequest. defaults to --dispatch-concurrency-limit")
	dispatchFlags.Uint16Var(&config.DispatchConcurrencyLimits.ReachableResources, "dispatch-reachable-resources-concurrency-limit", 0, "maximum number of parallel goroutines to create for each reachable resources request or subrequest. defaults to --dispatch-concurrency-limit")

	dispatchFlags.Uint16Var(&config.DispatchHashringReplicationFactor, "dispatch-hashring-replication-factor", 100, "set the replication factor of the consistent hasher used for the dispatcher")
	dispatchFlags.Uint8Var(&config.DispatchHashringSpread, "dispatch-hashring-spread", 1, "set the spread of the consistent hasher used for the dispatcher")

	cmd.Flags().BoolVar(&config.V1SchemaAdditiveOnly, "testing-only-schema-additive-writes", false, "append new definitions to the existing schema, rather than overwriting it")
	if err := cmd.Flags().MarkHidden("testing-only-schema-additive-writes"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	experimentalFlags := nfs.FlagSet(BoldBlue("Experimental"))
	// Flags for experimental features
	experimentalFlags.BoolVar(&config.EnableExperimentalLookupResources, "enable-experimental-lookup-resources", true, "enables the experimental version of the lookup resources API")
	if err := experimentalFlags.MarkDeprecated("enable-experimental-lookup-resources", "The old implementation of LookupResources has been removed and this flag is a no-op. This flag will be removed in the future."); err != nil {
		return fmt.Errorf("failed to mark flag as deprecated: %w", err)
	}
	if !config.EnableExperimentalLookupResources {
		log.Warn().
			Bool("value", false).
			Msg("The old implementation of LookupResources is no longer available, and a `false` value is no longer valid. Please remove this flag.")
	}

	experimentalFlags.BoolVar(&config.EnableExperimentalRelationshipExpiration, "enable-experimental-relationship-expiration", false, "enables experimental support for first-class relationship expiration")
	experimentalFlags.BoolVar(&config.EnableExperimentalWatchableSchemaCache, "enable-experimental-watchable-schema-cache", false, "enables the experimental schema cache which makes use of the Watch API for automatic updates")
	// TODO: these two could reasonably be put in either the Dispatch group or the Experimental group. Is there a preference?
	experimentalFlags.StringToStringVar(&config.DispatchSecondaryUpstreamAddrs, "experimental-dispatch-secondary-upstream-addrs", nil, "secondary upstream addresses for dispatches, each with a name")
	experimentalFlags.StringToStringVar(&config.DispatchSecondaryUpstreamExprs, "experimental-dispatch-secondary-upstream-exprs", nil, "map from request type (currently supported: `check`) to its associated CEL expression, which returns the secondary upstream(s) to be used for the request")

	observabilityFlags := nfs.FlagSet(BoldBlue("Observability"))
	// Flags for observability and profiling
	// NOTE: cobraotel.New takes service name as an arg rather than command name.
	otel := cobraotel.New("spicedb")
	otel.RegisterFlags(observabilityFlags)
	runtime.RegisterFlags(observabilityFlags)

	metricsFlags := nfs.FlagSet(BoldBlue("Metrics Server"))
	// Flags for metrics
	util.RegisterHTTPServerFlags(metricsFlags, &config.MetricsAPI, "metrics", "metrics", ":9090", true)

	telemetryFlags := nfs.FlagSet(BoldBlue("Telemetry"))
	// Flags for telemetry
	telemetryFlags.StringVar(&config.TelemetryEndpoint, "telemetry-endpoint", telemetry.DefaultEndpoint, "endpoint to which telemetry is reported, empty string to disable")
	telemetryFlags.StringVar(&config.TelemetryCAOverridePath, "telemetry-ca-override-path", "", "path to a custom CA to use with the telemetry endpoint")
	telemetryFlags.DurationVar(&config.TelemetryInterval, "telemetry-interval", telemetry.DefaultInterval, "approximate period between telemetry reports, minimum 1 minute")

	miscellaneousFlags := nfs.FlagSet(BoldBlue("Miscellaneous"))
	// Flags for things that don't neatly fit into another bucket
	termination.RegisterFlags(miscellaneousFlags)
	miscellaneousFlags.BoolVar(&config.SchemaPrefixesRequired, "schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for misc services

	if err := util.RegisterDeprecatedHTTPServerFlags(cmd, "dashboard", "dashboard"); err != nil {
		return err
	}

	// Attach the created flagsets to the command - they're created but not registered
	// until this function is called.
	nfs.AddFlagSets(cmd)

	return nil
}

func NewServeCommand(programName string, config *server.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			server, err := config.Complete(cmd.Context())
			if err != nil {
				return err
			}
			signalctx := SignalContextWithGracePeriod(
				context.Background(),
				config.ShutdownGracePeriod,
			)
			return server.Run(signalctx)
		}),
		Example: server.ServeExample(programName),
	}
}

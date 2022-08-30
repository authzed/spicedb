package cmd

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

const PresharedKeyFlag = "grpc-preshared-key"

func RegisterServeFlags(cmd *cobra.Command, config *server.Config) {
	// Flags for the gRPC API server
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	cmd.Flags().StringSliceVar(&config.PresharedKey, PresharedKeyFlag, []string{}, "preshared key(s) to require for authenticated requests")
	cmd.Flags().DurationVar(&config.ShutdownGracePeriod, "grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := cmd.MarkFlagRequired(PresharedKeyFlag); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for the datastore
	datastore.RegisterDatastoreFlags(cmd, &config.DatastoreConfig)

	// Flags for the namespace manager
	cmd.Flags().Duration("ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")
	if err := cmd.Flags().MarkHidden("ns-cache-expiration"); err != nil {
		panic("failed to mark flag hidden: " + err.Error())
	}
	server.RegisterCacheConfigFlags(cmd.Flags(), &config.NamespaceCacheConfig, "ns-cache")

	// Flags for parsing and validating schemas.
	cmd.Flags().BoolVar(&config.SchemaPrefixesRequired, "schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for HTTP gateway
	util.RegisterHTTPServerFlags(cmd.Flags(), &config.HTTPGateway, "http", "http", ":8443", false)
	cmd.Flags().StringVar(&config.HTTPGatewayUpstreamAddr, "http-upstream-override-addr", "", "Override the upstream to point to a different gRPC server")
	if err := cmd.Flags().MarkHidden("http-upstream-override-addr"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().StringVar(&config.HTTPGatewayUpstreamTLSCertPath, "http-upstream-override-tls-cert-path", "", "Override the upstream TLS certificate")
	if err := cmd.Flags().MarkHidden("http-upstream-override-tls-cert-path"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().BoolVar(&config.HTTPGatewayCorsEnabled, "http-cors-enabled", false, "DANGEROUS: Enable CORS on the http gateway")
	if err := cmd.Flags().MarkHidden("http-cors-enabled"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}
	cmd.Flags().StringSliceVar(&config.HTTPGatewayCorsAllowedOrigins, "http-cors-allowed-origins", []string{"*"}, "Set CORS allowed origins for http gateway, defaults to all origins")
	if err := cmd.Flags().MarkHidden("http-cors-allowed-origins"); err != nil {
		panic("failed to mark flag as hidden: " + err.Error())
	}

	// Flags for configuring the dispatch server
	util.RegisterGRPCServerFlags(cmd.Flags(), &config.DispatchServer, "dispatch-cluster", "dispatch", ":50053", false)
	server.RegisterCacheConfigFlags(cmd.Flags(), &config.DispatchCacheConfig, "dispatch-cache")
	server.RegisterCacheConfigFlags(cmd.Flags(), &config.ClusterDispatchCacheConfig, "dispatch-cluster-cache")

	// Flags for configuring dispatch requests
	cmd.Flags().Uint32Var(&config.DispatchMaxDepth, "dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	cmd.Flags().StringVar(&config.DispatchUpstreamAddr, "dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	cmd.Flags().StringVar(&config.DispatchUpstreamCAPath, "dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")
	cmd.Flags().Uint16Var(&config.DispatchConcurrencyLimit, "dispatch-concurrency-limit", 50, "maximum number of parallel goroutines to create for each request or subrequest")

	// Flags for configuring API behavior
	cmd.Flags().BoolVar(&config.DisableV1SchemaAPI, "disable-v1-schema-api", false, "disables the V1 schema API")
	cmd.Flags().BoolVar(&config.DisableVersionResponse, "disable-version-response", false, "disables version response support in the API")
	cmd.Flags().Uint16Var(&config.MaximumUpdatesPerWrite, "write-relationships-max-updates-per-call", 1000, "maximum number of updates allowed for WriteRelationships calls")
	cmd.Flags().Uint16Var(&config.MaximumPreconditionCount, "update-relationships-max-preconditions-per-call", 1000, "maximum number of preconditions allowed for WriteRelationships and DeleteRelationships calls")

	// Flags for misc services
	util.RegisterHTTPServerFlags(cmd.Flags(), &config.DashboardAPI, "dashboard", "dashboard", ":8080", true)
	util.RegisterHTTPServerFlags(cmd.Flags(), &config.MetricsAPI, "metrics", "metrics", ":9090", true)

	// Flags for telemetry
	cmd.Flags().StringVar(&config.TelemetryEndpoint, "telemetry-endpoint", telemetry.DefaultEndpoint, "endpoint to which telemetry is reported, empty string to disable")
	cmd.Flags().StringVar(&config.TelemetryCAOverridePath, "telemetry-ca-override-path", "", "TODO")
	cmd.Flags().DurationVar(&config.TelemetryInterval, "telemetry-interval", telemetry.DefaultInterval, "approximate period between telemetry reports, minimum 1 minute")
}

func NewServeCommand(programName string, config *server.Config) *cobra.Command {
	return &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: server.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			server, err := config.Complete()
			if err != nil {
				return err
			}
			signalctx := SignalContextWithGracePeriod(
				context.Background(),
				config.ShutdownGracePeriod,
			)
			return server.Run(signalctx)
		},
		Example: server.ServeExample(programName),
	}
}

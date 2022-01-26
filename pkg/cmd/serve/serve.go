package serve

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	cmdutil "github.com/authzed/spicedb/pkg/cmd"
)

const PresharedKeyFlag = "grpc-preshared-key"

func RegisterServeFlags(cmd *cobra.Command, config *cmdutil.ServerConfig) {
	// Flags for the gRPC API server
	cmdutil.RegisterGRPCServerFlags(cmd.Flags(), &config.GRPCServer, "grpc", "gRPC", ":50051", true)
	cmd.Flags().StringVar(&config.PresharedKey, PresharedKeyFlag, "", "preshared key to require for authenticated requests")
	cmd.Flags().DurationVar(&config.ShutdownGracePeriod, "grpc-shutdown-grace-period", 0*time.Second, "amount of time after receiving sigint to continue serving")
	if err := cmd.MarkFlagRequired(PresharedKeyFlag); err != nil {
		panic("failed to mark flag as required: " + err.Error())
	}

	// Flags for the datastore
	cmdutil.RegisterDatastoreFlags(cmd, &config.Datastore)

	// Flags for the namespace manager
	cmd.Flags().DurationVar(&config.NamespaceCacheExpiration, "ns-cache-expiration", 1*time.Minute, "amount of time a namespace entry should remain cached")

	// Flags for parsing and validating schemas.
	cmd.Flags().BoolVar(&config.SchemaPrefixesRequired, "schema-prefixes-required", false, "require prefixes on all object definitions in schemas")

	// Flags for HTTP gateway
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.HTTPGateway, "http", "http", ":8443", false)
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
	cmdutil.RegisterGRPCServerFlags(cmd.Flags(), &config.DispatchServer, "dispatch-cluster", "dispatch", ":50053", false)

	// Flags for configuring dispatch requests
	cmd.Flags().Uint32Var(&config.DispatchMaxDepth, "dispatch-max-depth", 50, "maximum recursion depth for nested calls")
	cmd.Flags().StringVar(&config.DispatchUpstreamAddr, "dispatch-upstream-addr", "", "upstream grpc address to dispatch to")
	cmd.Flags().StringVar(&config.DispatchUpstreamCAPath, "dispatch-upstream-ca-path", "", "local path to the TLS CA used when connecting to the dispatch cluster")

	// Flags for configuring API behavior
	cmd.Flags().BoolVar(&config.DisableV1SchemaAPI, "disable-v1-schema-api", false, "disables the V1 schema API")

	// Flags for misc services
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.DashboardAPI, "dashboard", "dashboard", ":8080", true)
	cmdutil.RegisterHTTPServerFlags(cmd.Flags(), &config.MetricsAPI, "metrics", "metrics", ":9090", true)
}

func NewServeCommand(programName string, config *cmdutil.ServerConfig) *cobra.Command {
	return &cobra.Command{
		Use:     "serve",
		Short:   "serve the permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		PreRunE: cmdutil.DefaultPreRunE(programName),
		RunE: func(cmd *cobra.Command, args []string) error {
			server, err := config.Complete()
			if err != nil {
				return err
			}
			signalctx := cmdutil.SignalContextWithGracePeriod(
				context.Background(),
				config.ShutdownGracePeriod,
			)
			return server.Run(signalctx)
		},
		Example: cmdutil.ServeExample(programName),
	}
}

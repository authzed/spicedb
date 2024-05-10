package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/jzelinskie/stringz"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/telemetry"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

var NamespaceCacheConfig = server.CacheConfig{
	Name:        "namespace",
	Enabled:     true,
	Metrics:     true,
	NumCounters: 1_000,
	MaxCost:     "32MiB",
}

// ServeExample creates the example usage string with the provided program name and examples
func ServeExample(programName string, examples ...string) string {
	formatted := make([]string, 0, len(examples))
	for _, example := range examples {
		formatted = append(formatted, fmt.Sprintf("  %s %s", programName, example))
	}

	return stringz.Join("\n", formatted...)
}

func ServeGRPCRunE(cfg *server.Config) cobrautil.CobraRunFunc {
	return termination.PublishError(func(cmd *cobra.Command, args []string) error {
		// Workaround for the flags that have been reworked.
		cfg.DisableVersionResponse = !cobrautil.MustGetBool(cmd, "api-version-response")
		cfg.DisableV1SchemaAPI = !cobrautil.MustGetBool(cmd, "api-schema-v1-enabled")
		cfg.GRPCServer.Network, cfg.GRPCServer.Address = ParseCombinedGRPCURI(cobrautil.MustGetStringExpanded(cmd, "grpc-addr"))
		cfg.DatastoreConfig.EnableDatastoreMetrics = true
		cfg.DatastoreConfig.GCInterval = 180000
		cfg.DatastoreConfig.GCMaxOperationTime = 60000
		cfg.DatastoreConfig.GCWindow = 86400000
		cfg.DatastoreConfig.LegacyFuzzing = -1
		cfg.NamespaceCacheConfig = NamespaceCacheConfig

		server, err := cfg.Complete(cmd.Context())
		if err != nil {
			return err
		}
		signalctx := SignalContextWithGracePeriod(
			context.Background(),
			cfg.ShutdownGracePeriod,
		)
		return server.Run(signalctx)
	})
}

func RegisterCommonServeFlags(programName string, cmd *cobra.Command, nfs *cobrautil.NamedFlagSets, cfg *server.Config, includeRemoteDispatch bool) (func() error, error) {
	if err := RegisterGRPCFlags(nfs.FlagSet("gRPC"), cfg); err != nil {
		return nil, err
	}

	if err := RegisterAPIFlags(nfs.FlagSet("API"), cfg); err != nil {
		return nil, err
	}

	dispatchFlags := nfs.FlagSet("Dispatch")
	if err := RegisterDispatchFlags(dispatchFlags, cfg); err != nil {
		return nil, err
	}

	if includeRemoteDispatch {
		if err := RegisterRemoteDispatchFlags(dispatchFlags, cfg); err != nil {
			return nil, err
		}
	}

	if err := RegisterDatastoreFlags(nfs.FlagSet("Datastore"), cfg, "memory"); err != nil {
		return nil, err
	}

	RegisterSeedFlags(nfs.FlagSet("Data Seeding"), cfg)

	obsFlags := nfs.FlagSet("Observability")
	if err := RegisterMetricsFlags(obsFlags, cfg); err != nil {
		return nil, err
	}
	otel := cobraotel.New(programName)
	otel.RegisterFlags(obsFlags)
	runtime.RegisterFlags(obsFlags) // TODO(jzelinskie): hide these?

	miscFlags := nfs.FlagSet("Miscellaneous")
	termination.RegisterFlags(miscFlags)
	releases.RegisterFlags(miscFlags)

	return func() error {
		if err := cmd.MarkFlagRequired("grpc-preshared-key"); err != nil {
			return err
		}
		if err := otel.RegisterFlagCompletion(cmd); err != nil {
			return fmt.Errorf("failed to register otel flag completion: %w", err)
		}
		return nil
	}, nil
}

func RegisterCRDBDatastoreFlags(cmd *cobra.Command, flags *pflag.FlagSet, cfg *server.Config) error {
	flags.DurationVar(&cfg.DatastoreConfig.ConnectRate, "crdb-connect-rate", 100*time.Millisecond, "max rate for new establishing new connections")
	flags.BoolVar(&cfg.DatastoreConfig.EnableConnectionBalancing, "crdb-conn-balance-enabled", true, "balance connections across discoverable database instances")
	flags.DurationVar(&cfg.DatastoreConfig.FollowerReadDelay, "crdb-follower-read-delay", 4_800*time.Millisecond, "time subtracted from revision timestamps to ensure they are beyond any replication delay")

	return cobrautil.MarkFlagsHidden(flags, "crdb-conn-balance-enabled")
}

func RegisterPostgresDatastoreFlags(cmd *cobra.Command, flags *pflag.FlagSet, cfg *server.Config) error {
	flags.StringVar(&cfg.DatastoreConfig.URI, "pg-uri", "postgres://postgres:password@localhost:5432", "connection string used to connect to PostgreSQL")
	flags.StringVar(&cfg.DatastoreConfig.CredentialsProviderName, "pg-credential-provider", "", "retrieve credentials from the environment") // TODO(jzelinskie): we should just detect and use them without any flag
	flags.DurationVar(&cfg.DatastoreConfig.GCInterval, "pg-gc-interval", 3*time.Minute, "frequency with which garbage collection runs")
	flags.DurationVar(&cfg.DatastoreConfig.GCMaxOperationTime, "pg-gc-timeout", time.Minute, "max duration for a garbage collection pass")
	flags.DurationVar(&cfg.DatastoreConfig.GCWindow, "pg-gc-window", 24*time.Hour, "max age for a revision before it can be garbage collected")
	return nil
}

func RegisterGRPCFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	flags.StringVar(&cfg.GRPCServer.Address, "grpc-addr", ":50051", "address to listen on")
	flags.DurationVar(&cfg.GRPCServer.MaxConnAge, "grpc-conn-age-limit", 30*time.Second, "max duration a connection should live")
	flags.BoolVar(&cfg.GRPCServer.Enabled, "grpc-enabled", true, "enable the gRPC server")
	// flags.StringSliceVar(&cfg.GRPCServer.GatewayAllowedOrigins, "grpc-gateway-allowed-origins", "CORS origins for the gRPC REST gateway")
	// flags.BoolVar(&cfg. "grpc-gateway-enabled", false, "enable the gRPC REST gateway")
	flags.Uint32Var(&cfg.GRPCServer.MaxWorkers, "grpc-workers", 0, "number of workers for this server (0 for 1/request)")
	flags.StringVar(&cfg.GRPCServer.TLSCertPath, "grpc-tls-cert", "", "local path to the TLS certificate")
	flags.StringVar(&cfg.GRPCServer.TLSKeyPath, "grpc-tls-key", "", "local path to the TLS key")
	flags.StringSliceVar(&cfg.PresharedSecureKey, "grpc-preshared-key", []string{}, "preshared key(s) for authenticating requests")

	return cobrautil.MarkFlagsHidden(flags, "grpc-conn-age-limit", "grpc-workers")
}

func RegisterRemoteDispatchFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	flags.BoolVar(&cfg.DispatchServer.Enabled, "dispatch-remote-enabled", true, "serve and request dispatches to/from other instances of SpiceDB")
	flags.Uint32Var(&cfg.DispatchServer.MaxWorkers, "dispatch-remote-workers", 0, "number of workers for this server (0 for 1/request)")
	flags.StringVar(&cfg.DispatchServer.Address, "dispatch-remote-addr", ":50053", "TODO")
	flags.StringVar(&cfg.DispatchServer.TLSCertPath, "dispatch-remote-tls-cert", "", "TODO")
	flags.StringVar(&cfg.DispatchServer.TLSKeyPath, "dispatch-remote-tls-key", "", "TODO")
	flags.DurationVar(&cfg.DispatchServer.MaxConnAge, "dispatch-remote-conn-age-limit", 30*time.Second, "max duration a connection should live")
	flags.DurationVar(&cfg.DispatchUpstreamTimeout, "dispatch-remote-timeout", time.Second, "max duration for a single dispatch")
	flags.StringVar(&cfg.DispatchUpstreamCAPath, "dispatch-remote-ca", "", "local path to the certificate authority used to connect for remote dispatching")

	flags.Uint16Var(&cfg.DispatchHashringReplicationFactor, "dispatch-hashring-replication", 1000, "replication factor of the consistent hash")
	flags.Uint8Var(&cfg.DispatchHashringSpread, "dispatch-hashring-spread", 1, "spread of the consistent hash")

	return cobrautil.MarkFlagsHidden(
		flags,
		"dispatch-remote-conn-age-limit",
		"dispatch-remote-workers",
		"dispatch-hashring-replication",
		"dispatch-hashring-spread",
	)
}

func RegisterDispatchFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	// flags.StringVar("dispatch-cache-limit", "10GiB", "TODO")
	// flags.StringVar("dispatch-local-cache-limit", "10GiB", "TODO")
	// flags.StringVar("dispatch-remote-cache-limit", "1GiB", "TODO")

	flags.Uint32Var(&cfg.DispatchMaxDepth, "dispatch-depth-limit", 50, "max dispatches per request")
	flags.Uint16Var(&cfg.GlobalDispatchConcurrencyLimit, "dispatch-concurrency-limit", 50, "max goroutines created per CheckPermission dispatch")

	return cobrautil.MarkFlagsHidden(flags, "dispatch-concurrency-limit")
}

func DispatchFlags(cmd *cobra.Command, cfg *server.Config) {
	limit := cobrautil.MustGetUint16(cmd, "dispatch-concurrency-limit")
	cfg.DispatchConcurrencyLimits = graph.ConcurrencyLimits{
		Check:              limit,
		ReachableResources: limit,
		LookupResources:    limit,
		LookupSubjects:     limit,
	}
}

func RegisterSeedFlags(flags *pflag.FlagSet, cfg *server.Config) {
	flags.StringSliceVar(&cfg.DatastoreConfig.BootstrapFiles, "seed", nil, "local path to YAML-formatted schema and relationships file")
	flags.BoolVar(&cfg.DatastoreConfig.BootstrapOverwrite, "seed-overwrite", false, "overwrite any existing data with the seed data")
	flags.DurationVar(&cfg.DatastoreConfig.BootstrapTimeout, "seed-timeout", 10*time.Second, "max duration writing seed data")
}

func RegisterAPIFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	// flags.Bool("api-experimental-enabled", true, "serve experimental APIs")
	flags.Bool("api-schema-v1-enabled", true, "serve the v1 schema API")
	flags.Bool("api-version-response", true, "expose the version over the API")
	flags.BoolVar(&cfg.DatastoreConfig.ReadOnly, "api-readonly", false, "prevent any data modifications")
	flags.BoolVar(&cfg.SchemaPrefixesRequired, "api-schema-prefix-required", false, "require prefixes on all object definitions")

	flags.Uint16Var(&cfg.MaximumPreconditionCount, "api-preconditions-limit", 1000, "max preconditions allowed per write and delete request")
	flags.Uint16Var(&cfg.MaximumUpdatesPerWrite, "api-updates-limit", 1000, "max updates allowed per write request")
	flags.DurationVar(&cfg.StreamingAPITimeout, "api-stream-timeout", 30*time.Second, "max duration between stream responses")

	flags.DurationVar(&cfg.WatchHeartbeat, "api-watch-heartbeat", 0, "watch API heartbeat interval (defaults to the datastore min)")
	flags.Uint16("api-watch-buffer-size", 0, "number of watch responses buffered in memory")

	flags.DurationVar(&cfg.DatastoreConfig.RevisionQuantization, "api-quantization-interval", 5*time.Second, "boundary interval with which to round the revision")
	flags.Float64Var(&cfg.DatastoreConfig.MaxRevisionStalenessPercent, "api-quantization-staleness", 0.1, "percentage of quantization interval where stale revisions can be preferred")
	flags.IntVar(&cfg.MaxCaveatContextSize, "api-caveat-context-limit", 4096, "max number of bytes for caveat context per request (<=0 is no limit")

	return cobrautil.MarkFlagsHidden(
		flags,
		"api-schema-v1-enabled",
		"api-version-response",
		"api-watch-heartbeat",
		"api-watch-buffer-size",
		"api-caveat-context-limit",
	)
}

func RegisterMetricsFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	flags.StringVar(&cfg.MetricsAPI.HTTPAddress, "metrics-addr", ":9090", "address to listen on for serving Prometheus metrics")
	flags.BoolVar(&cfg.MetricsAPI.HTTPEnabled, "metrics-enabled", true, "enable the metrics server")
	flags.StringVar(&cfg.MetricsAPI.HTTPTLSCertPath, "metrics-tls-cert", "", "local path to the TLS certificate")
	flags.StringVar(&cfg.MetricsAPI.HTTPTLSKeyPath, "metrics-tls-key", "", "local path to the TLS key")
	return nil
}

// Universal datastore flags only!
func RegisterDatastoreFlags(flags *pflag.FlagSet, cfg *server.Config, prefix string) error {
	p := func(s string) string {
		return stringz.Join("-", prefix, s)
	}

	flags.IntVar(&cfg.MaxRelationshipContextSize, p("caveat-context-limit"), 25000, "max number of bytes for caveat context per relationship")
	return cobrautil.MarkFlagsHidden(flags, p("caveat-context-limit"))
}

func RegisterTelemetryFlags(flags *pflag.FlagSet, cfg *server.Config) error {
	flags.StringVar(&cfg.TelemetryEndpoint, "telemetry-endpoint", telemetry.DefaultEndpoint, "endpoint to which telemetry is reported, empty string to disable")
	flags.StringVar(&cfg.TelemetryCAOverridePath, "telemetry-ca", "", "local path to the certificate authority used to connect to telemetry")
	flags.DurationVar(&cfg.TelemetryInterval, "telemetry-interval", telemetry.DefaultInterval, "approximate duration between telemetry reports (min 1m)")

	return cobrautil.MarkFlagsHidden(
		flags,
		"telemetry-endpoint",
		"telemetry-ca",
		"telemetry-interval",
	)
}

func ParseCombinedGRPCURI(uri string) (network, uriWithoutNetwork string) {
	before, after, found := strings.Cut(uri, "://")
	if !found {
		return uri, "tcp"
	}
	return strings.ToLower(before), after
}

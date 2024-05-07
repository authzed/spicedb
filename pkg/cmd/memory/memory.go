package memory

import (
	"fmt"
	"time"

	"github.com/fatih/color"
	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

const (
	ExampleServeWithoutTLS = `memory serve-grpc --grpc-preshared-key secretKeyHere`
	ExampleServeWithTLS    = `memory serve-grpc --grpc-preshared-key secretKeyHere --grpc-tls-cert path/to/cert --grpc-tls-key path/to/key`
)

// ServeExample creates an example usage string with the provided program name.
func ServeExample(programName string) string {
	return fmt.Sprintf("\t%[2]s:\n\t%[1]s %[3]s\n\n\t%[4]s:\n\t%[1]s %[5]s",
		programName,
		color.YellowString("Without TLS"),
		ExampleServeWithoutTLS,
		color.GreenString("With TLS"),
		ExampleServeWithTLS,
	)
}

type GRPCServerConfig struct {
	Addr           string        `debugmap:"visible"`
	ConnMaxAge     time.Duration `debugmap:"visible"` // TODO(jzelinskie): find this flag?
	Enabled        bool          `debugmap:"visible"`
	GatewayEnabled bool          `debugmap:"visible"`
	MaxWorkers     uint32        `debugmap:"visible"`
	PresharedKey   string        `debugmap:"invisible"`
	TLSCertPath    string        `debugmap:"visible"`
	TLSKeyPath     string        `debugmap:"visible"`
}

func RegisterGRPCFlags(flags *pflag.FlagSet, config *GRPCServerConfig) error {
	flags.StringVar(&config.Addr, "grpc-addr", ":50051", "address to listen on")
	flags.DurationVar(&config.ConnMaxAge, "grpc-conn-max-age", 30*time.Second, "max duration a connection should live")
	flags.BoolVar(&config.Enabled, "grpc-enabled", true, "enable the gRPC server")
	flags.BoolVar(&config.GatewayEnabled, "grpc-gateway-enabled", false, "enable the gRPC REST gateway")
	flags.Uint32Var(&config.MaxWorkers, "grpc-max-workers", 0, "number of workers for this server (defaults to 1 per request)")
	flags.StringVar(&config.TLSCertPath, "grpc-tls-cert", "", "local path to the TLS certificate")
	flags.StringVar(&config.TLSKeyPath, "grpc-tls-key", "", "local path to the TLS key")

	if err := flags.MarkHidden("grpc-conn-max-age"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}
	if err := flags.MarkHidden("grpc-max-workers"); err != nil {
		return fmt.Errorf("failed to mark flag as hidden: %w", err)
	}

	return nil
}

func ServeGRPCPreRunE() cobrautil.CobraRunFunc {
	return cobrautil.CommandStack(
		cobraotel.New("spicedb", cobraotel.WithLogger(zerologr.New(&logging.Logger))).RunE(),
		releases.CheckAndLogRunE(),
		runtime.RunE(),
	)
}

func RegisterOTELFlags(flags *pflag.FlagSet) {
}

func NewCommand(programName string) *cobra.Command {
	memCmd := &cobra.Command{
		Use:     "memory",
		Aliases: []string{"mem"},
		Short:   "Perform operations on data stored in non-persistent memory",
		GroupID: "datastores",
	}

	cfg := &GRPCServerConfig{}
	serveCmd := &cobra.Command{
		Use:     "serve-grpc",
		Short:   "Serve the SpiceDB gRPC API services",
		Example: ServeExample(programName),
		PreRunE: ServeGRPCPreRunE(),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println(cfg.Addr)
			return nil
		},
	}

	zl := cobraotel.New(programName)
	zl.RegisterFlags(serveCmd.Flags())
	if err := zl.RegisterFlagCompletion(serveCmd); err != nil {
		panic(fmt.Errorf("failed to register otel flag completion: %w", err))
	}
	termination.RegisterFlags(serveCmd.Flags())
	releases.RegisterFlags(serveCmd.Flags())
	runtime.RegisterFlags(serveCmd.Flags())
	if err := RegisterGRPCFlags(serveCmd.Flags(), cfg); err != nil {
		panic(err) // TODO
	}
	memCmd.AddCommand(serveCmd)

	return memCmd
}

package cmd

import (
	"context"
	"time"

	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/lsp"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/releases"
)

// LSPConfig is the configuration for the LSP command.
type LSPConfig struct {
	// Addr is the address to listen on to serve the language server protocol.
	Addr string

	Stdio bool
}

// Complete adapts the LSPConfig into a usable LSP server.
func (c *LSPConfig) Complete(ctx context.Context) (*lsp.Server, error) {
	return lsp.NewServer(), nil
}

func RegisterLSPFlags(cmd *cobra.Command, config *LSPConfig) error {
	cmd.Flags().StringVar(&config.Addr, "addr", "-", "address to listen on to serve LSP")
	cmd.Flags().BoolVar(&config.Stdio, "stdio", true, "enable stdio mode for LSP")
	return nil
}

func NewLSPCommand(programName string, config *LSPConfig) *cobra.Command {
	return &cobra.Command{
		Use:   "lsp",
		Short: "serve language server protocol",
		PreRunE: cobrautil.CommandStack(
			cobrautil.SyncViperDotEnvPreRunE(programName, "spicedb.env", zerologr.New(&logging.Logger)),
			cobrazerolog.New(
				cobrazerolog.WithTarget(func(logger zerolog.Logger) {
					logging.SetGlobalLogger(logger)
				}),
			).RunE(),
			releases.CheckAndLogRunE(),
		),
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			srv, err := config.Complete(cmd.Context())
			if err != nil {
				return err
			}

			signalctx := SignalContextWithGracePeriod(
				context.Background(),
				time.Second*0, // No grace period
			)

			return srv.Run(signalctx, config.Addr, config.Stdio)
		}),
	}
}

package cmd

import (
	"fmt"

	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/server"
)

func RegisterRootFlags(cmd *cobra.Command) error {
	zl := cobrazerolog.New()
	zl.RegisterFlags(cmd.PersistentFlags())
	if err := zl.RegisterFlagCompletion(cmd); err != nil {
		return fmt.Errorf("failed to register zerolog flag completion: %w", err)
	}
	return nil
}

// DeprecatedRunE wraps the RunFunc with a warning log statement.
func DeprecatedRunE(fn cobrautil.CobraRunFunc, newCmd string) cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		log.Warn().Str("newCommand", newCmd).Msg("use of deprecated command")
		return fn(cmd, args)
	}
}

func NewRootCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:           programName,
		Short:         "A modern permissions database",
		Long:          "A database that stores, computes, and validates application permissions",
		Example:       server.ServeExample(programName),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: cobrautil.CommandStack(
			cobrautil.SyncViperDotEnvPreRunE(programName, "spicedb.env", zerologr.New(&log.Logger)),
			cobrazerolog.New(
				cobrazerolog.WithTarget(func(logger zerolog.Logger) {
					log.SetGlobalLogger(logger)
				}),
				cobrazerolog.WithPreRunLevel(zerolog.DebugLevel),
			).RunE(),
		),
	}
}

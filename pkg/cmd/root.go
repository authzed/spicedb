package cmd

import (
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/spf13/cobra"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

func RegisterRootFlags(cmd *cobra.Command) {
	cobrazerolog.New().RegisterFlags(cmd.PersistentFlags())
	cobraotel.New(cmd.Use).RegisterFlags(cmd.PersistentFlags())
	releases.RegisterFlags(cmd.PersistentFlags())
	termination.RegisterFlags(cmd.PersistentFlags())
	runtime.RegisterFlags(cmd.PersistentFlags())
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
	}
}

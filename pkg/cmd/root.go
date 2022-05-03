package cmd

import (
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/releases"
)

func RegisterRootFlags(cmd *cobra.Command) {
	cobrautil.RegisterZeroLogFlags(cmd.PersistentFlags(), "log")
	cobrautil.RegisterOpenTelemetryFlags(cmd.PersistentFlags(), "otel", cmd.Use)
	releases.RegisterFlags(cmd.PersistentFlags())
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

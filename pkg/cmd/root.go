package cmd

import (
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"
)

func RegisterRootFlags(cmd *cobra.Command) {
	cobrautil.RegisterZeroLogFlags(cmd.PersistentFlags(), "log")
	cobrautil.RegisterOpenTelemetryFlags(cmd.PersistentFlags(), "otel", cmd.Use)
}

func NewRootCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     programName,
		Short:   "A modern permissions database",
		Long:    "A database that stores, computes, and validates application permissions",
		Example: server.ServeExample(programName),
	}
}

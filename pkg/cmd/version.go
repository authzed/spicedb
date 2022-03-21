package cmd

import (
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"
)

func RegisterVersionFlags(cmd *cobra.Command) {
	cobrautil.RegisterVersionFlags(cmd.Flags())
}

func NewVersionCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "displays the version of SpiceDB",
		RunE:  cobrautil.VersionRunFunc(programName),
	}
}

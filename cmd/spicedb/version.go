package main

import (
	"fmt"

	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/version"
)

func registerVersionCmd(rootCmd *cobra.Command) {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "displays the version of spicedb",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.UsageVersion(cobrautil.MustGetBool(cmd, "include-deps")))
		},
	}
	versionCmd.Flags().Bool("include-deps", false, "include versions of dependencies")
	rootCmd.AddCommand(versionCmd)
}

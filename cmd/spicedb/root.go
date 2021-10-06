package main

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:               "spicedb",
		Short:             "A modern permissions database",
		Long:              "A database that stores, computes, and validates application permissions",
		PersistentPreRunE: persistentPreRunE,
		TraverseChildren:  true,
		Example: fmt.Sprintf(`	%s:
		spicedb serve --grpc-preshared-key "somerandomkeyhere" --grpc-no-tls

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-cert-path path/to/tls/cert
	        		  --grpc-key-path path/to/tls/key --datastore-engine postgres
	        		  --datastore-conn-uri "postgres-connection-string-here"
	%s:
		spicedb serve-testing
`,
			color.YellowString("No TLS and in-memory"),
			color.GreenString("TLS and a real datastore"),
			color.CyanString("In-memory integration test server"),
		),
	}

	cobrautil.RegisterZeroLogFlags(rootCmd.PersistentFlags())
	cobrautil.RegisterOpenTelemetryFlags(rootCmd.PersistentFlags(), rootCmd.Use)

	return rootCmd
}

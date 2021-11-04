package main

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"
)

func newRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "spicedb",
		Short: "A modern permissions database",
		Long:  "A database that stores, computes, and validates application permissions",
		Example: fmt.Sprintf(`	%s:
		spicedb serve --grpc-preshared-key "somerandomkeyhere"

	%s:
		spicedb serve --grpc-preshared-key "realkeyhere" --grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
			--http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
			--datastore-engine postgres --datastore-conn-uri "postgres-connection-string-here"
	%s:
		spicedb serve-testing
`,
			color.YellowString("No TLS and in-memory"),
			color.GreenString("TLS and a real datastore"),
			color.CyanString("In-memory integration test server"),
		),
	}

	cobrautil.RegisterZeroLogFlags(rootCmd.PersistentFlags(), "log")
	cobrautil.RegisterOpenTelemetryFlags(rootCmd.PersistentFlags(), "otel", rootCmd.Use)

	return rootCmd
}

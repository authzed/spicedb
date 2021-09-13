package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
)

var persistentPreRunE = cobrautil.CommandStack(
	cobrautil.SyncViperPreRunE("spicedb"),
	cobrautil.ZeroLogPreRunE,
	cobrautil.OpenTelemetryPreRunE,
)

func main() {
	if len(os.Args) < 2 {
		d := color.New(color.FgHiWhite, color.Bold)
		d.Println("SpiceDB")
		fmt.Println("-----------------------------------------------")

		fmt.Println()
		color.White("Example Usage (%s):", color.YellowString("No TLS and in-memory"))
		fmt.Println()
		fmt.Println("\tspicedb --grpc-preshared-key \"somerandomkeyhere\" --grpc-no-tls")
		fmt.Println()

		fmt.Println()
		color.White("Example Usage (%s):", color.GreenString("With TLS and a real datastore"))
		fmt.Println()
		fmt.Println("\tspicedb --grpc-preshared-key \"realkeyhere\" --grpc-cert-path path/to/tls/cert")
		fmt.Println("\t        --grpc-key-path path/to/tls/key --datastore-engine postgres")
		fmt.Println("\t        --datastore-conn-uri \"postgres-connection-string-here\"")

		return
	}

	rootCmd := newRootCmd()
	registerMigrateCmd(rootCmd)
	registerHeadCmd(rootCmd)
	registerDeveloperServiceCmd(rootCmd)

	rootCmd.Execute()
}

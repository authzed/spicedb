package main

import (
	"github.com/jzelinskie/cobrautil"
)

var persistentPreRunE = cobrautil.CommandStack(
	cobrautil.SyncViperPreRunE("spicedb"),
	cobrautil.ZeroLogPreRunE,
	cobrautil.OpenTelemetryPreRunE,
)

func main() {
	rootCmd := newRootCmd()
	registerVersionCmd(rootCmd)
	registerServeCmd(rootCmd)
	registerMigrateCmd(rootCmd)
	registerHeadCmd(rootCmd)
	registerDeveloperServiceCmd(rootCmd)
	registerTestserverCmd(rootCmd)
	registerValidateCmd(rootCmd)

	_ = rootCmd.Execute()
}

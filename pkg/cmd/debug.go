package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/releases"
)

// redactedEnvVarMatchers defines patterns that, if found in an env var name, will
// redact the env var's value when placed into the output of `spicedb debug info`.
var redactedEnvVarMatchers = []string{"key"}

func RegisterDebugFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("datastore-engine", "", `type of datastore to initialize ("memory", "postgres", "cockroachdb", "mysql", "spanner")`)
}

func NewDebugCommand(programName string) *cobra.Command {
	debugCmd := &cobra.Command{
		Use:     "debug",
		Short:   "provides debug tooling for SpiceDB",
		PreRunE: server.DefaultPreRunE(programName),
	}

	debugCmd.AddCommand(&cobra.Command{
		Use:   "info",
		Short: "provides debug information for SpiceDB",
		RunE:  debugInfoRun(programName),
	})

	return debugCmd
}

func debugInfoRun(programName string) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		state, currentVersion, release, err := releases.CheckIsLatestVersion(ctx, releases.CurrentVersion, releases.GetLatestRelease)
		if err != nil {
			return err
		}

		versionDescription := ""
		switch state {
		case releases.UnreleasedVersion:
			versionDescription = color.RedString("This is an unreleased version of SpiceDB")

		case releases.UpdateAvailable:
			versionDescription = color.HiYellowString("An update to SpiceDB is available: %s", release.ViewURL)
		}

		datastoreEngine, err := cmd.Flags().GetString("datastore-engine")
		if err != nil {
			return err
		}

		datastoreDescription := os.ExpandEnv(datastoreEngine)
		if datastoreDescription == "" {
			datastoreDescription = "(unknown)"
		}

		environment := ""
		faint := color.New(color.Faint)
		for _, env := range os.Environ() {
			lowercase := strings.ToLower(env)
			if strings.HasPrefix(lowercase, strings.ToLower(programName)+"_") {
				pieces := strings.SplitN(lowercase, "=", 2)
				redact := false
				for _, matcher := range redactedEnvVarMatchers {
					if strings.Contains(pieces[0], matcher) {
						redact = true
						break
					}
				}

				if redact {
					environment += fmt.Sprintf(
						"  %s=%s\n",
						strings.ToUpper(pieces[0]),
						faint.Sprintf("(value of length %d)", len(pieces[1])),
					)
				} else {
					environment += fmt.Sprintf("  %s=%s\n", strings.ToUpper(pieces[0]), pieces[1])
				}
			}
		}

		bold := color.New(color.Bold)
		fmt.Printf(`%[1]s %[2]s %[3]s
%[4]s %[5]s

%[6]s
%[7]s

%[8]s
%[9]s
`,
			// Version
			bold.Sprint("Version:"),
			currentVersion,
			versionDescription,

			// Datastore
			bold.Sprint("Datastore:"),
			datastoreDescription,

			// Environment Variables
			bold.Sprint("Environment Variables:"),
			environment,

			// Dependencies
			bold.Sprint("All Dependencies:"),
			cobrautil.UsageVersion(programName, true /* include deps */),
		)
		return nil
	}
}

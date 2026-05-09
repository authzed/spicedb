package cobrautil

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/jzelinskie/stringz"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func findBuildSetting(bi *debug.BuildInfo, key string) string {
	for _, setting := range bi.Settings {
		if setting.Key == key {
			return setting.Value
		}
	}
	return ""
}

func vcsRevision(bi *debug.BuildInfo) string {
	revision := findBuildSetting(bi, "vcs.revision")
	if revision == "" {
		return ""
	}

	revision = revision[:12] // Short SHA

	if findBuildSetting(bi, "vcs.modified") == "true" {
		revision = revision + "-dirty"
	}

	return revision
}

// Version is variable that holds program's version string.
// This should be set with the follow flags to the `go build` command:
// -ldflags '-X github.com/jzelinskie/cobrautil.Version=$YOUR_VERSION_HERE'
var Version string

// VersionWithFallbacks returns a string of the program version.
// If the version wasn't set by ldflags, falls back to the VCS revision, and
// finally Go module version.
func VersionWithFallbacks(bi *debug.BuildInfo) string {
	return stringz.DefaultEmpty(stringz.DefaultEmpty(Version, vcsRevision(bi)), bi.Main.Version)
}

// UsageVersion introspects the process debug data for Go modules to return a
// version string.
func UsageVersion(programName string, includeDeps bool) string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		panic("failed to read BuildInfo because the program was compiled with Go " + runtime.Version())
	}

	version := VersionWithFallbacks(bi)

	if !includeDeps {
		if Version == "(devel)" {
			return fmt.Sprintf("%s development build (unknown exact version)", programName)
		}
		return fmt.Sprintf("%s %s", programName, version)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", bi.Path, version)
	for _, dep := range bi.Deps {
		fmt.Fprintf(&b, "\n\t%s %s", dep.Path, dep.Version)
	}
	return b.String()
}

// RegisterVersionFlags registers the flags used for the VersionRunFunc.
func RegisterVersionFlags(flags *pflag.FlagSet) {
	flags.Bool("include-deps", false, "include dependencies' versions")
}

// VersionRunFunc provides a generic implementation of a version command that
// reads its values from ldflags and the internal Go module data stored in a
// binary.
func VersionRunFunc(programName string) CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		_, err := fmt.Println(UsageVersion(programName, MustGetBool(cmd, "include-deps")))
		return err
	}
}

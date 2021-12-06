package version

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/jzelinskie/cobrautil"
	"github.com/spf13/cobra"
)

func RegisterVersionFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("include-deps", false, "include versions of dependencies")
}

func NewCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "displays the version of spicedb",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(UsageVersion(programName, cobrautil.MustGetBool(cmd, "include-deps")))
		},
	}
}

// Version is this program's version string.
var Version string

// UsageVersion introspects the process debug data for Go modules to return a
// version string.
func UsageVersion(programName string, includeDeps bool) string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		panic("failed to read BuildInfo because the program was compiled with Go " + runtime.Version())
	}

	if Version == "" {
		// The version wasn't set by ldflags, so fallback to the git sha
		Version = sha()
	}

	if Version == "" {
		// Couldn't determine git sha, fall back to build info
		// Although, this value is pretty much guaranteed to just be "(devel)".
		Version = bi.Main.Version
	}

	if !includeDeps {
		if Version == "(devel)" {
			return fmt.Sprintf("%s development build (unknown exact version)", programName)
		}
		return fmt.Sprintf("%s %s", programName, Version)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", bi.Path, Version)
	for _, dep := range bi.Deps {
		fmt.Fprintf(&b, "\n\t%s %s", dep.Path, dep.Version)
	}
	return b.String()
}

func sha() string {
	info, err := os.Stat(".git")
	if os.IsNotExist(err) {
		return ""
	}
	dotGit := info.Name()

	// handle submodule case, .git points to the real directory
	if !info.IsDir() {
		file, err := os.ReadFile(info.Name())
		if err != nil {
			return ""
		}
		dotGit = strings.TrimSpace(strings.TrimPrefix(string(file), "gitdir: "))
	}

	// head ref location
	headFile, err := os.ReadFile(filepath.Join(dotGit, "HEAD"))
	if err != nil {
		return ""
	}
	headRef := strings.TrimSpace(strings.TrimPrefix(string(headFile), "ref: "))
	sha, err := os.ReadFile(filepath.Join(dotGit, headRef))
	if err != nil {
		return ""
	}
	return string(sha)
}

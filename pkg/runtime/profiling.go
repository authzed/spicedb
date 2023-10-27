package runtime

import (
	"runtime"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// RegisterFlags adds flags for configuring profiling rates.
//
// The following flags are added:
// - "pprof-mutex-profile-rate"
// - "pprof-block-profile-rate"
func RegisterFlags(flags *pflag.FlagSet) {
	flags.Int("pprof-mutex-profile-rate", 0, "sets the mutex profile sampling rate")
	flags.Int("pprof-block-profile-rate", 0, "sets the block profile sampling rate")
}

// RunE returns a Cobra RunFunc that configures mutex and block profiles.
//
// The required flags can be added to a command by using RegisterFlags().
func RunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		if cobrautil.IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		runtime.SetMutexProfileFraction(cobrautil.MustGetInt(cmd, "pprof-mutex-profile-rate"))
		runtime.SetBlockProfileRate(cobrautil.MustGetInt(cmd, "pprof-block-profile-rate"))
		return nil
	}
}

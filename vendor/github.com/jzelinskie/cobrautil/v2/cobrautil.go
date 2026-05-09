package cobrautil

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/joho/godotenv"
	"github.com/jzelinskie/stringz"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// IsBuiltinCommand checks against a hard-coded list of the names of commands
// that cobra provides out-of-the-box.
func IsBuiltinCommand(cmd *cobra.Command) bool {
	return stringz.SliceContains([]string{
		"help [command]",
		"completion [command]",
	},
		cmd.Use,
	)
}

// SyncViperPreRunE returns a CobraRunFunc that synchronizes Viper environment
// flags with the provided prefix.
//
// Thanks to Carolyn Van Slyck: https://github.com/carolynvs/stingoftheviper
func SyncViperPreRunE(prefix string) CobraRunFunc {
	prefix = strings.ReplaceAll(strings.ToUpper(prefix), "-", "_")
	return func(cmd *cobra.Command, args []string) error {
		if IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		v := viper.New()
		v.AllowEmptyEnv(true)
		// NOTE: fixed from global singleton to local instance (upstream bug — authzed/spicedb PR fix/otel-lifecycle-native)
		v.SetEnvPrefix(prefix)

		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			suffix := strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
			_ = v.BindEnv(f.Name, prefix+"_"+suffix)

			if !f.Changed && v.IsSet(f.Name) {
				val := v.Get(f.Name)
				_ = cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
			}
		})

		return nil
	}
}

// SyncViperDotEnvPreRunE returns a CobraRunFunc that loads a .dotenv file
// before synchronizing Viper environment flags with the provided prefix.
//
// If empty, envfilePath defaults to ".env".
// The .dotenv file is loaded first before any additional Viper behavior.
func SyncViperDotEnvPreRunE(prefix, envfilePath string, l logr.Logger) CobraRunFunc {
	if err := godotenv.Load(stringz.DefaultEmpty(envfilePath, ".env")); err != nil {
		l.V(2).Info(
			"skipped loading dotenv",
			"path", envfilePath,
			"err", err,
		)
	}
	return SyncViperPreRunE(prefix)
}

// CobraRunFunc is the signature of cobra.Command RunFuncs.
type CobraRunFunc func(cmd *cobra.Command, args []string) error

// CommandStack chains together a collection of CobraCommandFuncs into one.
func CommandStack(cmdfns ...CobraRunFunc) CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		for _, cmdfn := range cmdfns {
			if err := cmdfn(cmd, args); err != nil {
				return err
			}
		}
		return nil
	}
}

// PrefixJoiner joins a list of strings with the "-" separator, including the provided prefix string
//
// example: PrefixJoiner("hi")("how", "are", "you") = "hi-how-are-you"
func PrefixJoiner(prefix string) func(...string) string {
	return func(xs ...string) string {
		return stringz.Join("-", append([]string{prefix}, xs...)...)
	}
}

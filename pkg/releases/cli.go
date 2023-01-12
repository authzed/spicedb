package releases

import (
	"context"
	"time"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

// RegisterFlags registers the flags for the CheckAndLogRunE function.
func RegisterFlags(flagset *flag.FlagSet) {
	flagset.Bool("skip-release-check", false, "if true, skips checking for new SpiceDB releases")
}

// CheckAndLogRunE is a run function that checks if the current version of SpiceDB is the latest
// and, if not, logs a warning. This check is disabled by setting --skip-release-check=false.
func CheckAndLogRunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		skipReleaseCheck := cobrautil.MustGetBool(cmd, "skip-release-check")
		if skipReleaseCheck {
			return nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		state, currentVersion, release, err := CheckIsLatestVersion(ctx, CurrentVersion, GetLatestRelease)
		if err != nil {
			log.Ctx(ctx).Warn().Str("this-version", currentVersion).Err(err).Msg("could not perform version checking; if this problem persists or to skip this check, add --skip-release-check=true")
			return nil
		}

		switch state {
		case UnreleasedVersion:
			log.Ctx(ctx).Warn().Str("version", currentVersion).Msg("not running a released version of SpiceDB")
			return nil

		case UpdateAvailable:
			log.Ctx(ctx).Warn().Str("this-version", currentVersion).Str("latest-released-version", release.Version).Msgf("this version of SpiceDB is out of date. See: %s", release.ViewURL)
			return nil

		case UpToDate:
			log.Ctx(ctx).Info().Str("latest-released-version", release.Version).Msg("this is the latest released version of SpiceDB")
			return nil

		case Unknown:
			log.Ctx(ctx).Warn().Str("unknown-released-version", release.Version).Msg("unable to check for a new SpiceDB version")
			return nil

		default:
			panic("Unknown state for CheckAndLogRunE")
		}
	}
}

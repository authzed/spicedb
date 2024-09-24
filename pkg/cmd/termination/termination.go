package termination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

const (
	terminationLogFlagName  = "termination-log-path"
	kubeTerminationLogLimit = 4096
)

// PublishError returns a new wrapping cobra run function that executes the provided argument runFunc, and
// writes to disk an error returned by the latter if it is of type termination.TerminationError.
func PublishError(runFunc cobrautil.CobraRunFunc) cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		runFuncErr := runFunc(cmd, args)
		var termErr spiceerrors.TerminationError
		if runFuncErr != nil && errors.As(runFuncErr, &termErr) {
			ctx := context.Background()
			if cmd.Context() != nil {
				ctx = cmd.Context()
			}
			terminationLogPath := cobrautil.MustGetString(cmd, terminationLogFlagName)
			if terminationLogPath == "" {
				return runFuncErr
			}
			bytes, err := json.Marshal(termErr)
			if err != nil {
				log.Ctx(ctx).Error().Err(fmt.Errorf("unable to marshall termination log: %w", err)).Msg("failed to report termination log")
				return runFuncErr
			}

			if len(bytes) > kubeTerminationLogLimit {
				log.Ctx(ctx).Warn().Msg("termination log exceeds 4096 bytes limit, metadata will be truncated")
				termErr.Metadata = nil
				bytes, err = json.Marshal(termErr)
				if err != nil {
					return runFuncErr
				}
			}

			if _, err := os.Stat(path.Dir(terminationLogPath)); os.IsNotExist(err) {
				mkdirErr := os.MkdirAll(path.Dir(terminationLogPath), 0o700) // Create your file
				if mkdirErr != nil {
					log.Ctx(ctx).Error().Err(fmt.Errorf("unable to create directory for termination log: %w", err)).Msg("failed to report termination log")
					return runFuncErr
				}
			}
			if err := os.WriteFile(terminationLogPath, bytes, 0o600); err != nil {
				log.Ctx(ctx).Error().Err(fmt.Errorf("unable to write terminationlog file: %w", err)).Msg("failed to report termination log")
			}
		}
		return runFuncErr
	}
}

// RegisterFlags registers the termination log flag
func RegisterFlags(flagset *flag.FlagSet) {
	flagset.String(terminationLogFlagName,
		"",
		"define the path to the termination log file, which contains a JSON payload to surface as reason for termination - disabled by default",
	)
}

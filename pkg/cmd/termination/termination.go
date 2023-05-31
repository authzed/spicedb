package termination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
)

const (
	defaultTerminationLogPath = "/dev/termination-log"
	terminationLogFlagName    = "termination-log-path"
	kubeTerminationLogLimit   = 4096
)

// Error represents an error that captures contextual information to make
// available on process termination. The error will be marshalled as JSON and
// serialized into a file-path specified via CLI arguments
type Error struct {
	error
	Component   string            `json:"component"`
	Timestamp   time.Time         `json:"timestamp"`
	ErrorString string            `json:"error"`
	Metadata    map[string]string `json:"metadata"`
	exitCode    int
}

// ExitCode returns the exit code to be returned on process termination
func (e Error) ExitCode() int {
	return e.exitCode
}

// ErrorBuilder is a fluent-style builder for Error
type ErrorBuilder struct {
	terminationErr Error
}

// Error returns the built termination Error
func (eb *ErrorBuilder) Error() Error {
	return eb.terminationErr
}

// Component specifies the component in SpiceDB that
func (eb *ErrorBuilder) Component(component string) *ErrorBuilder {
	eb.terminationErr.Component = component
	return eb
}

// Metadata adds a new key-value pair of metadata to the termination Error being built
func (eb *ErrorBuilder) Metadata(key, value string) *ErrorBuilder {
	eb.terminationErr.Metadata[key] = value
	return eb
}

// ExitCode defines the ExitCode to be used upon process termination. Defaults to 1 if not specified.
func (eb *ErrorBuilder) ExitCode(exitCode int) *ErrorBuilder {
	eb.terminationErr.exitCode = exitCode
	return eb
}

// Timestamp defines the time of the error. Defaults to time.Now().UTC() if not specified.
func (eb *ErrorBuilder) Timestamp(timestamp time.Time) *ErrorBuilder {
	eb.terminationErr.Timestamp = timestamp
	return eb
}

// New returns a new ErrorBuilder for a termination.Error.
func New(err error) *ErrorBuilder {
	return &ErrorBuilder{terminationErr: Error{
		error:       err,
		Component:   "unspecified",
		Timestamp:   time.Now().UTC(),
		ErrorString: err.Error(),
		Metadata:    make(map[string]string, 0),
		exitCode:    1,
	}}
}

// PublishError returns a new wrapping cobra run function that executes the provided argument runFunc, and
// writes to disk an error returned by the latter if it is of type termination.Error.
func PublishError(runFunc cobrautil.CobraRunFunc) cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		runFuncErr := runFunc(cmd, args)
		var termErr Error
		if runFuncErr != nil && errors.As(runFuncErr, &termErr) {
			ctx := context.Background()
			if cmd.Context() != nil {
				ctx = cmd.Context()
			}
			terminationLogPath := cobrautil.MustGetString(cmd, terminationLogFlagName)
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
		defaultTerminationLogPath,
		"define the path to the termination log file, which contains a JSON payload to surface as reason for termination",
	)
}

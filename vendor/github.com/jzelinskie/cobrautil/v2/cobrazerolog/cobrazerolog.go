// Package cobrahttp implements a builder for registering flags and producing
// a Cobra RunFunc that configures Zerolog.
package cobrazerolog

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jzelinskie/cobrautil/v2"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Option is function used to configure Zerolog within a Cobra RunFunc.
type Option func(*Builder)

// New creates a Cobra RunFunc Builder for ZeroLog.
func New(opts ...Option) *Builder {
	b := &Builder{
		flagPrefix:  "log",
		preRunLevel: zerolog.InfoLevel,
	}

	for _, configure := range opts {
		configure(b)
	}
	return b
}

// Builder is used to configure Zerolog via Cobra.
type Builder struct {
	flagPrefix        string
	target            func(zerolog.Logger)
	async             bool
	asyncSize         int
	asyncPollInterval time.Duration
	preRunLevel       zerolog.Level
}

func (b *Builder) prefix(s string) string {
	return cobrautil.PrefixJoiner(b.flagPrefix)(s)
}

// RegisterFlags adds flags for configuring Zerolog.
//
// The following flags are added:
// - "$PREFIX-level"
// - "$PREFIX-format"
func (b *Builder) RegisterFlags(flags *pflag.FlagSet) {
	flags.String(b.prefix("level"), "info", `verbosity of logging ("trace", "debug", "info", "warn", "error")`)
	flags.String(b.prefix("format"), "auto", `format of logs ("auto", "console", "json")`)
}

// RegisterFlagCompletion adds completion functions supported flags.
//
// The following flags are completed:
// - "$PREFIX-level"
// - "$PREFIX-format"
func (b *Builder) RegisterFlagCompletion(cmd *cobra.Command) error {
	if err := cmd.RegisterFlagCompletionFunc(b.prefix("level"), func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"trace", "debug", "info", "warn", "error"}, cobra.ShellCompDirectiveDefault
	}); err != nil {
		return err
	}

	if err := cmd.RegisterFlagCompletionFunc(b.prefix("format"), func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"auto", "console", "json"}, cobra.ShellCompDirectiveDefault
	}); err != nil {
		return err
	}

	return nil
}

// RunE returns a Cobra RunFunc that configures Zerolog.
//
// The required flags can be added to a command by using RegisterFlags().
func (b *Builder) RunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		if cobrautil.IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		var output io.Writer

		format := cobrautil.MustGetString(cmd, b.prefix("format"))
		if format == "console" || format == "auto" && isatty.IsTerminal(os.Stdout.Fd()) {
			output = zerolog.ConsoleWriter{Out: os.Stderr}
		} else {
			output = os.Stderr
		}

		if b.async {
			output = diode.NewWriter(output, 1000, 10*time.Millisecond, func(missed int) {
				fmt.Printf("Logger Dropped %d messages", missed)
			})
		}

		l := zerolog.New(output).With().Timestamp().Logger()

		level := strings.ToLower(cobrautil.MustGetString(cmd, b.prefix("level")))
		switch level {
		case "trace":
			l = l.Level(zerolog.TraceLevel)
		case "debug":
			l = l.Level(zerolog.DebugLevel)
		case "info":
			l = l.Level(zerolog.InfoLevel)
		case "warn":
			l = l.Level(zerolog.WarnLevel)
		case "error":
			l = l.Level(zerolog.ErrorLevel)
		case "fatal":
			l = l.Level(zerolog.FatalLevel)
		case "panic":
			l = l.Level(zerolog.PanicLevel)
		default:
			return fmt.Errorf("unknown log level: %s", level)
		}

		if b.target != nil {
			b.target(l)
		} else {
			log.Logger = l
		}

		l.WithLevel(b.preRunLevel).
			Str("format", format).
			Str("log_level", level).
			Str("provider", "zerolog").
			Bool("async", b.async).
			Msg("configured logging")
		return nil
	}
}

// WithFlagPrefix defines prefix used with the generated flags.
// Defaults to "log".
func WithFlagPrefix(flagPrefix string) Option {
	return func(b *Builder) { b.flagPrefix = flagPrefix }
}

// WithPreRunLevel defines the logging level used for pre-run log messages.
// Defaults to "debug".
func WithPreRunLevel(preRunLevel zerolog.Level) Option {
	return func(b *Builder) { b.preRunLevel = preRunLevel }
}

// WithAsync enables non-blocking logging.
//
// Size of the buffer and polling interval can be configured.
// Disabled by default.
func WithAsync(size int, pollInterval time.Duration) Option {
	return func(b *Builder) {
		b.async = true
		b.asyncSize = size
		b.asyncPollInterval = pollInterval
	}
}

// WithTarget callback that forwards the configured logger.
// Useful when we want to keep it in a global variable.
func WithTarget(fn func(zerolog.Logger)) Option {
	return func(b *Builder) { b.target = fn }
}

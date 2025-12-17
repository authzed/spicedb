package server

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
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/logging"
)

const (
	logLevelFlag  = "log-level"
	logFormatFlag = "log-format"

	// diodeBufferSize is the number of log messages to buffer before dropping.
	// This matches the default used by cobrazerolog.
	diodeBufferSize = 1000

	// diodePollInterval is how often the diode flushes buffered messages.
	// This matches the default used by cobrazerolog.
	diodePollInterval = 10 * time.Millisecond
)

// noCloseWriter wraps an io.Writer to satisfy io.WriteCloser without
// actually closing the underlying writer. This prevents diode.Writer.Close()
// from closing os.Stderr.
type noCloseWriter struct{ io.Writer }

// Compile-time check that noCloseWriter implements io.WriteCloser.
var _ io.WriteCloser = noCloseWriter{}

// Close is a no-op that satisfies io.Closer without closing the underlying writer.
func (noCloseWriter) Close() error { return nil }

// ConfigureLoggingRunE mirrors cobrazerolog's flag handling for --log-level and
// --log-format, but also stores the diode.Writer closer so the program can flush
// logs on shutdown.
func ConfigureLoggingRunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		if cobrautil.IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		format := cobrautil.MustGetString(cmd, logFormatFlag)
		switch format {
		case "auto", "console", "json":
			// valid formats
		default:
			return fmt.Errorf("unknown log format: %s", format)
		}

		var output io.Writer
		// Check stderr for TTY since that's where logs are written
		if format == "console" || (format == "auto" && isatty.IsTerminal(os.Stderr.Fd())) {
			output = zerolog.ConsoleWriter{Out: os.Stderr}
		} else {
			output = os.Stderr
		}

		// Ensure closing the diode doesn't close stderr.
		diodeWriter := diode.NewWriter(noCloseWriter{output}, diodeBufferSize, diodePollInterval, func(missed int) {
			fmt.Fprintf(os.Stderr, "Logger Dropped %d messages\n", missed)
		})

		l := zerolog.New(&diodeWriter).With().Timestamp().Logger()

		level := strings.ToLower(cobrautil.MustGetString(cmd, logLevelFlag))
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

		logging.SetGlobalLoggerWithCloser(l, &diodeWriter)

		l.Info().
			Str("format", format).
			Str("log_level", level).
			Str("provider", "zerolog").
			Bool("async", true).
			Msg("configured logging")

		return nil
	}
}

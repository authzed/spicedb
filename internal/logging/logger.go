package logging

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// defaultCloseTimeout is the maximum time to wait for log flushing during shutdown.
// If the underlying writer blocks longer than this, Close() will return with an error
// rather than hanging indefinitely.
const defaultCloseTimeout = 5 * time.Second

var Logger zerolog.Logger

type closerHolder struct {
	mu     sync.Mutex
	closer io.Closer // GUARDED_BY(mu)
	// closed is permanent once set to true. This prevents double-close bugs where
	// multiple shutdown paths might try to close the same closer. Once closed,
	// any new closer passed to Set() is immediately closed to prevent resource leaks.
	closed bool // GUARDED_BY(mu)
}

func (ch *closerHolder) Set(closer io.Closer) {
	var toClose io.Closer

	ch.mu.Lock()
	if ch.closed {
		toClose = closer
		ch.mu.Unlock()
		if toClose != nil {
			_ = toClose.Close()
		}
		return
	}

	// Don't close if the same closer is being set again
	if ch.closer != closer {
		toClose = ch.closer
	}
	ch.closer = closer
	ch.mu.Unlock()

	if toClose != nil {
		_ = toClose.Close()
	}
}

func (ch *closerHolder) Close() error {
	return ch.CloseWithTimeout(defaultCloseTimeout)
}

// CloseWithTimeout closes the held closer with a timeout to prevent shutdown hangs.
// If the closer doesn't complete within the timeout, an error is returned but the
// close operation may still complete in the background.
//
// NOTE: On timeout, the goroutine performing the close continues running until the
// underlying Close() completes. This is intentional for shutdown scenarios where we
// prefer to return promptly rather than block indefinitely. The goroutine will not
// leak permanently—it will terminate when Close() eventually returns (or when the
// process exits).
func (ch *closerHolder) CloseWithTimeout(timeout time.Duration) error {
	var toClose io.Closer

	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil
	}

	ch.closed = true
	toClose = ch.closer
	ch.closer = nil
	ch.mu.Unlock()

	if toClose == nil {
		return nil
	}

	// For zero or negative timeout, close synchronously
	if timeout <= 0 {
		err := toClose.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to flush logs: %v\n", err)
		}
		return err
	}

	// Use a channel to wait for close with timeout
	done := make(chan error, 1)
	go func() {
		done <- toClose.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			// Write directly to stderr since the logger may not be functional
			fmt.Fprintf(os.Stderr, "warning: failed to flush logs: %v\n", err)
		}
		return err
	case <-time.After(timeout):
		err := fmt.Errorf("timeout after %v waiting for log flush", timeout)
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
		return err
	}
}

// reset clears the closer state without closing the current closer.
// This is only for test use to restore state between test cases.
func (ch *closerHolder) reset() {
	ch.mu.Lock()
	ch.closer = nil
	ch.closed = false
	ch.mu.Unlock()
}

var globalCloser closerHolder

func init() {
	SetGlobalLogger(zerolog.Nop())
	logf.SetLogger(zerologr.New(&Logger))
}

func SetGlobalLogger(logger zerolog.Logger) {
	Logger = logger
	zerolog.DefaultContextLogger = &Logger
}

func SetGlobalLoggerWithCloser(logger zerolog.Logger, closer io.Closer) {
	// Store the closer BEFORE activating the logger to avoid a race window where
	// the logger is active but Close() wouldn't flush it (if called between the two operations).
	globalCloser.Set(closer)
	SetGlobalLogger(logger)
}

// Close flushes and releases resources owned by the globally configured logger.
// It is safe to call multiple times.
func Close() error { return globalCloser.Close() }

// ResetCloserForTesting resets the global closer state.
//
// WARNING: This is for TEST USE ONLY. Production code must NEVER call this function.
// Calling this in production will silently discard any pending log closer, potentially
// losing buffered log messages.
//
// This function exists because Close() is designed to be idempotent and final (sets
// closed=true permanently), but tests need to reset state between test cases to ensure
// isolation. Without this, a test that calls Close() would cause subsequent tests'
// SetGlobalLoggerWithCloser() calls to immediately close their new closers.
//
// Go's package visibility constraints prevent test files in other packages from
// accessing the unexported globalCloser directly, necessitating this exported helper.
func ResetCloserForTesting() {
	globalCloser.reset()
}

func With() zerolog.Context { return Logger.With() }

func Err(err error) *zerolog.Event { return Logger.Err(err) }

func Trace() *zerolog.Event { return Logger.Trace() }

func Debug() *zerolog.Event { return Logger.Debug() }

func Info() *zerolog.Event { return Logger.Info() }

func Warn() *zerolog.Event { return Logger.Warn() }

func Error() *zerolog.Event { return Logger.Error() }

func Fatal() *zerolog.Event { return Logger.Fatal() }

func WithLevel(level zerolog.Level) *zerolog.Event { return Logger.WithLevel(level) }

func Log() *zerolog.Event { return Logger.Log() }

func Ctx(ctx context.Context) *zerolog.Logger { return zerolog.Ctx(ctx) }

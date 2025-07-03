package logging

import (
	"context"

	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var Logger zerolog.Logger

func init() {
	SetGlobalLogger(zerolog.Nop())
	logf.SetLogger(zerologr.New(&Logger))
}

func SetGlobalLogger(logger zerolog.Logger) {
	Logger = logger
	zerolog.DefaultContextLogger = &Logger
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

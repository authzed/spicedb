// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package zerolog

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
)

// Compatibility check.
var _ logging.Logger = &Logger{}

// Logger is a zerolog logging adapter compatible with logging middlewares.
type Logger struct {
	zerolog.Context
}

// InterceptorLogger is a zerolog.Logger to Logger adapter.
func InterceptorLogger(logger zerolog.Logger) *Logger {
	return &Logger{logger.With()}
}

// Log implements the logging.Logger interface.
func (l *Logger) Log(lvl logging.Level, msg string) {
	cl := l.Logger()
	switch lvl {
	case logging.DEBUG:
		cl.Debug().Msg(msg)
	case logging.INFO:
		cl.Info().Msg(msg)
	case logging.WARNING:
		cl.Warn().Msg(msg)
	case logging.ERROR:
		cl.Error().Msg(msg)
	default:
		// TODO(kb): Perhaps this should be a logged warning, defaulting to ERROR to get attention
		// without interrupting code flow?
		panic(fmt.Sprintf("zerolog: unknown level %s", lvl))
	}
}

// With implements the logging.Logger interface.
func (l *Logger) With(fields ...string) logging.Logger {
	vals := make(map[string]interface{}, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		vals[fields[i]] = fields[i+1]
	}
	return InterceptorLogger(l.Fields(vals).Logger())
}

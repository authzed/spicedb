// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

// Package logging is a copy of https://github.com/grpc-ecosystem/go-grpc-middleware/tree/v2/providers/zerolog
// with race conditions removed (see grpc-ecosystem/go-grpc-middleware#487)
package logging

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
)

// Compatibility check.
var _ logging.Logger = &interceptor{}

// interceptor is a zerolog logging adapter compatible with logging middlewares.
type interceptor struct {
	zerolog.Logger
}

// InterceptorLogger is a zerolog.interceptor to interceptor adapter.
func InterceptorLogger(logger zerolog.Logger) logging.Logger {
	return &interceptor{logger}
}

// Log implements the logging.interceptor interface.
func (l *interceptor) Log(lvl logging.Level, msg string) {
	switch lvl {
	case logging.DEBUG:
		l.Debug().Msg(msg)
	case logging.INFO:
		l.Info().Msg(msg)
	case logging.WARNING:
		l.Warn().Msg(msg)
	case logging.ERROR:
		l.Error().Msg(msg)
	default:
		// TODO(kb): Perhaps this should be a logged warning, defaulting to ERROR to get attention
		// without interrupting code flow?
		panic(fmt.Sprintf("zerolog: unknown level %s", lvl))
	}
}

// With implements the logging.interceptor interface.
func (l interceptor) With(fields ...string) logging.Logger {
	vals := make(map[string]interface{}, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		vals[fields[i]] = fields[i+1]
	}
	return InterceptorLogger(l.Logger.With().Fields(vals).Logger())
}

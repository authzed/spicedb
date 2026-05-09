// Copyright (c) 2024 Alexey Mayshev and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otter

import (
	"context"
	"log/slog"
)

// Logger is the interface used to get log output from otter.
type Logger interface {
	// Warn logs a message at the warn level with an error.
	Warn(ctx context.Context, msg string, err error)
	// Error logs a message at the error level with an error.
	Error(ctx context.Context, msg string, err error)
}

type defaultLogger struct {
	log *slog.Logger
}

func newDefaultLogger() *defaultLogger {
	return &defaultLogger{
		log: slog.Default(),
	}
}

func (dl *defaultLogger) Warn(ctx context.Context, msg string, err error) {
	dl.log.WarnContext(ctx, msg, slog.Any("err", err))
}

func (dl *defaultLogger) Error(ctx context.Context, msg string, err error) {
	dl.log.ErrorContext(ctx, msg, slog.Any("err", err))
}

// NoopLogger is a stub implementation of [Logger] interface. It may be useful if error logging is not necessary.
type NoopLogger struct{}

func (nl *NoopLogger) Warn(ctx context.Context, msg string, err error)  {}
func (nl *NoopLogger) Error(ctx context.Context, msg string, err error) {}

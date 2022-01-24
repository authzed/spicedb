// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	defaultOptions = &options{
		shouldLog:         DefaultDeciderMethod,
		codeFunc:          DefaultErrorToCode,
		durationFieldFunc: DefaultDurationToFields,
		// levelFunc depends if it's client or server.
		levelFunc:       nil,
		timestampFormat: time.RFC3339,
	}
)

type options struct {
	levelFunc         CodeToLevel
	shouldLog         Decider
	codeFunc          ErrorToCode
	durationFieldFunc DurationToFields
	timestampFormat   string
}

type Option func(*options)

func evaluateServerOpt(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	optCopy.levelFunc = DefaultServerCodeToLevel
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

func evaluateClientOpt(opts []Option) *options {
	optCopy := &options{}
	*optCopy = *defaultOptions
	optCopy.levelFunc = DefaultClientCodeToLevel
	for _, o := range opts {
		o(optCopy)
	}
	return optCopy
}

// DurationToFields function defines how to produce duration fields for logging.
type DurationToFields func(duration time.Duration) Fields

// ErrorToCode function determines the error code of an error.
// This makes using custom errors with grpc middleware easier.
type ErrorToCode func(err error) codes.Code

func DefaultErrorToCode(err error) codes.Code {
	return status.Code(err)
}

// Decider function defines rules for suppressing any interceptor logs.
type Decider func(fullMethodName string, err error) Decision

// DefaultDeciderMethod is the default implementation of decider to see if you should log the call
// by default this if always true so all calls are logged.
func DefaultDeciderMethod(_ string, _ error) Decision {
	return LogStartAndFinishCall
}

// CodeToLevel function defines the mapping between gRPC return codes and interceptor log level.
type CodeToLevel func(code codes.Code) Level

// DefaultServerCodeToLevel is the helper mapper that maps gRPC return codes to log levels for server side.
func DefaultServerCodeToLevel(code codes.Code) Level {
	switch code {
	case codes.OK, codes.NotFound, codes.Canceled, codes.AlreadyExists, codes.InvalidArgument, codes.Unauthenticated:
		return INFO

	case codes.DeadlineExceeded, codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.Unavailable:
		return WARNING

	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return ERROR

	default:
		return ERROR
	}
}

// DefaultClientCodeToLevel is the helper mapper that maps gRPC return codes to log levels for client side.
func DefaultClientCodeToLevel(code codes.Code) Level {
	switch code {
	case codes.OK, codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.ResourceExhausted,
		codes.FailedPrecondition, codes.Aborted, codes.OutOfRange:
		return DEBUG
	case codes.Unknown, codes.DeadlineExceeded, codes.PermissionDenied, codes.Unauthenticated:
		return INFO
	case codes.Unimplemented, codes.Internal, codes.Unavailable, codes.DataLoss:
		return WARNING
	default:
		return INFO
	}
}

// WithDecider customizes the function for deciding if the gRPC interceptor logs should log.
func WithDecider(f Decider) Option {
	return func(o *options) {
		o.shouldLog = f
	}
}

// WithLevels customizes the function for mapping gRPC return codes and interceptor log level statements.
func WithLevels(f CodeToLevel) Option {
	return func(o *options) {
		o.levelFunc = f
	}
}

// WithCodes customizes the function for mapping errors to error codes.
func WithCodes(f ErrorToCode) Option {
	return func(o *options) {
		o.codeFunc = f
	}
}

// WithDurationField customizes the function for mapping request durations to log fields.
func WithDurationField(f DurationToFields) Option {
	return func(o *options) {
		o.durationFieldFunc = f
	}
}

// DefaultDurationToFields is the default implementation of converting request duration to a field.
var DefaultDurationToFields = DurationToTimeMillisFields

// DurationToTimeMillisFields converts the duration to milliseconds and uses the key `grpc.time_ms`.
func DurationToTimeMillisFields(duration time.Duration) Fields {
	return Fields{"grpc.time_ms", fmt.Sprintf("%v", durationToMilliseconds(duration))}
}

// DurationToDurationField uses a Duration field to log the request duration
// and leaves it up to Log's encoder settings to determine how that is output.
func DurationToDurationField(duration time.Duration) Fields {
	return Fields{"grpc.duration", duration.String()}
}

func durationToMilliseconds(duration time.Duration) float32 {
	return float32(duration.Nanoseconds()/1000) / 1000
}

// WithTimestampFormat customizes the timestamps emitted in the log fields.
func WithTimestampFormat(format string) Option {
	return func(o *options) {
		o.timestampFormat = format
	}
}

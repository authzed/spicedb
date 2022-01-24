// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
)

type reporter struct {
	interceptors.CallMeta

	ctx             context.Context
	kind            string
	startCallLogged bool

	opts   *options
	logger Logger
}

func (c *reporter) logMessage(logger Logger, err error, msg string, duration time.Duration) {
	code := c.opts.codeFunc(err)
	logger = logger.With("grpc.code", code.String())
	if err != nil {
		logger = logger.With("grpc.error", fmt.Sprintf("%v", err))
	}
	logger.With(c.opts.durationFieldFunc(duration)...).Log(c.opts.levelFunc(code), msg)
}

func (c *reporter) PostCall(err error, duration time.Duration) {
	switch c.opts.shouldLog(c.FullMethod(), err) {
	case LogFinishCall, LogStartAndFinishCall:
		if err == io.EOF {
			err = nil
		}
		c.logMessage(c.logger, err, "finished call", duration)
	default:
		return
	}
}

func (c *reporter) PostMsgSend(_ interface{}, err error, duration time.Duration) {
	if c.startCallLogged {
		return
	}
	switch c.opts.shouldLog(c.FullMethod(), err) {
	case LogStartAndFinishCall:
		c.startCallLogged = true
		c.logMessage(c.logger, err, "started call", duration)
	}
}

func (c *reporter) PostMsgReceive(_ interface{}, err error, duration time.Duration) {
	if c.startCallLogged {
		return
	}
	switch c.opts.shouldLog(c.FullMethod(), err) {
	case LogStartAndFinishCall:
		c.startCallLogged = true
		c.logMessage(c.logger, err, "started call", duration)
	}
}

func reportable(logger Logger, opts *options) interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta, isClient bool) (interceptors.Reporter, context.Context) {
		kind := KindServerFieldValue
		if isClient {
			kind = KindClientFieldValue
		}

		fields := newCommonFields(kind, c)
		if !isClient {
			if peer, ok := peer.FromContext(ctx); ok {
				fields = append(fields, "peer.address", peer.Addr.String())
			}
		}
		fields = fields.AppendUnique(ExtractFields(ctx))

		singleUseFields := []string{"grpc.start_time", time.Now().Format(opts.timestampFormat)}
		if d, ok := ctx.Deadline(); ok {
			singleUseFields = append(singleUseFields, "grpc.request.deadline", d.Format(opts.timestampFormat))
		}
		return &reporter{
			CallMeta:        c,
			ctx:             ctx,
			startCallLogged: false,
			opts:            opts,
			logger:          logger.With(fields...).With(singleUseFields...),
			kind:            kind,
		}, InjectFields(ctx, fields)
	}
}

// UnaryClientInterceptor returns a new unary client interceptor that optionally logs the execution of external gRPC calls.
// Logger will read existing and write new logging.Fields available in current context.
// See `ExtractFields` and `InjectFields` for details.
func UnaryClientInterceptor(logger Logger, opts ...Option) grpc.UnaryClientInterceptor {
	o := evaluateClientOpt(opts)
	return interceptors.UnaryClientInterceptor(reportable(logger, o))
}

// StreamClientInterceptor returns a new streaming client interceptor that optionally logs the execution of external gRPC calls.
// Logger will read existing and write new logging.Fields available in current context.
// See `ExtractFields` and `InjectFields` for details.
func StreamClientInterceptor(logger Logger, opts ...Option) grpc.StreamClientInterceptor {
	o := evaluateClientOpt(opts)
	return interceptors.StreamClientInterceptor(reportable(logger, o))
}

// UnaryServerInterceptor returns a new unary server interceptors that optionally logs endpoint handling.
// Logger will read existing and write new logging.Fields available in current context.
// See `ExtractFields` and `InjectFields` for details.
func UnaryServerInterceptor(logger Logger, opts ...Option) grpc.UnaryServerInterceptor {
	o := evaluateServerOpt(opts)
	return interceptors.UnaryServerInterceptor(reportable(logger, o))
}

// StreamServerInterceptor returns a new stream server interceptors that optionally logs endpoint handling.
// Logger will read existing and write new logging.Fields available in current context.
// See `ExtractFields` and `InjectFields` for details..
func StreamServerInterceptor(logger Logger, opts ...Option) grpc.StreamServerInterceptor {
	o := evaluateServerOpt(opts)
	return interceptors.StreamServerInterceptor(reportable(logger, o))
}

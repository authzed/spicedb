package streamtimeout

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Option configures MustStreamServerInterceptor.
type Option func(*options)

type options struct {
	exemptMethods map[string]struct{}
}

// WithExemptMethods exempts the given gRPC full method names from the streaming
// timeout entirely. Use only for methods that can legitimately go quiet for
// arbitrarily long without the stream being unhealthy, and whose handlers hold
// no per-stream resources while quiet. Prefer the default behavior, which
// bounds inactivity rather than total duration.
//
// Pass the generated <Service>_<Method>_FullMethodName constants rather than
// string literals: a literal missing the leading slash never matches, and the
// exemption silently becomes a no-op.
func WithExemptMethods(methods ...string) Option {
	return func(o *options) {
		if o.exemptMethods == nil {
			o.exemptMethods = make(map[string]struct{}, len(methods))
		}
		for _, m := range methods {
			o.exemptMethods[m] = struct{}{}
		}
	}
}

// MustStreamServerInterceptor returns a new stream server interceptor that cancels the context
// after a timeout if the stream has been inactive. Messages sent to the client and messages
// received from it both count as activity, so the timeout bounds inactivity rather than the
// total duration of the call. The timer is stopped once the stream stops making progress in
// either direction, including when the client half-closes and RecvMsg returns io.EOF.
func MustStreamServerInterceptor(timeout time.Duration, opts ...Option) grpc.StreamServerInterceptor {
	if timeout <= 0 {
		panic("timeout must be >= 0 for streaming timeout interceptor")
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if _, exempt := o.exemptMethods[info.FullMethod]; exempt {
			return handler(srv, stream)
		}
		ctx := stream.Context()
		withCancel, internalCancelFn := context.WithCancelCause(ctx)
		timer := time.AfterFunc(timeout, func() {
			internalCancelFn(spiceerrors.WithCodeAndDetailsAsError(fmt.Errorf("operation took longer than allowed %v to complete", timeout), codes.DeadlineExceeded))
		})
		wrapper := &activityWrapper{stream, withCancel, timer, timeout}
		return handler(srv, wrapper)
	}
}

type activityWrapper struct {
	grpc.ServerStream

	ctx     context.Context
	timer   *time.Timer
	timeout time.Duration
}

func (s *activityWrapper) Context() context.Context {
	return s.ctx
}

func (s *activityWrapper) SetTrailer(_ metadata.MD) {
	s.timer.Stop()
}

func (s *activityWrapper) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err != nil {
		s.timer.Stop()
	} else {
		s.timer.Reset(s.timeout)
	}
	return err
}

// RecvMsg counts a message received from the client as activity. Without this, a
// client-streaming method such as ImportBulkRelationships would be bounded by the
// total duration of the call rather than by inactivity, because its handler does
// not send anything until SendAndClose. Once the client half-closes, RecvMsg
// returns io.EOF and the timer stops, so committing the work is not bounded either.
func (s *activityWrapper) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		s.timer.Stop()
	} else {
		s.timer.Reset(s.timeout)
	}
	return err
}

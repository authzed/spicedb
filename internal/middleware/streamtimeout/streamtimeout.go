package streamtimeout

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// MustStreamServerInterceptor returns a new stream server interceptor that cancels the context
// after a timeout if no new data has been received.
func MustStreamServerInterceptor(timeout time.Duration) grpc.StreamServerInterceptor {
	if timeout <= 0 {
		panic("timeout must be >= 0 for streaming timeout interceptor")
	}

	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		withCancel, cancelFn := context.WithCancel(ctx)
		timer := time.AfterFunc(timeout, cancelFn)
		wrapper := &recvWrapper{stream, withCancel, cancelFn, timer, timeout}
		return handler(srv, wrapper)
	}
}

type recvWrapper struct {
	grpc.ServerStream

	ctx      context.Context
	cancelFn func()
	timer    *time.Timer
	timeout  time.Duration
}

func (s *recvWrapper) Context() context.Context {
	return s.ctx
}

func (s *recvWrapper) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err != nil {
		s.timer.Stop()
	} else {
		s.timer.Reset(s.timeout)
	}
	return err
}

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

// MustStreamServerInterceptor returns a new stream server interceptor that cancels the context
// after a timeout if no new data has been received.
func MustStreamServerInterceptor(timeout time.Duration) grpc.StreamServerInterceptor {
	if timeout <= 0 {
		panic("timeout must be >= 0 for streaming timeout interceptor")
	}

	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		withCancel, internalCancelFn := context.WithCancelCause(ctx)
		timer := time.AfterFunc(timeout, func() {
			internalCancelFn(spiceerrors.WithCodeAndDetailsAsError(fmt.Errorf("operation took longer than allowed %v to complete", timeout), codes.DeadlineExceeded))
		})
		wrapper := &sendWrapper{stream, withCancel, timer, timeout}
		return handler(srv, wrapper)
	}
}

type sendWrapper struct {
	grpc.ServerStream

	ctx     context.Context
	timer   *time.Timer
	timeout time.Duration
}

func (s *sendWrapper) Context() context.Context {
	return s.ctx
}

func (s *sendWrapper) SetTrailer(_ metadata.MD) {
	s.timer.Stop()
}

func (s *sendWrapper) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err != nil {
		s.timer.Stop()
	} else {
		s.timer.Reset(s.timeout)
	}
	return err
}

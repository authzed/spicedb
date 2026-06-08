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

// WithExemptMethods exempts the given gRPC full method names (e.g.
// "/authzed.api.v1.PermissionsService/ExportBulkRelationships") from the
// streaming timeout. Use for methods where long server-side gaps between
// sends are expected by design — bulk export, bulk import — and where the
// interceptor's "client has hung up" purpose does not apply.
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
// after a timeout if no new data has been received.
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

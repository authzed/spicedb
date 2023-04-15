package handwrittenvalidation

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handwrittenValidator interface {
	HandwrittenValidate() error
}

// UnaryServerInterceptor returns a new unary server interceptor that runs the handwritten validation
// on the incoming request, if any.
func UnaryServerInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	validator, ok := req.(handwrittenValidator)
	if ok {
		err := validator.HandwrittenValidate()
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%s", err)
		}
	}

	return handler(ctx, req)
}

// StreamServerInterceptor returns a new stream server interceptor that runs the handwritten validation
// on the incoming request messages, if any.
func StreamServerInterceptor(srv interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	wrapper := &recvWrapper{stream}
	return handler(srv, wrapper)
}

type recvWrapper struct {
	grpc.ServerStream
}

func (s *recvWrapper) RecvMsg(m interface{}) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	validator, ok := m.(handwrittenValidator)
	if ok {
		err := validator.HandwrittenValidate()
		if err != nil {
			return err
		}
	}

	return nil
}

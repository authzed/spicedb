// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package protovalidate

import (
	"context"
	"errors"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// UnaryServerInterceptor returns a new unary server interceptor that validates incoming messages.
// If the request is invalid, clients may access a structured representation of the validation failure as an error detail.
func UnaryServerInterceptor(validator protovalidate.Validator, opts ...Option) grpc.UnaryServerInterceptor {
	o := evaluateOpts(opts)

	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if err := validateMsg(req, validator, o); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a new streaming server interceptor that validates incoming messages.
// If the request is invalid, clients may access a structured representation of the validation failure as an error detail.
func StreamServerInterceptor(validator protovalidate.Validator, opts ...Option) grpc.StreamServerInterceptor {
	o := evaluateOpts(opts)
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		return handler(srv, &wrappedServerStream{
			ServerStream: stream,
			validator:    validator,
			options:      o,
		})
	}
}

// wrappedServerStream is a thin wrapper around grpc.ServerStream that allows modifying context.
type wrappedServerStream struct {
	grpc.ServerStream

	validator protovalidate.Validator
	options   *options
}

func (w *wrappedServerStream) RecvMsg(m interface{}) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	return validateMsg(m, w.validator, w.options)
}

func validateMsg(m interface{}, validator protovalidate.Validator, opts *options) error {
	msg, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "unsupported message type: %T", m)
	}
	if opts.shouldIgnoreMessage(msg.ProtoReflect().Descriptor().FullName()) {
		return nil
	}
	err := validator.Validate(msg)
	if err == nil {
		return nil
	}
	var valErr *protovalidate.ValidationError
	if errors.As(err, &valErr) {
		// Message is invalid.
		st := status.New(codes.InvalidArgument, err.Error())
		ds, detErr := st.WithDetails(valErr.ToProto())
		if detErr != nil {
			return st.Err()
		}
		return ds.Err()
	}
	// CEL expression doesn't compile or type-check.
	return status.Error(codes.Internal, err.Error())
}

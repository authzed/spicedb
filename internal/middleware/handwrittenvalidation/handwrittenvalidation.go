package handwrittenvalidation

import (
	"context"
	"errors"
	"fmt"

	"buf.build/go/protovalidate"
	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var tracer = otel.Tracer("spicedb/internal/middleware")

// mustNewProtoValidator wraps protovalidate.New() to panic
// if the validator can't be constructed.
func mustNewProtoValidator(opts ...protovalidate.ValidatorOption) protovalidate.Validator {
	validator, err := protovalidate.New(opts...)
	if err != nil {
		wrappedErr := fmt.Errorf("could not construct validator: %w", err)
		panic(wrappedErr)
	}
	return validator
}

type handwrittenValidator interface {
	HandwrittenValidate() error
}

// UnaryServerInterceptor returns a function that performs standard proto validation and handwritten validation (if any) on the incoming request.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	protovalidator := mustNewProtoValidator()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		_, span := tracer.Start(ctx, "protovalidate")
		err := standardValidate(req, protovalidator, span)
		if err != nil {
			span.End()
			return nil, err
		}

		err = handwrittenValidate(req, span)
		if err != nil {
			span.End()
			return nil, err
		}

		span.End()

		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a function that performs standard proto validation and handwritten validation (if any) on the incoming request.
func StreamServerInterceptor(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	wrapper := &recvWrapper{ServerStream: stream, protovalidator: mustNewProtoValidator()}
	return handler(srv, wrapper)
}

type recvWrapper struct {
	grpc.ServerStream
	protovalidator protovalidate.Validator
}

func (s *recvWrapper) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	_, span := tracer.Start(s.Context(), "protovalidate")
	err := standardValidate(m, s.protovalidator, span)
	if err != nil {
		span.End()
		return err
	}

	err = handwrittenValidate(m, span)
	if err != nil {
		span.End()
		return err
	}

	span.End()

	return nil
}

// standardValidate was copied from https://github.com/grpc-ecosystem/go-grpc-middleware/blob/ab2131d954af9580c1b49a3d9475f6adbe5de9d3/interceptors/protovalidate/protovalidate.go#L68
// it validates the proto and if an error occurs, marks the span as errored.
func standardValidate(m any, validator protovalidate.Validator, span trace.Span) error {
	msg, ok := m.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "unsupported message type: %T", m)
	}
	err := validator.Validate(msg)
	if err == nil {
		return nil
	}
	var valErr *protovalidate.ValidationError
	if errors.As(err, &valErr) {
		span.SetStatus(otelcodes.Error, err.Error())
		span.RecordError(err)
		st := status.New(codes.InvalidArgument, err.Error())
		ds, detErr := st.WithDetails(valErr.ToProto())
		if detErr != nil {
			return st.Err()
		}
		return ds.Err()
	}

	return status.Error(codes.Internal, err.Error())
}

func handwrittenValidate(req any, span trace.Span) error {
	validator, ok := req.(handwrittenValidator)
	if ok {
		err := validator.HandwrittenValidate()
		if err != nil {
			span.SetStatus(otelcodes.Error, err.Error())
			span.RecordError(err)
			return status.Errorf(codes.InvalidArgument, "%s", err)
		}
	}
	return nil
}

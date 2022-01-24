// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
)

type serverPayloadReporter struct {
	ctx      context.Context
	logger   Logger
	decision PayloadDecision
}

func (c *serverPayloadReporter) PostCall(error, time.Duration) {}

func (c *serverPayloadReporter) PostMsgSend(req interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case LogPayloadResponse, LogPayloadRequestAndResponse:
	default:
		return
	}

	logger := c.logger.With(ExtractFields(c.ctx)...)
	p, ok := req.(proto.Message)
	if !ok {
		logger.With("req.type", fmt.Sprintf("%T", req)).Log(ERROR, "req is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}
	// For server send message is the response.
	logProtoMessageAsJson(
		logger.With("grpc.send.duration", duration.String()),
		p,
		"grpc.response.content",
		"response payload logged as grpc.response.content field",
	)
}

func (c *serverPayloadReporter) PostMsgReceive(reply interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case LogPayloadRequest, LogPayloadRequestAndResponse:
	default:
		return
	}

	logger := c.logger.With(ExtractFields(c.ctx)...)

	p, ok := reply.(proto.Message)
	if !ok {
		logger.With("reply.type", fmt.Sprintf("%T", reply)).Log(ERROR, "reply is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}
	// For server recv message is the request.
	logProtoMessageAsJson(
		logger.With("grpc.recv.duration", duration.String()),
		p,
		"grpc.request.content",
		"request payload logged as grpc.request.content field",
	)
}

type clientPayloadReporter struct {
	ctx      context.Context
	logger   Logger
	decision PayloadDecision
}

func (c *clientPayloadReporter) PostCall(error, time.Duration) {}

func (c *clientPayloadReporter) PostMsgSend(req interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case LogPayloadRequest, LogPayloadRequestAndResponse:
	default:
		return
	}

	logger := c.logger.With(ExtractFields(c.ctx)...)
	p, ok := req.(proto.Message)
	if !ok {
		logger.With("req.type", fmt.Sprintf("%T", req)).Log(ERROR, "req is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}
	logProtoMessageAsJson(
		logger.With("grpc.send.duration", duration.String()),
		p,
		"grpc.request.content",
		"request payload logged as grpc.request.content field",
	)
}

func (c *clientPayloadReporter) PostMsgReceive(reply interface{}, err error, duration time.Duration) {
	if err != nil {
		return
	}
	switch c.decision {
	case LogPayloadResponse, LogPayloadRequestAndResponse:
	default:
		return
	}

	logger := c.logger.With(ExtractFields(c.ctx)...)
	p, ok := reply.(proto.Message)
	if !ok {
		logger.With("reply.type", fmt.Sprintf("%T", reply)).Log(ERROR, "reply is not a google.golang.org/protobuf/proto.Message; programmatic error?")
		return
	}
	logProtoMessageAsJson(
		logger.With("grpc.recv.duration", duration.String()),
		p,
		"grpc.response.content",
		"response payload logged as grpc.response.content field",
	)
}

type payloadReportable struct {
	clientDecider   ClientPayloadLoggingDecider
	serverDecider   ServerPayloadLoggingDecider
	logger          Logger
	timestampFormat string
}

func (r *payloadReportable) ServerReporter(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	decision := r.serverDecider(ctx, c.FullMethod(), c.ReqProtoOrNil)
	if decision == NoPayloadLogging {
		return interceptors.NoopReporter{}, ctx
	}
	fields := newCommonFields(KindServerFieldValue, c)
	fields = fields.AppendUnique(ExtractFields(ctx))
	if peer, ok := peer.FromContext(ctx); ok {
		fields = append(fields, "peer.address", peer.Addr.String())
	}

	singleUseFields := []string{"grpc.start_time", time.Now().Format(r.timestampFormat)}
	if d, ok := ctx.Deadline(); ok {
		singleUseFields = append(singleUseFields, "grpc.request.deadline", d.Format(r.timestampFormat))
	}
	return &serverPayloadReporter{ctx: ctx, logger: r.logger.With(fields...).With(singleUseFields...), decision: decision}, InjectFields(ctx, fields)
}
func (r *payloadReportable) ClientReporter(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	decision := r.clientDecider(ctx, c.FullMethod())
	if decision == NoPayloadLogging {
		return interceptors.NoopReporter{}, ctx
	}
	fields := newCommonFields(KindClientFieldValue, c)
	fields = fields.AppendUnique(ExtractFields(ctx))
	singleUseFields := []string{"grpc.start_time", time.Now().Format(r.timestampFormat)}
	if d, ok := ctx.Deadline(); ok {
		singleUseFields = append(singleUseFields, "grpc.request.deadline", d.Format(r.timestampFormat))
	}
	return &clientPayloadReporter{ctx: ctx, logger: r.logger.With(fields...).With(singleUseFields...), decision: decision}, InjectFields(ctx, fields)
}

// PayloadUnaryServerInterceptor returns a new unary server interceptors that logs the payloads of requests on INFO level.
// Logger tags will be used from tags context.
func PayloadUnaryServerInterceptor(logger Logger, decider ServerPayloadLoggingDecider,
	timestampFormat string) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&payloadReportable{
		logger:          logger,
		serverDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadStreamServerInterceptor returns a new server interceptors that logs the payloads of requests on INFO level.
// Logger tags will be used from tags context.
func PayloadStreamServerInterceptor(logger Logger, decider ServerPayloadLoggingDecider,
	timestampFormat string) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&payloadReportable{
		logger:          logger,
		serverDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadUnaryClientInterceptor returns a new unary client interceptor that logs the payloads of requests and responses on INFO level.
// Logger tags will be used from tags context.
func PayloadUnaryClientInterceptor(logger Logger, decider ClientPayloadLoggingDecider,
	timestampFormat string) grpc.UnaryClientInterceptor {
	return interceptors.UnaryClientInterceptor(&payloadReportable{
		logger:          logger,
		clientDecider:   decider,
		timestampFormat: timestampFormat})
}

// PayloadStreamClientInterceptor returns a new streaming client interceptor that logs the paylods of requests and responses on INFO level.
// Logger tags will be used from tags context.
func PayloadStreamClientInterceptor(logger Logger, decider ClientPayloadLoggingDecider,
	timestampFormat string) grpc.StreamClientInterceptor {
	return interceptors.StreamClientInterceptor(&payloadReportable{
		logger:          logger,
		clientDecider:   decider,
		timestampFormat: timestampFormat})
}

func logProtoMessageAsJson(logger Logger, pbMsg proto.Message, key string, msg string) {
	payload, err := protojson.Marshal(pbMsg)
	if err != nil {
		logger = logger.With(key, err.Error())
	} else {
		// Trim spaces for deterministic output.
		// See: https://github.com/golang/protobuf/issues/1269
		logger = logger.With(key, string(bytes.Replace(payload, []byte{' '}, []byte{}, -1)))
	}
	logger.Log(INFO, msg)
}

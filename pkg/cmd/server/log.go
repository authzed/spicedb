package server

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type customReportable struct {
	ctx                      context.Context
	log                      zerolog.Logger
	method                   string
	excludedMethods          map[string]struct{}
	excludedRequestPayloads  map[string]struct{}
	excludedResponsePayloads map[string]struct{}
}

func (c customReportable) PostCall(err error, rpcDuration time.Duration) {
	if _, ok := c.excludedMethods[c.method]; ok {
		return
	}
	c.logEvent(nil, err, rpcDuration).Msg("finished call")
}

func (c customReportable) PostMsgSend(respProto any, err error, sendDuration time.Duration) {
	if _, ok := c.excludedMethods[c.method]; ok {
		return
	}
	if _, ok := c.excludedResponsePayloads[c.method]; !ok {
		c.logEvent(respProto, err, sendDuration).Msg("response received")
	}
}

func (c customReportable) PostMsgReceive(reqProto any, err error, recvDuration time.Duration) {
	if _, ok := c.excludedMethods[c.method]; ok {
		return
	}
	c.logEvent(nil, err, recvDuration).Msg("started call")
	if _, ok := c.excludedRequestPayloads[c.method]; !ok {
		c.logEvent(reqProto, err, recvDuration).Msg("request received")
	}
}

func (c customReportable) logEvent(payload any, err error, recvDuration time.Duration) *zerolog.Event {
	code := status.Code(err)
	event := c.log.WithLevel(grpcCodeToLogLevel(code))
	if err != nil {
		event = event.Str("grpc.code", code.String()).Str("grpc.error", fmt.Sprintf("%v", err))
	}

	if p, ok := peer.FromContext(c.ctx); ok {
		event = event.Str("peer.address", p.Addr.String())
	}

	if payload != nil {
		event = event.Str("grpc.request.type", fmt.Sprintf("%T", payload)).Str("grpc.request.content", fmt.Sprintf("%v", payload))
	}

	return event.Int64("grpc.time_ms", recvDuration.Milliseconds())
}

func newReporter(log zerolog.Logger, excludedMethods, excludedRequestPayloads, excludedResponsePayloads map[string]struct{}) interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
		return &customReportable{
			ctx:                      ctx,
			log:                      log,
			method:                   c.Method,
			excludedMethods:          excludedMethods,
			excludedRequestPayloads:  excludedRequestPayloads,
			excludedResponsePayloads: excludedResponsePayloads,
		}, ctx
	}
}

func grpcCodeToLogLevel(code codes.Code) zerolog.Level {
	switch code {
	case codes.OK, codes.NotFound, codes.Canceled, codes.DeadlineExceeded, codes.AlreadyExists, codes.InvalidArgument, codes.Unauthenticated:
		return zerolog.InfoLevel

	case codes.PermissionDenied, codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
		codes.OutOfRange, codes.Unavailable:
		return zerolog.WarnLevel

	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return zerolog.ErrorLevel

	default:
		return zerolog.ErrorLevel
	}
}

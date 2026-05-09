// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package selector

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"google.golang.org/grpc"
)

// Matcher allows matching.
type Matcher interface {
	// Match returns true, if given context and gRPC call metadata like type, service and method are matching.
	Match(ctx context.Context, callMeta interceptors.CallMeta) bool
}

// MatchFunc return Matcher from closure.
func MatchFunc(f func(ctx context.Context, callMeta interceptors.CallMeta) bool) Matcher {
	return funcSelector{f: f}
}

type funcSelector struct {
	f func(ctx context.Context, callMeta interceptors.CallMeta) bool
}

func (s funcSelector) Match(ctx context.Context, callMeta interceptors.CallMeta) bool {
	return s.f(ctx, callMeta)
}

// UnaryServerInterceptor returns a new unary server interceptor that will decide whether to call
// the interceptor based on the return argument from the Matcher.
func UnaryServerInterceptor(i grpc.UnaryServerInterceptor, matcher Matcher) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		c := interceptors.NewServerCallMeta(info.FullMethod, nil, req)
		if matcher.Match(ctx, c) {
			return i(ctx, req, info, handler)
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that will decide whether to call
// the interceptor based on the return argument from the Matcher.
func StreamServerInterceptor(i grpc.StreamServerInterceptor, matcher Matcher) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		c := interceptors.NewServerCallMeta(info.FullMethod, info, nil)
		if matcher.Match(ss.Context(), c) {
			return i(srv, ss, info, handler)
		}
		return handler(srv, ss)
	}
}

// UnaryClientInterceptor returns a new unary client interceptor that will decide whether to call
// the interceptor based on the return argument from the Matcher.
// TODO(bwplotka): Write unit test.
func UnaryClientInterceptor(i grpc.UnaryClientInterceptor, matcher Matcher) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		c := interceptors.NewClientCallMeta(method, nil, req)
		if matcher.Match(ctx, c) {
			return i(ctx, method, req, reply, cc, invoker, opts...)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor returns a new stream client interceptor that will decide whether to call
// the interceptor based on the return argument from the Matcher.
// TODO(bwplotka): Write unit test.
func StreamClientInterceptor(i grpc.StreamClientInterceptor, matcher Matcher) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		c := interceptors.NewClientCallMeta(method, desc, nil)
		if matcher.Match(ctx, c) {
			return i(ctx, desc, cc, method, streamer, opts...)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

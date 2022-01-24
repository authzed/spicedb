package grpcutil

import (
	"context"
	"fmt"

	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpcvalidate "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// IgnoreAuthMixin is a struct that can be embedded to make a gRPC handler
// ignore any auth requirements set by the gRPC community auth middleware.
type IgnoreAuthMixin struct{}

var _ grpc_auth.ServiceAuthFuncOverride = (*IgnoreAuthMixin)(nil)

// AuthFuncOverride implements the grpc_auth.ServiceAuthFuncOverride by
// performing a no-op.
func (m IgnoreAuthMixin) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	return ctx, nil
}

// AuthlessHealthServer implements a gRPC health endpoint that will ignore any auth
// requirements set by github.com/grpc-ecosystem/go-grpc-middleware/auth.
type AuthlessHealthServer struct {
	*health.Server
	IgnoreAuthMixin
}

// NewAuthlessHealthServer returns a new gRPC health server that ignores auth
// middleware.
func NewAuthlessHealthServer() *AuthlessHealthServer {
	return &AuthlessHealthServer{Server: health.NewServer()}
}

// SetServicesHealthy sets the service to SERVING
func (s *AuthlessHealthServer) SetServicesHealthy(svcDesc ...*grpc.ServiceDesc) {
	for _, d := range svcDesc {
		s.SetServingStatus(
			d.ServiceName,
			healthpb.HealthCheckResponse_SERVING,
		)
	}
}

// DefaultUnaryMiddleware is a recommended set of middleware that should each gracefully no-op if the middleware is not
// applicable.
var DefaultUnaryMiddleware = []grpc.UnaryServerInterceptor{grpcvalidate.UnaryServerInterceptor()}

// WrapMethods wraps all non-streaming endpoints with the given list of interceptors.
// It returns a copy of the ServiceDesc with the new wrapped methods.
func WrapMethods(svcDesc grpc.ServiceDesc, interceptors ...grpc.UnaryServerInterceptor) (wrapped *grpc.ServiceDesc) {
	chain := grpcmw.ChainUnaryServer(interceptors...)
	for i, m := range svcDesc.Methods {
		handler := m.Handler
		wrapped := grpc.MethodDesc{
			MethodName: m.MethodName,
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				if interceptor == nil {
					interceptor = NoopUnaryInterceptor
				}
				return handler(srv, ctx, dec, grpcmw.ChainUnaryServer(interceptor, chain))
			},
		}
		svcDesc.Methods[i] = wrapped
	}
	return &svcDesc
}

// WrapStreams wraps all streaming endpoints with the given list of interceptors.
// It returns a copy of the ServiceDesc with the new wrapped methods.
func WrapStreams(svcDesc grpc.ServiceDesc, interceptors ...grpc.StreamServerInterceptor) (wrapped *grpc.ServiceDesc) {
	chain := grpcmw.ChainStreamServer(interceptors...)
	for i, s := range svcDesc.Streams {
		handler := s.Handler
		info := &grpc.StreamServerInfo{
			FullMethod:     fmt.Sprintf("/%s/%s", svcDesc.ServiceName, s.StreamName),
			IsClientStream: s.ClientStreams,
			IsServerStream: s.ServerStreams,
		}
		wrapped := grpc.StreamDesc{
			StreamName:    s.StreamName,
			ClientStreams: s.ClientStreams,
			ServerStreams: s.ServerStreams,
			Handler: func(srv interface{}, stream grpc.ServerStream) error {
				return chain(srv, stream, info, handler)
			},
		}
		svcDesc.Streams[i] = wrapped
	}
	return &svcDesc
}

// NoopUnaryInterceptor is a gRPC middleware that does not do anything.
func NoopUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	return handler(ctx, req)
}

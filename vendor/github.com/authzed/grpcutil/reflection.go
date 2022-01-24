package grpcutil

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

// NewAuthlessReflectionInterceptor creates a proxy GRPCServer which automatically converts
// ServerReflectionServer instances to onces that skip grpc auth middleware.
//
// change:
// reflection.Register(srv)
// to:
// reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))
func NewAuthlessReflectionInterceptor(srv reflection.GRPCServer) reflection.GRPCServer {
	return interceptingRegistrar{srv}
}

type interceptingRegistrar struct {
	delegate reflection.GRPCServer
}

func (ir interceptingRegistrar) GetServiceInfo() map[string]grpc.ServiceInfo {
	return ir.delegate.GetServiceInfo()
}

func (ir interceptingRegistrar) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	reflectionSrv := impl.(rpb.ServerReflectionServer)
	ir.delegate.RegisterService(desc, &authlessReflection{ServerReflectionServer: reflectionSrv})
}

type authlessReflection struct {
	IgnoreAuthMixin

	rpb.ServerReflectionServer
}

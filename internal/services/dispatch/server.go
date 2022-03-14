package dispatch

import (
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatch_v1 "github.com/authzed/spicedb/internal/services/dispatch/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// RegisterGrpcServices registers an internal dispatch service with the specified server.
func RegisterGrpcServices(
	srv *grpc.Server,
	d dispatch.Dispatcher,
) {
	srv.RegisterService(&dispatchv1.DispatchService_ServiceDesc, dispatch_v1.NewDispatchServer(d))
	healthSrv := grpcutil.NewAuthlessHealthServer()
	healthSrv.SetServicesHealthy(&dispatchv1.DispatchService_ServiceDesc)
	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}

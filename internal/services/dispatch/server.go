package dispatch

import (
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatch_v1 "github.com/authzed/spicedb/internal/services/dispatch/v1"
	v1svc "github.com/authzed/spicedb/internal/services/dispatch/v1"
)

// RegisterGrpcServices registers an internal dispatch service with the specified server.
func RegisterGrpcServices(
	srv *grpc.Server,
	d dispatch.Dispatcher,
) {
	healthSrv := grpcutil.NewAuthlessHealthServer()
	healthSrv.SetServicesHealthy(
		v1svc.RegisterDispatchServer(srv, dispatch_v1.NewDispatchServer(d)),
	)
	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}

package dispatch

import (
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatch_v1 "github.com/authzed/spicedb/internal/services/dispatch/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// RegisterGrpcServices registers an internal dispatch service with the specified server.
// The service dispatches with chunker. A nil chunker selects dispatch.DefaultChunker.
func RegisterGrpcServices(
	srv *grpc.Server,
	d dispatch.Dispatcher,
	chunker dispatch.Chunker,
) {
	srv.RegisterService(&dispatchv1.DispatchService_ServiceDesc, dispatch_v1.NewDispatchServer(d, chunker))
	healthSrv := grpcutil.NewAuthlessHealthServer()
	healthSrv.SetServicesHealthy(&dispatchv1.DispatchService_ServiceDesc)
	healthpb.RegisterHealthServer(srv, healthSrv)
	reflection.Register(srv)
}

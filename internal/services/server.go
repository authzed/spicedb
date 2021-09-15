package services

import (
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	v1 "github.com/authzed/spicedb/internal/services/v1"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
)

func RegisterGrpcServices(
	srv *grpc.Server,
	ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch dispatch.Dispatcher,
	maxDepth uint32,
	prefixRequired v1alpha1svc.PrefixRequiredOption,
) {
	healthSrv := grpcutil.NewAuthlessHealthServer()

	healthSrv.SetServicesHealthy(
		v0svc.RegisterACLServer(srv, v0svc.NewACLServer(ds, nsm, dispatch, maxDepth)),
		v0svc.RegisterNamespaceServer(srv, v0svc.NewNamespaceServer(ds)),
		v0svc.RegisterWatchServer(srv, v0svc.NewWatchServer(ds, nsm)),
		v1alpha1svc.RegisterSchemaServer(srv, v1alpha1svc.NewSchemaServer(ds, prefixRequired)),
		v1.RegisterPermissionsServer(srv, ds, nsm),
	)

	healthpb.RegisterHealthServer(srv, healthSrv)

	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))
}

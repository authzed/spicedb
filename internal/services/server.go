package services

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/dispatch"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
)

// SchemaServiceOption defines the options for enabled or disabled the V1 Schema service.
type SchemaServiceOption int

const (
	// V1SchemaServiceDisabled indicates that the V1 schema service is disabled.
	V1SchemaServiceDisabled SchemaServiceOption = 0

	// V1SchemaServiceEnabled indicates that the V1 schema service is enabled.
	V1SchemaServiceEnabled SchemaServiceOption = 1
)

// RegisterGrpcServices registers all services to be exposed on the GRPC server.
func RegisterGrpcServices(
	srv *grpc.Server,
	dispatch dispatch.Dispatcher,
	maxDepth uint32,
	prefixRequired v1alpha1svc.PrefixRequiredOption,
	schemaServiceOption SchemaServiceOption,
) {
	healthSrv := grpcutil.NewAuthlessHealthServer()

	v1alpha1.RegisterSchemaServiceServer(srv, v1alpha1svc.NewSchemaServer(prefixRequired))
	healthSrv.SetServicesHealthy(&v1alpha1.SchemaService_ServiceDesc)

	v1.RegisterPermissionsServiceServer(srv, v1svc.NewPermissionsServer(dispatch, maxDepth))
	healthSrv.SetServicesHealthy(&v1.PermissionsService_ServiceDesc)

	v1.RegisterWatchServiceServer(srv, v1svc.NewWatchServer())
	healthSrv.SetServicesHealthy(&v1.WatchService_ServiceDesc)

	if schemaServiceOption == V1SchemaServiceEnabled {
		v1.RegisterSchemaServiceServer(srv, v1svc.NewSchemaServer())
		healthSrv.SetServicesHealthy(&v1.SchemaService_ServiceDesc)
	}

	healthpb.RegisterHealthServer(srv, healthSrv)

	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))
}

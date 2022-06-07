package services

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/services/health"
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

const (
	// Empty string is used for grpc health check requests for the overall system.
	OverallServerHealthCheckKey = ""
)

// RegisterGrpcServices registers all services to be exposed on the GRPC server.
func RegisterGrpcServices(
	srv *grpc.Server,
	healthManager health.Manager,
	dispatch dispatch.Dispatcher,
	maxDepth uint32,
	prefixRequired v1alpha1svc.PrefixRequiredOption,
	schemaServiceOption SchemaServiceOption,
) {
	healthManager.RegisterReportedService(OverallServerHealthCheckKey)

	v1alpha1.RegisterSchemaServiceServer(srv, v1alpha1svc.NewSchemaServer(prefixRequired))
	healthManager.RegisterReportedService(v1alpha1.SchemaService_ServiceDesc.ServiceName)

	v1.RegisterPermissionsServiceServer(srv, v1svc.NewPermissionsServer(dispatch, maxDepth))
	healthManager.RegisterReportedService(v1.PermissionsService_ServiceDesc.ServiceName)

	v1.RegisterWatchServiceServer(srv, v1svc.NewWatchServer())
	healthManager.RegisterReportedService(v1.WatchService_ServiceDesc.ServiceName)

	if schemaServiceOption == V1SchemaServiceEnabled {
		v1.RegisterSchemaServiceServer(srv, v1svc.NewSchemaServer())
		healthManager.RegisterReportedService(v1.SchemaService_ServiceDesc.ServiceName)
	}

	healthpb.RegisterHealthServer(srv, healthManager.HealthSvc())
	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))
}

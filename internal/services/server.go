package services

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
)

// SchemaServiceOption defines the options for enabling or disabling the V1 Schema service.
type SchemaServiceOption int

// WatchServiceOption defines the options for enabling or disabling the V1 Watch service.
type WatchServiceOption int

// CaveatsOption defines the options for enabling or disabling caveats in the V1 services.
type CaveatsOption int

const (
	// V1SchemaServiceDisabled indicates that the V1 schema service is disabled.
	V1SchemaServiceDisabled SchemaServiceOption = 0

	// V1SchemaServiceEnabled indicates that the V1 schema service is enabled.
	V1SchemaServiceEnabled SchemaServiceOption = 1

	// V1SchemaServiceAdditiveOnly indicates that the V1 schema service is enabled in additive-only
	// mode for testing.
	V1SchemaServiceAdditiveOnly SchemaServiceOption = 2

	// WatchServiceDisabled indicates that the V1 watch service is disabled.
	WatchServiceDisabled WatchServiceOption = 0

	// WatchServiceEnabled indicates that the V1 watch service is enabled.
	WatchServiceEnabled WatchServiceOption = 1

	// CaveatsDisabled indicates that caveats are disabled.
	CaveatsDisabled CaveatsOption = 0

	// CaveatsEnabled indicates that caveats are enabled.
	CaveatsEnabled CaveatsOption = 1
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
	schemaServiceOption SchemaServiceOption,
	watchServiceOption WatchServiceOption,
	caveatsOption CaveatsOption,
	permSysConfig v1svc.PermissionsServerConfig,
) {
	healthManager.RegisterReportedService(OverallServerHealthCheckKey)

	v1.RegisterPermissionsServiceServer(srv, v1svc.NewPermissionsServer(dispatch, permSysConfig, caveatsOption == CaveatsEnabled))
	healthManager.RegisterReportedService(v1.PermissionsService_ServiceDesc.ServiceName)

	if watchServiceOption == WatchServiceEnabled {
		v1.RegisterWatchServiceServer(srv, v1svc.NewWatchServer())
		healthManager.RegisterReportedService(v1.WatchService_ServiceDesc.ServiceName)
	}

	if schemaServiceOption == V1SchemaServiceEnabled || schemaServiceOption == V1SchemaServiceAdditiveOnly {
		v1.RegisterSchemaServiceServer(srv, v1svc.NewSchemaServer(schemaServiceOption == V1SchemaServiceAdditiveOnly, caveatsOption == CaveatsEnabled))
		healthManager.RegisterReportedService(v1.SchemaService_ServiceDesc.ServiceName)
	}

	healthpb.RegisterHealthServer(srv, healthManager.HealthSvc())
	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))
}

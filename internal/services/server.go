package services

import (
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
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

	// Empty string is used for grpc health check requests for the overall system.
	OverallServerHealthCheckKey = ""
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
	healthSrv.Server.SetServingStatus(v1.PermissionsService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	v1.RegisterWatchServiceServer(srv, v1svc.NewWatchServer())
	healthSrv.SetServicesHealthy(&v1.WatchService_ServiceDesc)

	if schemaServiceOption == V1SchemaServiceEnabled {
		v1.RegisterSchemaServiceServer(srv, v1svc.NewSchemaServer())
		healthSrv.SetServicesHealthy(&v1.SchemaService_ServiceDesc)
	}

	healthpb.RegisterHealthServer(srv, healthSrv)

	reflection.Register(grpcutil.NewAuthlessReflectionInterceptor(srv))

	go checkDispatcherServicesReady(dispatch, healthSrv, v1.PermissionsService_ServiceDesc.ServiceName, OverallServerHealthCheckKey)
}

// checkDispatcherServicesReady waits for the dispatcher to become ready before setting the health check status for the given services
func checkDispatcherServicesReady(dispatch dispatch.Dispatcher, healthSrv *grpcutil.AuthlessHealthServer, services ...string) {
	backoffInterval := backoff.NewExponentialBackOff()
	// Run immediately for the initial check
	ticker := time.After(0)

	for {
		_, ok := <-ticker
		if !ok {
			log.Warn().Msg("backoff error while waiting for dispatcher health")
			return
		}

		if dispatch.Ready() {
			for _, s := range services {
				healthSrv.Server.SetServingStatus(s, healthpb.HealthCheckResponse_SERVING)
			}
			return
		}

		nextPush := backoffInterval.NextBackOff()
		if nextPush == backoff.Stop {
			log.Warn().Msg("exceed max attempts to check for dispatch ready")
			return
		}
		ticker = time.After(nextPush)
	}
}

package health

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"

	"github.com/authzed/grpcutil"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/authzed/spicedb/internal/dispatch"
)

const datastoreReadyTimeout = time.Millisecond * 500

// NewHealthManager creates and returns a new health manager that checks the IsReady
// status of the given dispatcher and datastore checker and sets the health check to
// return healthy once both have gone to true.
func NewHealthManager(dispatcher dispatch.Dispatcher, dsc DatastoreChecker) Manager {
	healthSvc := grpcutil.NewAuthlessHealthServer()
	return &healthManager{healthSvc, dispatcher, dsc, map[string]struct{}{}}
}

// DatastoreChecker is an interface for determining if the datastore is ready for
// traffic.
type DatastoreChecker interface {
	// IsReady returns whether the datastore is ready to be used.
	IsReady(ctx context.Context) (bool, error)
}

// Manager is a system which manages the health service statuses.
type Manager interface {
	// RegisterReportedService registers the name of service under the same server
	// for whom the health is being managed by this manager.
	RegisterReportedService(serviceName string)

	// HealthSvc is the health service this manager is managing.
	HealthSvc() *grpcutil.AuthlessHealthServer

	// Checker returns a function that can be run via an errgroup to perform the health checks.
	Checker(ctx context.Context) func() error
}

type healthManager struct {
	healthSvc    *grpcutil.AuthlessHealthServer
	dispatcher   dispatch.Dispatcher
	dsc          DatastoreChecker
	serviceNames map[string]struct{}
}

func (hm *healthManager) HealthSvc() *grpcutil.AuthlessHealthServer {
	return hm.healthSvc
}

func (hm *healthManager) RegisterReportedService(serviceName string) {
	hm.serviceNames[serviceName] = struct{}{}
	hm.healthSvc.Server.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
}

func (hm *healthManager) Checker(ctx context.Context) func() error {
	return func() error {
		// Run immediately for the initial check
		backoffInterval := backoff.NewExponentialBackOff()
		ticker := time.After(0)

		for {
			select {
			case _, ok := <-ticker:
				if !ok {
					log.Warn().Msg("backoff error while waiting for dispatcher or datastore health")
					return nil
				}

			case <-ctx.Done():
				log.Warn().Msg("datastore health context was canceled")
				return nil
			}

			isReady := hm.checkIsReady(ctx)
			if isReady {
				for serviceName := range hm.serviceNames {
					hm.healthSvc.Server.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
				}
				return nil
			}

			nextPush := backoffInterval.NextBackOff()
			if nextPush == backoff.Stop {
				log.Warn().Msg("exceed max attempts to check for dispatch or datastore ready")
				return nil
			}
			ticker = time.After(nextPush)
		}
	}
}

func (hm *healthManager) checkIsReady(ctx context.Context) bool {
	log.Debug().Msg("checking if datastore and dispatcher are ready")

	ctx, cancel := context.WithTimeout(ctx, datastoreReadyTimeout)
	defer cancel()

	dsReady, err := hm.dsc.IsReady(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("could not check if the datastore was ready")
	}

	dispatchReady := hm.dispatcher.IsReady()
	log.Debug().Bool("datastoreReady", dsReady).Bool("dispatchReady", dispatchReady).Msg("completed dispatcher and datastore readiness checks")
	return dsReady && dispatchReady
}

package health

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

// This is kept high because the spicedb deployment and the datastore might not be in the same region.
// Ideally, they are, but they might not.
const datastoreReadyTimeout = time.Second * 3

// recheckFailureThreshold is the number of consecutive failed readiness rechecks
// after which the reported services are marked NOT_SERVING. One failure is not
// enough: a single slow check should not take a healthy server out of rotation.
const recheckFailureThreshold = 2

// Option configures a health manager.
type Option func(*healthManager)

// WithReadinessRecheck makes Checker keep checking the datastore and dispatcher
// every interval after the services first become SERVING: after
// recheckFailureThreshold consecutive failures the services are marked
// NOT_SERVING, and SERVING again on the next success. An interval of zero (the
// default) keeps the startup-only behavior.
func WithReadinessRecheck(interval time.Duration) Option {
	return func(hm *healthManager) {
		hm.recheckInterval = interval
	}
}

// NewHealthManager creates and returns a new health manager that checks the IsReady
// status of the given dispatcher and datastore checker and sets the health check to
// return healthy once both have gone to true.
func NewHealthManager(dispatcher dispatch.Dispatcher, dsc DatastoreChecker, opts ...Option) Manager {
	healthSvc := grpcutil.NewAuthlessHealthServer()
	hm := &healthManager{healthSvc: healthSvc, dispatcher: dispatcher, dsc: dsc, serviceNames: map[string]struct{}{}}
	for _, opt := range opts {
		opt(hm)
	}
	return hm
}

// DatastoreChecker is an interface for determining if the datastore is ready for
// traffic.
type DatastoreChecker interface {
	// ReadyState returns whether the datastore is ready to be used.
	// This call must respect context cancellation and deadlines.
	ReadyState(ctx context.Context) (datastore.ReadyState, error)
}

// Manager is a system which manages the health service statuses.
type Manager interface {
	// RegisterReportedService registers the name of service under the same server
	// for whom the health is being managed by this manager.
	RegisterReportedService(serviceName string)

	// HealthSvc is the health service this manager is managing.
	HealthSvc() *grpcutil.AuthlessHealthServer

	// Checker blocks until the status is SERVING or until the context is done.
	Checker(ctx context.Context) error

	// Close marks every managed service NOT_SERVING and pins it there, so load
	// balancers stop routing to a server that is about to close its listeners.
	Close()

	// right now, Checker is serving both as a startup probe and a readiness probe.
	// However, its implementation is really only functioning as a startup probe because the function exits quickly.
	// TODO(miparnisari): Split Checker into two functions: Startup and Readiness (to match kubernetes probes)
	// Startup can continue with this implementation. Readiness can ... be something else (TBD).
	// ALso: we don't need a per-service probe, just one a general one for the entire service.
}

type healthManager struct {
	healthSvc       *grpcutil.AuthlessHealthServer
	dispatcher      dispatch.Dispatcher
	dsc             DatastoreChecker
	serviceNames    map[string]struct{}
	recheckInterval time.Duration
}

func (hm *healthManager) HealthSvc() *grpcutil.AuthlessHealthServer {
	return hm.healthSvc
}

func (hm *healthManager) Close() {
	hm.healthSvc.Shutdown()
}

func (hm *healthManager) RegisterReportedService(serviceName string) {
	hm.serviceNames[serviceName] = struct{}{}
	hm.healthSvc.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
}

// Checker blocks until the underlying dependencies are ready.
// When they are, it marks the service as "serving" and returns, unless the
// manager was built WithReadinessRecheck: then it keeps rechecking until the
// context is done (see recheck).
// If they never become ready, it just returns.
func (hm *healthManager) Checker(ctx context.Context) error {
	log.Ctx(ctx).Info().Msg("HEALTHCHECK")
	// Run immediately for the initial check
	backoffInterval := backoff.NewExponentialBackOff()

	ticker := time.After(0)

	for {
		select {
		case _, ok := <-ticker:
			if !ok {
				log.Ctx(ctx).Warn().Msg("backoff error while waiting for dispatcher or datastore health")
				return nil
			}

		case <-ctx.Done():
			log.Ctx(ctx).Info().Msg("datastore health check canceled")
			return nil
		}

		isReady := hm.checkIsReady(ctx)
		if isReady {
			hm.setStatus(healthpb.HealthCheckResponse_SERVING)
			if hm.recheckInterval <= 0 {
				return nil
			}
			return hm.recheck(ctx)
		}

		nextPush := backoffInterval.NextBackOff()
		if nextPush == backoff.Stop {
			log.Ctx(ctx).Warn().Msg("exceed max attempts to check for dispatch or datastore ready")
			return nil
		}
		ticker = time.After(nextPush)
	}
}

// setStatus sets every reported service to the given status. After Close the
// underlying health server ignores it, so a recheck never revives a server that
// is shutting down.
func (hm *healthManager) setStatus(status healthpb.HealthCheckResponse_ServingStatus) {
	for serviceName := range hm.serviceNames {
		hm.healthSvc.SetServingStatus(serviceName, status)
	}
}

// recheck keeps checking readiness every recheckInterval until the context is
// done, so the health service also works as a readiness probe: a server that
// loses its datastore after startup stops reporting SERVING.
func (hm *healthManager) recheck(ctx context.Context) error {
	ticker := time.NewTicker(hm.recheckInterval)
	defer ticker.Stop()

	serving := true
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		if hm.checkIsReady(ctx) {
			failures = 0
			if !serving {
				log.Ctx(ctx).Info().Msg("dispatcher and datastore are ready again; marking services SERVING")
				hm.setStatus(healthpb.HealthCheckResponse_SERVING)
				serving = true
			}
			continue
		}

		failures++
		if serving && failures >= recheckFailureThreshold {
			log.Ctx(ctx).Warn().Int("consecutiveFailures", failures).
				Msg("dispatcher or datastore failed readiness rechecks; marking services NOT_SERVING")
			hm.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)
			serving = false
		}
	}
}

// checkIsReady returns true if both the datastore and the dispatcher are ready
func (hm *healthManager) checkIsReady(ctx context.Context) bool {
	log.Ctx(ctx).Debug().Msg("checking if datastore and dispatcher are ready")

	ctx, cancel := context.WithTimeout(ctx, datastoreReadyTimeout)
	defer cancel()

	dsReady, err := hm.dsc.ReadyState(ctx)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("could not check if the datastore was ready")
		return false
	}

	if !dsReady.IsReady {
		log.Ctx(ctx).Warn().Bool("datastoreReady", false).Msgf("datastore failed readiness checks: %s", dsReady.Message)
		return false
	}

	dispatchReady := hm.dispatcher.ReadyState()
	if !dispatchReady.IsReady {
		log.Ctx(ctx).Warn().Bool("dispatchReady", false).Msgf("dispatcher failed readiness checks: %s", dispatchReady.Message)
		return false
	}

	log.Ctx(ctx).Debug().Bool("datastoreReady", true).Bool("dispatchReady", true).Msg("completed dispatcher and datastore readiness checks")
	return true
}

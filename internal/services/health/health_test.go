package health

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatchmocks "github.com/authzed/spicedb/internal/dispatch/mocks"
	"github.com/authzed/spicedb/pkg/datastore"
	datastoremocks "github.com/authzed/spicedb/pkg/datastore/mocks"
)

func TestNewHealthManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dispatcher := dispatchmocks.NewMockDispatcher(ctrl)
	dsc := datastoremocks.NewMockDatastore(ctrl)

	manager := NewHealthManager(dispatcher, dsc)

	require.NotNil(t, manager)
	require.NotNil(t, manager.HealthSvc())
}

func TestHealthManagerRegisterReportedService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dispatcher := dispatchmocks.NewMockDispatcher(ctrl)
	dsc := datastoremocks.NewMockDatastore(ctrl)
	manager := NewHealthManager(dispatcher, dsc)

	serviceName := "test-service"
	manager.RegisterReportedService(serviceName)

	// Verify service was registered by checking the internal map
	hm := manager.(*healthManager)
	_, exists := hm.serviceNames[serviceName]
	require.True(t, exists)
}

func TestHealthManagerCheckIsReady(t *testing.T) {
	testCases := []struct {
		name            string
		datastoreReady  bool
		datastoreError  error
		dispatcherReady bool
		expectedResult  bool
	}{
		{
			name:            "both ready",
			datastoreReady:  true,
			datastoreError:  nil,
			dispatcherReady: true,
			expectedResult:  true,
		},
		{
			name:            "datastore not ready",
			datastoreReady:  false,
			datastoreError:  nil,
			dispatcherReady: true,
			expectedResult:  false,
		},
		{
			name:            "dispatcher not ready",
			datastoreReady:  true,
			datastoreError:  nil,
			dispatcherReady: false,
			expectedResult:  false,
		},
		{
			name:            "both not ready",
			datastoreReady:  false,
			datastoreError:  nil,
			dispatcherReady: false,
			expectedResult:  false,
		},
		{
			name:            "datastore error dispatcher ready",
			datastoreReady:  false, // doesn't matter when there's an error
			datastoreError:  errors.New("datastore error"),
			dispatcherReady: true,
			expectedResult:  false,
		},
		{
			name:            "datastore error dispatcher not ready",
			datastoreReady:  false, // doesn't matter when there's an error
			datastoreError:  errors.New("datastore error"),
			dispatcherReady: false,
			expectedResult:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			dispatcher := dispatchmocks.NewMockDispatcher(ctrl)
			dispatcher.EXPECT().ReadyState().Return(dispatch.ReadyState{IsReady: tc.dispatcherReady}).MaxTimes(1)

			dsc := datastoremocks.NewMockDatastore(ctrl)
			dsc.EXPECT().ReadyState(gomock.Any()).Return(datastore.ReadyState{IsReady: tc.datastoreReady}, tc.datastoreError).Times(1)

			hm := &healthManager{
				dispatcher: dispatcher,
				dsc:        dsc,
			}

			require.Equal(t, tc.expectedResult, hm.checkIsReady(t.Context()))
		})
	}
}

func TestHealthManagerCloseReportsNotServing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	manager := NewHealthManager(dispatchmocks.NewMockDispatcher(ctrl), datastoremocks.NewMockDatastore(ctrl))
	manager.RegisterReportedService("test-service")
	manager.HealthSvc().SetServingStatus("test-service", healthpb.HealthCheckResponse_SERVING)

	manager.Close()

	resp, err := manager.HealthSvc().Check(t.Context(), &healthpb.HealthCheckRequest{Service: "test-service"})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())

	// A late readiness check must not flip the status back while shutting down.
	manager.HealthSvc().SetServingStatus("test-service", healthpb.HealthCheckResponse_SERVING)
	resp, err = manager.HealthSvc().Check(t.Context(), &healthpb.HealthCheckRequest{Service: "test-service"})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())
}

// flakyDatastore is a DatastoreChecker whose readiness the test controls: it is
// down while healthy is false, and fails the next failNext checks even if healthy.
type flakyDatastore struct {
	healthy  atomic.Bool
	failNext atomic.Int32
	calls    atomic.Int32
}

func (f *flakyDatastore) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	f.calls.Add(1)
	if f.failNext.Load() > 0 {
		f.failNext.Add(-1)
		return datastore.ReadyState{}, errors.New("connection refused")
	}
	if f.healthy.Load() {
		return datastore.ReadyState{IsReady: true}, nil
	}
	return datastore.ReadyState{}, errors.New("connection refused")
}

func readyDispatcher(t *testing.T) dispatch.Dispatcher {
	ctrl := gomock.NewController(t)
	dispatcher := dispatchmocks.NewMockDispatcher(ctrl)
	dispatcher.EXPECT().ReadyState().Return(dispatch.ReadyState{IsReady: true}).AnyTimes()
	return dispatcher
}

func servingStatus(t *testing.T, manager Manager) healthpb.HealthCheckResponse_ServingStatus {
	resp, err := manager.HealthSvc().Check(t.Context(), &healthpb.HealthCheckRequest{Service: "test-service"})
	require.NoError(t, err)
	return resp.GetStatus()
}

// startChecker runs Checker in the background until the test ends and waits for SERVING.
func startChecker(t *testing.T, manager Manager) context.CancelFunc {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Checker(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("Checker did not return after the context was canceled")
		}
	})
	require.Eventually(t, func() bool { return servingStatus(t, manager) == healthpb.HealthCheckResponse_SERVING },
		5*time.Second, time.Millisecond)
	return cancel
}

func TestCheckerWithoutRecheckReturnsOnceServing(t *testing.T) {
	ds := &flakyDatastore{}
	ds.healthy.Store(true)
	manager := NewHealthManager(readyDispatcher(t), ds)
	manager.RegisterReportedService("test-service")

	require.NoError(t, manager.Checker(t.Context()))
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, servingStatus(t, manager))
	require.Equal(t, int32(1), ds.calls.Load())
}

func TestCheckerRecheckReportsDatastoreLossAndRecovery(t *testing.T) {
	ds := &flakyDatastore{}
	ds.healthy.Store(true)
	manager := NewHealthManager(readyDispatcher(t), ds, WithReadinessRecheck(10*time.Millisecond))
	manager.RegisterReportedService("test-service")
	startChecker(t, manager)

	// The datastore goes away after startup: the server stops reporting SERVING.
	ds.healthy.Store(false)
	require.Eventually(t, func() bool { return servingStatus(t, manager) == healthpb.HealthCheckResponse_NOT_SERVING },
		5*time.Second, time.Millisecond)

	// And reports SERVING again once it is back.
	ds.healthy.Store(true)
	require.Eventually(t, func() bool { return servingStatus(t, manager) == healthpb.HealthCheckResponse_SERVING },
		5*time.Second, time.Millisecond)
}

func TestCheckerRecheckToleratesASingleFailure(t *testing.T) {
	ds := &flakyDatastore{}
	ds.healthy.Store(true)
	manager := NewHealthManager(readyDispatcher(t), ds, WithReadinessRecheck(10*time.Millisecond))
	manager.RegisterReportedService("test-service")
	startChecker(t, manager)

	// One failed recheck among good ones never takes the server out of rotation.
	ds.failNext.Store(recheckFailureThreshold - 1)
	calls := ds.calls.Load()
	for ds.calls.Load() < calls+recheckFailureThreshold+3 {
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, servingStatus(t, manager))
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, int32(0), ds.failNext.Load())
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, servingStatus(t, manager))
}

func TestCheckerRecheckNeverRevivesAClosedServer(t *testing.T) {
	ds := &flakyDatastore{}
	ds.healthy.Store(true)
	manager := NewHealthManager(readyDispatcher(t), ds, WithReadinessRecheck(10*time.Millisecond))
	manager.RegisterReportedService("test-service")
	startChecker(t, manager)

	manager.Close()
	calls := ds.calls.Load()
	require.Eventually(t, func() bool { return ds.calls.Load() >= calls+3 }, 5*time.Second, time.Millisecond)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, servingStatus(t, manager))
}

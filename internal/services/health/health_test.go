package health

import (
	"errors"
	"testing"

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

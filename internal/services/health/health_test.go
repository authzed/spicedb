package health

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// fakeDatastoreChecker implements DatastoreChecker for testing
type fakeDatastoreChecker struct {
	readyState datastore.ReadyState
	err        error
}

func (f *fakeDatastoreChecker) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return f.readyState, f.err
}

// fakeDispatcher implements dispatch.Dispatcher for testing
type fakeDispatcher struct {
	readyState dispatch.ReadyState
}

func (f *fakeDispatcher) ReadyState() dispatch.ReadyState {
	return f.readyState
}

func (f *fakeDispatcher) Close() error { return nil }

// The following methods are required by dispatch.Dispatcher interface but not used in health tests
func (f *fakeDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeDispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return errors.New("not implemented")
}

func (f *fakeDispatcher) DispatchLookupResources2(req *v1.DispatchLookupResources2Request, stream dispatch.LookupResources2Stream) error {
	return errors.New("not implemented")
}

func TestNewHealthManager(t *testing.T) {
	dispatcher := &fakeDispatcher{}
	dsc := &fakeDatastoreChecker{}

	manager := NewHealthManager(dispatcher, dsc)

	require.NotNil(t, manager)
	require.NotNil(t, manager.HealthSvc())
}

func TestHealthManagerRegisterReportedService(t *testing.T) {
	dispatcher := &fakeDispatcher{}
	dsc := &fakeDatastoreChecker{}
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
			dispatcher := &fakeDispatcher{
				readyState: dispatch.ReadyState{
					IsReady: tc.dispatcherReady,
					Message: func() string {
						if tc.dispatcherReady {
							return "dispatcher ready"
						}
						return "dispatcher not ready"
					}(),
				},
			}

			dsc := &fakeDatastoreChecker{
				readyState: datastore.ReadyState{
					IsReady: tc.datastoreReady,
					Message: func() string {
						if tc.datastoreReady {
							return "datastore ready"
						}
						return "datastore not ready"
					}(),
				},
				err: tc.datastoreError,
			}

			hm := &healthManager{
				dispatcher: dispatcher,
				dsc:        dsc,
			}

			require.Equal(t, tc.expectedResult, hm.checkIsReady(t.Context()))
		})
	}
}

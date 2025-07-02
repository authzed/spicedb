package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
)

func TestWithDatastore(t *testing.T) {
	someLogger := zerolog.Nop()
	someAuthFunc := func(ctx context.Context) (context.Context, error) {
		return nil, fmt.Errorf("expected auth error")
	}
	var someDispatcher dispatch.Dispatcher

	opts := MiddlewareOption{
		someLogger,
		someAuthFunc,
		true,
		someDispatcher,
		true,
		true,
		false,
		"service",
		nil,
		nil,
	}

	someDS, err := memdb.NewMemdbDatastore(0, time.Hour, time.Hour)
	require.NoError(t, err)

	withDS := opts.WithDatastore(someDS)
	require.NotNil(t, withDS)
	require.NotNil(t, withDS.unaryDatastoreMiddleware)
	require.NotNil(t, withDS.streamDatastoreMiddleware)

	require.Equal(t, opts.Logger, withDS.Logger)
	require.Equal(t, opts.DispatcherForMiddleware, withDS.DispatcherForMiddleware)
	require.Equal(t, opts.EnableRequestLog, withDS.EnableRequestLog)
	require.Equal(t, opts.EnableResponseLog, withDS.EnableResponseLog)
	require.Equal(t, opts.DisableGRPCHistogram, withDS.DisableGRPCHistogram)
	require.Equal(t, opts.MiddlewareServiceLabel, withDS.MiddlewareServiceLabel)

	_, authError := withDS.AuthFunc(t.Context())
	require.Error(t, authError)
	require.ErrorContains(t, authError, "expected auth error")
}

func TestWithDatastoreMiddleware(t *testing.T) {
	someLogger := zerolog.Nop()
	someAuthFunc := func(ctx context.Context) (context.Context, error) {
		return nil, fmt.Errorf("expected auth error")
	}
	var someDispatcher dispatch.Dispatcher

	opts := MiddlewareOption{
		someLogger,
		someAuthFunc,
		true,
		someDispatcher,
		true,
		true,
		false,
		"anotherservice",
		nil,
		nil,
	}

	someMiddleware := pertoken.NewMiddleware(nil, caveattypes.Default.TypeSet)

	withDS := opts.WithDatastoreMiddleware(someMiddleware)
	require.NotNil(t, withDS)
	require.NotNil(t, withDS.unaryDatastoreMiddleware)
	require.NotNil(t, withDS.streamDatastoreMiddleware)

	require.Equal(t, opts.Logger, withDS.Logger)
	require.Equal(t, opts.DispatcherForMiddleware, withDS.DispatcherForMiddleware)
	require.Equal(t, opts.EnableRequestLog, withDS.EnableRequestLog)
	require.Equal(t, opts.EnableResponseLog, withDS.EnableResponseLog)
	require.Equal(t, opts.DisableGRPCHistogram, withDS.DisableGRPCHistogram)
	require.Equal(t, opts.MiddlewareServiceLabel, withDS.MiddlewareServiceLabel)

	_, authError := withDS.AuthFunc(t.Context())
	require.Error(t, authError)
	require.ErrorContains(t, authError, "expected auth error")
}

func TestRequestBodyLogger(t *testing.T) {
	ctx := t.Context()

	t.Run("nil_request", func(t *testing.T) {
		callMeta := interceptors.CallMeta{
			ReqOrNil: nil,
		}

		fields := requestBodyLogger(ctx, callMeta)
		require.Nil(t, fields)
	})

	t.Run("non-accepted_request_type", func(t *testing.T) {
		callMeta := interceptors.CallMeta{
			ReqOrNil: &v1.ExpandPermissionTreeRequest{},
		}

		fields := requestBodyLogger(ctx, callMeta)
		require.Nil(t, fields)
	})

	testCases := map[string]struct {
		request     any
		expectation func(t *testing.T, jsonString string)
	}{
		"valid_CheckPermissionRequest": {
			request: &v1.CheckPermissionRequest{
				Resource: &v1.ObjectReference{
					ObjectType: "document",
					ObjectId:   "sensitive_document_id",
				},
				Permission: "read",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "sensitive_user_id",
					},
				},
			},
			expectation: func(t *testing.T, jsonString string) {
				var unmarshaled v1.CheckPermissionRequest
				err := json.Unmarshal([]byte(jsonString), &unmarshaled)
				require.NoError(t, err)
				require.Equal(t, "*****", unmarshaled.Subject.Object.ObjectId)
				require.Equal(t, "*****", unmarshaled.Resource.ObjectId)
				require.Equal(t, "user", unmarshaled.Subject.Object.ObjectType)
				require.Equal(t, "document", unmarshaled.Resource.ObjectType)
				require.Equal(t, "read", unmarshaled.Permission)
			},
		},
		"valid_LookupResourcesRequest": {
			request: &v1.LookupResourcesRequest{
				ResourceObjectType: "document",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "sensitive_user_id",
					},
				},
			},
			expectation: func(t *testing.T, jsonString string) {
				var unmarshaled v1.LookupResourcesRequest
				err := json.Unmarshal([]byte(jsonString), &unmarshaled)
				require.NoError(t, err)
				require.Equal(t, "*****", unmarshaled.Subject.Object.ObjectId)
				require.Equal(t, "user", unmarshaled.Subject.Object.ObjectType)
				require.Equal(t, "document", unmarshaled.ResourceObjectType)
				require.Equal(t, "view", unmarshaled.Permission)
			},
		},
		"valid_LookupSubjectsRequest": {
			request: &v1.LookupSubjectsRequest{
				Resource: &v1.ObjectReference{
					ObjectType: "document",
					ObjectId:   "sensitive_document_id",
				},
				Permission:        "view",
				SubjectObjectType: "user",
			},
			expectation: func(t *testing.T, jsonString string) {
				var unmarshaled v1.LookupSubjectsRequest
				err := json.Unmarshal([]byte(jsonString), &unmarshaled)
				require.NoError(t, err)
				require.Equal(t, "*****", unmarshaled.Resource.ObjectId)
				require.Equal(t, "document", unmarshaled.Resource.ObjectType)
				require.Equal(t, "view", unmarshaled.Permission)
				require.Equal(t, "user", unmarshaled.SubjectObjectType)
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			callMeta := interceptors.CallMeta{
				ReqOrNil: tc.request,
			}

			fields := requestBodyLogger(ctx, callMeta)
			require.NotNil(t, fields)

			it := fields.Iterator()
			for it.Next() {
				k, v := it.At()
				require.Equal(t, "grpc.request", k)

				jsonString, ok := v.(string)
				require.True(t, ok)

				var unmarshaled any
				err := json.Unmarshal([]byte(jsonString), &unmarshaled)
				require.NoError(t, err)

				tc.expectation(t, jsonString)
			}
		})
	}
}

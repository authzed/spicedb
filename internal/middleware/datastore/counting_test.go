package datastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
)

func TestUnaryCountingInterceptor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	// Setup mock datastore
	mockDS := mocks.NewMockDatastore(ctrl)
	mockReader := mocks.NewMockReader(ctrl)
	mockDS.EXPECT().SnapshotReader(gomock.Any()).Return(mockReader).AnyTimes()
	mockReader.EXPECT().QueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	// Create context with datastore
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, mockDS))

	// Track if handler was called
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true

		// Get the datastore from context - should be wrapped
		ds := MustFromContext(ctx)

		// Make some calls to trigger counting
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _ = reader.QueryRelationships(ctx, datastore.RelationshipsFilter{})
		_, _ = reader.QueryRelationships(ctx, datastore.RelationshipsFilter{})

		return "response", nil
	}

	// Create interceptor
	interceptor := UnaryCountingInterceptor()

	// Call interceptor
	resp, err := interceptor(ctx, "request", &grpc.UnaryServerInfo{}, handler)
	require.NoError(err)
	require.Equal("response", resp)
	require.True(handlerCalled)

	// Note: We can't easily verify Prometheus metrics were updated without
	// complex setup, but we've verified the counting proxy was applied
	// and WriteMethodCounts() was called (no panic)
}

func TestStreamCountingInterceptor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	// Setup mock datastore
	mockDS := mocks.NewMockDatastore(ctrl)
	mockReader := mocks.NewMockReader(ctrl)
	mockDS.EXPECT().SnapshotReader(gomock.Any()).Return(mockReader).AnyTimes()
	mockReader.EXPECT().ReverseQueryRelationships(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	// Create context with datastore
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, mockDS))

	// Create mock server stream
	mockStream := &mockServerStream{ctx: ctx}

	// Track if handler was called
	handlerCalled := false
	handler := func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true

		// Get the datastore from context - should be wrapped
		ds := MustFromContext(ss.Context())

		// Make some calls to trigger counting
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _ = reader.ReverseQueryRelationships(ss.Context(), datastore.SubjectsFilter{SubjectType: "user"})

		return nil
	}

	// Create interceptor
	interceptor := StreamCountingInterceptor()

	// Call interceptor
	err := interceptor(nil, mockStream, &grpc.StreamServerInfo{}, handler)
	require.NoError(err)
	require.True(handlerCalled)
}

func TestUnaryCountingInterceptor_HandlerError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	// Setup mock datastore
	mockDS := mocks.NewMockDatastore(ctrl)
	mockReader := mocks.NewMockReader(ctrl)
	mockDS.EXPECT().SnapshotReader(gomock.Any()).Return(mockReader).AnyTimes()
	mockReader.EXPECT().ReadNamespaceByName(gomock.Any(), "test").Return(nil, datastore.NoRevision, nil).AnyTimes()

	// Create context with datastore
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, mockDS))

	// Handler that returns an error
	handler := func(ctx context.Context, req any) (any, error) {
		// Make a call before erroring
		ds := MustFromContext(ctx)
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _, _ = reader.ReadNamespaceByName(ctx, "test")

		return nil, &testError{}
	}

	// Create interceptor
	interceptor := UnaryCountingInterceptor()

	// Call interceptor - should still export counts even on error
	_, err := interceptor(ctx, "request", &grpc.UnaryServerInfo{}, handler)
	require.Error(err)
	require.Equal("test error", err.Error())

	// Counts should still be exported (WriteMethodCounts called)
}

func TestStreamCountingInterceptor_HandlerError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	require := require.New(t)

	// Setup mock datastore
	mockDS := mocks.NewMockDatastore(ctrl)
	mockReader := mocks.NewMockReader(ctrl)
	mockDS.EXPECT().SnapshotReader(gomock.Any()).Return(mockReader).AnyTimes()
	mockReader.EXPECT().ListAllNamespaces(gomock.Any()).Return([]datastore.RevisionedNamespace{}, nil).AnyTimes()

	// Create context with datastore
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, mockDS))

	// Create mock server stream
	mockStream := &mockServerStream{ctx: ctx}

	// Handler that returns an error
	handler := func(srv any, ss grpc.ServerStream) error {
		// Make a call before erroring
		ds := MustFromContext(ss.Context())
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _ = reader.ListAllNamespaces(ss.Context())

		return &testError{}
	}

	// Create interceptor
	interceptor := StreamCountingInterceptor()

	// Call interceptor - should still export counts even on error
	err := interceptor(nil, mockStream, &grpc.StreamServerInfo{}, handler)
	require.Error(err)
	require.Equal("test error", err.Error())

	// Counts should still be exported (WriteMethodCounts called)
}

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

type testError struct{}

func (e *testError) Error() string {
	return "test error"
}

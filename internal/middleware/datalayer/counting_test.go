package datalayer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestUnaryCountingInterceptor(t *testing.T) {
	require := require.New(t)

	// Setup mock datastore
	mockDS := &proxy_test.MockDatastore{}
	mockReader := &proxy_test.MockReader{}
	mockDS.On("SnapshotReader", mock.Anything).Return(mockReader)
	mockReader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	// Create context with datastore, wrapping mock in datalayer
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, datalayer.NewDataLayer(mockDS)))

	// Track if handler was called
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true

		// Get the datalayer from context - should be wrapped with counting
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

	mockDS.AssertExpectations(t)
	mockReader.AssertExpectations(t)
}

func TestStreamCountingInterceptor(t *testing.T) {
	require := require.New(t)

	// Setup mock datastore
	mockDS := &proxy_test.MockDatastore{}
	mockReader := &proxy_test.MockReader{}
	mockDS.On("SnapshotReader", mock.Anything).Return(mockReader)
	mockReader.On("ReverseQueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	// Create context with datastore, wrapping mock in datalayer
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, datalayer.NewDataLayer(mockDS)))

	// Create mock server stream
	mockStream := &mockServerStream{ctx: ctx}

	// Track if handler was called
	handlerCalled := false
	handler := func(srv any, ss grpc.ServerStream) error {
		handlerCalled = true

		// Get the datalayer from context - should be wrapped with counting
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

	mockDS.AssertExpectations(t)
	mockReader.AssertExpectations(t)
}

func TestUnaryCountingInterceptor_HandlerError(t *testing.T) {
	require := require.New(t)

	// Setup mock datastore
	mockDS := &proxy_test.MockDatastore{}
	mockReader := &proxy_test.MockReader{}
	mockDS.On("SnapshotReader", mock.Anything).Return(mockReader)
	mockReader.On("QueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	// Create context with datastore, wrapping mock in datalayer
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, datalayer.NewDataLayer(mockDS)))

	// Handler that returns an error
	handler := func(ctx context.Context, req any) (any, error) {
		// Make a call before erroring
		ds := MustFromContext(ctx)
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _ = reader.QueryRelationships(ctx, datastore.RelationshipsFilter{})

		return nil, &testError{}
	}

	// Create interceptor
	interceptor := UnaryCountingInterceptor()

	// Call interceptor - should still export counts even on error
	_, err := interceptor(ctx, "request", &grpc.UnaryServerInfo{}, handler)
	require.Error(err)
	require.Equal("test error", err.Error())

	// Counts should still be exported
	mockDS.AssertExpectations(t)
	mockReader.AssertExpectations(t)
}

func TestStreamCountingInterceptor_HandlerError(t *testing.T) {
	require := require.New(t)

	// Setup mock datastore
	mockDS := &proxy_test.MockDatastore{}
	mockReader := &proxy_test.MockReader{}
	mockDS.On("SnapshotReader", mock.Anything).Return(mockReader)
	mockReader.On("ReverseQueryRelationships", mock.Anything, mock.Anything).Return(nil, nil)

	// Create context with datastore, wrapping mock in datalayer
	ctx := ContextWithHandle(context.Background())
	require.NoError(SetInContext(ctx, datalayer.NewDataLayer(mockDS)))

	// Create mock server stream
	mockStream := &mockServerStream{ctx: ctx}

	// Handler that returns an error
	handler := func(srv any, ss grpc.ServerStream) error {
		// Make a call before erroring
		ds := MustFromContext(ss.Context())
		reader := ds.SnapshotReader(datastore.NoRevision)
		_, _ = reader.ReverseQueryRelationships(ss.Context(), datastore.SubjectsFilter{SubjectType: "user"})

		return &testError{}
	}

	// Create interceptor
	interceptor := StreamCountingInterceptor()

	// Call interceptor - should still export counts even on error
	err := interceptor(nil, mockStream, &grpc.StreamServerInfo{}, handler)
	require.Error(err)
	require.Equal("test error", err.Error())

	// Counts should still be exported
	mockDS.AssertExpectations(t)
	mockReader.AssertExpectations(t)
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

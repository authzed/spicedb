package consistency

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestSetFullConsistencyRevisionToContext(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when no handle in context", func(t *testing.T) {
		t.Parallel()
		err := setFullConsistencyRevisionToContext(context.Background(), &v1.ReadRelationshipsRequest{}, nil, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)
	})

	t.Run("sets head revision for request with consistency", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, nil).Once()

		ctx := ContextWithHandle(t.Context())
		ctx = datastoremw.ContextWithDatastore(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.NoError(err)

		rev, _, err := RevisionFromContext(ctx)
		require.NoError(err)
		require.True(head.Equal(rev))

		ds.AssertExpectations(t)
	})

	t.Run("sets head revision without service label", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, nil).Once()

		ctx := ContextWithHandle(t.Context())
		ctx = datastoremw.ContextWithDatastore(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(err)

		rev, _, err := RevisionFromContext(ctx)
		require.NoError(err)
		require.True(head.Equal(rev))

		ds.AssertExpectations(t)
	})

	t.Run("returns nil for request without consistency interface", func(t *testing.T) {
		t.Parallel()
		ctx := ContextWithHandle(t.Context())
		err := setFullConsistencyRevisionToContext(ctx, "not-a-consistency-request", nil, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, errors.New("datastore unavailable")).Once()

		ctx := ContextWithHandle(t.Context())
		ctx = datastoremw.ContextWithDatastore(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)

		ds.AssertExpectations(t)
	})
}

func TestForceFullConsistencyUnaryServerInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("bypasses whitelisted reflection service", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "response", nil
		}

		resp, err := interceptor(
			context.Background(),
			&v1.ReadRelationshipsRequest{},
			&grpc.UnaryServerInfo{FullMethod: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"},
			handler,
		)
		require.NoError(err)
		require.Equal("response", resp)
		require.True(handlerCalled)
	})

	t.Run("bypasses whitelisted health service", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "response", nil
		}

		resp, err := interceptor(
			context.Background(),
			&v1.ReadRelationshipsRequest{},
			&grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
			handler,
		)
		require.NoError(err)
		require.Equal("response", resp)
		require.True(handlerCalled)
	})

	t.Run("sets full consistency revision for non-bypass method", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, nil).Once()

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), ds)

		var capturedCtx context.Context
		handler := func(ctx context.Context, req any) (any, error) {
			capturedCtx = ctx
			return "response", nil
		}

		resp, err := interceptor(
			ctx,
			&v1.ReadRelationshipsRequest{},
			&grpc.UnaryServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			handler,
		)
		require.NoError(err)
		require.Equal("response", resp)

		rev, _, err := RevisionFromContext(capturedCtx)
		require.NoError(err)
		require.True(head.Equal(rev))

		ds.AssertExpectations(t)
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, errors.New("datastore unavailable")).Once()

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), ds)

		_, err := interceptor(
			ctx,
			&v1.ReadRelationshipsRequest{},
			&grpc.UnaryServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			func(ctx context.Context, req any) (any, error) {
				t.Fatal("handler should not be called on error")
				return nil, nil
			},
		)
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)

		ds.AssertExpectations(t)
	})

	t.Run("does not call HeadRevision for non-consistency request", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &proxy_test.MockDatastore{}
		// No HeadRevision expectation — should not be called.

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), ds)

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "response", nil
		}

		resp, err := interceptor(
			ctx,
			"not-a-consistency-request",
			&grpc.UnaryServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/SomeMethod"},
			handler,
		)
		require.NoError(err)
		require.Equal("response", resp)
		require.True(handlerCalled)

		ds.AssertExpectations(t)
	})
}

// mockServerStream implements grpc.ServerStream for testing purposes.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}

func (m *mockServerStream) RecvMsg(msg any) error {
	return nil
}

func (m *mockServerStream) SendMsg(msg any) error {
	return nil
}

func TestForceFullConsistencyStreamServerInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("bypasses whitelisted reflection service", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

		handlerCalled := false
		handler := func(srv any, stream grpc.ServerStream) error {
			handlerCalled = true
			return nil
		}

		stream := &mockServerStream{ctx: context.Background()}

		err := interceptor(
			nil,
			stream,
			&grpc.StreamServerInfo{FullMethod: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"},
			handler,
		)
		require.NoError(err)
		require.True(handlerCalled)
	})

	t.Run("bypasses whitelisted health service", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

		handlerCalled := false
		handler := func(srv any, stream grpc.ServerStream) error {
			handlerCalled = true
			return nil
		}

		stream := &mockServerStream{ctx: context.Background()}

		err := interceptor(
			nil,
			stream,
			&grpc.StreamServerInfo{FullMethod: "/grpc.health.v1.Health/Watch"},
			handler,
		)
		require.NoError(err)
		require.True(handlerCalled)
	})

	t.Run("wraps stream with recvWrapper for non-bypass method", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, nil).Once()

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), ds)
		stream := &mockServerStream{ctx: ctx}

		var capturedStream grpc.ServerStream
		handler := func(srv any, stream grpc.ServerStream) error {
			capturedStream = stream
			return nil
		}

		err := interceptor(
			nil,
			stream,
			&grpc.StreamServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			handler,
		)
		require.NoError(err)

		// The handler should have received a recvWrapper, not the original stream.
		wrapper, ok := capturedStream.(*recvWrapper)
		require.True(ok, "expected stream to be wrapped in recvWrapper")

		// Simulate receiving a message to trigger setFullConsistencyRevisionToContext.
		err = wrapper.RecvMsg(&v1.ReadRelationshipsRequest{})
		require.NoError(err)

		rev, _, err := RevisionFromContext(wrapper.Context())
		require.NoError(err)
		require.True(head.Equal(rev))

		ds.AssertExpectations(t)
	})

	t.Run("recvWrapper returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &proxy_test.MockDatastore{}
		ds.On("HeadRevision").Return(head, errors.New("datastore unavailable")).Once()

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), ds)
		stream := &mockServerStream{ctx: ctx}

		var capturedStream grpc.ServerStream
		handler := func(srv any, stream grpc.ServerStream) error {
			capturedStream = stream
			return nil
		}

		err := interceptor(
			nil,
			stream,
			&grpc.StreamServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			handler,
		)
		require.NoError(t, err)

		// Simulate receiving a message — should return an error.
		wrapper := capturedStream.(*recvWrapper)
		err = wrapper.RecvMsg(&v1.ReadRelationshipsRequest{})
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)

		ds.AssertExpectations(t)
	})

	t.Run("recvWrapper propagates upstream RecvMsg errors", func(t *testing.T) {
		t.Parallel()

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

		ctx := datastoremw.ContextWithDatastore(t.Context(), &proxy_test.MockDatastore{})

		upstreamErr := errors.New("upstream recv error")
		failingStream := &failingRecvStream{ctx: ctx, err: upstreamErr}

		var capturedStream grpc.ServerStream
		handler := func(srv any, stream grpc.ServerStream) error {
			capturedStream = stream
			return nil
		}

		err := interceptor(
			nil,
			failingStream,
			&grpc.StreamServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			handler,
		)
		require.NoError(t, err)

		wrapper := capturedStream.(*recvWrapper)
		err = wrapper.RecvMsg(&v1.ReadRelationshipsRequest{})
		require.ErrorIs(t, err, upstreamErr)
	})
}

// failingRecvStream is a server stream whose RecvMsg always returns an error.
type failingRecvStream struct {
	grpc.ServerStream
	ctx context.Context
	err error
}

func (f *failingRecvStream) Context() context.Context {
	return f.ctx
}

func (f *failingRecvStream) RecvMsg(_ any) error {
	return f.err
}

func TestForceFullConsistencyUnaryBypassWhitelist(t *testing.T) {
	t.Parallel()

	// Table-driven test covering all bypass prefixes.
	bypassMethods := []struct {
		name   string
		method string
	}{
		{"reflection v1alpha", "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"},
		{"reflection v1", "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"},
		{"health", "/grpc.health.v1.Health/Check"},
	}

	for _, tc := range bypassMethods {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

			handlerCalled := false
			resp, err := interceptor(
				context.Background(),
				&v1.ReadRelationshipsRequest{},
				&grpc.UnaryServerInfo{FullMethod: tc.method},
				func(ctx context.Context, req any) (any, error) {
					handlerCalled = true
					return "bypassed", nil
				},
			)
			require.NoError(err)
			require.Equal("bypassed", resp)
			require.True(handlerCalled)
		})
	}
}

func TestForceFullConsistencyStreamBypassWhitelist(t *testing.T) {
	t.Parallel()

	bypassMethods := []struct {
		name   string
		method string
	}{
		{"reflection v1alpha", "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"},
		{"reflection v1", "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"},
		{"health", "/grpc.health.v1.Health/Watch"},
	}

	for _, tc := range bypassMethods {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")

			handlerCalled := false
			stream := &mockServerStream{ctx: context.Background()}

			err := interceptor(
				nil,
				stream,
				&grpc.StreamServerInfo{FullMethod: tc.method},
				func(srv any, stream grpc.ServerStream) error {
					handlerCalled = true
					return nil
				},
			)
			require.NoError(err)
			require.True(handlerCalled)
		})
	}
}

func TestSetFullConsistencyRevisionToContextWithReadonlyError(t *testing.T) {
	t.Parallel()

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, datastore.NewReadonlyErr()).Once()

	ctx := ContextWithHandle(t.Context())
	ctx = datastoremw.ContextWithDatastore(ctx, ds)

	err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unavailable, err)

	ds.AssertExpectations(t)
}

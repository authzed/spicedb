package consistency

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

// testRevision is a simple revision type for testing.
type testRevision uint64

func (r testRevision) String() string     { return fmt.Sprintf("%d", r) }
func (r testRevision) ByteSortable() bool { return false }
func (r testRevision) Equal(other datastore.Revision) bool {
	o, ok := other.(testRevision)
	return ok && r == o
}

func (r testRevision) GreaterThan(other datastore.Revision) bool {
	return r > other.(testRevision)
}

func (r testRevision) LessThan(other datastore.Revision) bool {
	return r < other.(testRevision)
}

var headRev testRevision = 145

// fakeDataLayer implements datalayer.DataLayer with a configurable HeadRevision.
type fakeDataLayer struct {
	datalayer.DataLayer
	headRevision datastore.Revision
	headErr      error
}

func (f *fakeDataLayer) HeadRevision(_ context.Context) (datastore.Revision, error) {
	return f.headRevision, f.headErr
}

func (f *fakeDataLayer) UniqueID(_ context.Context) (string, error) {
	return "fake-ds", nil
}

// NOTE: testpb.InterceptorTestSuite cannot be used here because testpb requests don't
// implement hasConsistency. The unit tests below cover the behavior by testing interceptors directly.

func TestSetFullConsistencyRevisionToContext(t *testing.T) {
	t.Parallel()

	t.Run("returns nil when no handle in context", func(t *testing.T) {
		t.Parallel()
		ds := &fakeDataLayer{headRevision: headRev}
		ctx := context.Background()
		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)

		rev, _, err := RevisionFromContext(ctx)
		require.Error(t, err)
		require.Nil(t, rev)
	})

	t.Run("sets head revision for request with consistency", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &fakeDataLayer{headRevision: headRev}
		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.NoError(err)

		rev, _, err := RevisionFromContext(ctx)
		require.NoError(err)
		require.True(headRev.Equal(rev))
	})

	t.Run("sets head revision without service label", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &fakeDataLayer{headRevision: headRev}
		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(err)

		rev, _, err := RevisionFromContext(ctx)
		require.NoError(err)
		require.True(headRev.Equal(rev))
	})

	t.Run("returns nil for request without consistency interface", func(t *testing.T) {
		t.Parallel()

		// Use a failing datastore to verify HeadRevision is never called.
		ds := &fakeDataLayer{headErr: errors.New("should not be called")}
		ctx := ContextWithHandle(t.Context())
		err := setFullConsistencyRevisionToContext(ctx, "not-a-consistency-request", ds, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &fakeDataLayer{headErr: errors.New("datastore unavailable")}
		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, ds)

		err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)
	})
}

func TestForceFullConsistencyUnaryServerInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("sets full consistency revision for non-bypass method", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &fakeDataLayer{headRevision: headRev}
		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)

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
		require.True(headRev.Equal(rev))
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &fakeDataLayer{headErr: errors.New("datastore unavailable")}
		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)

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
	})

	t.Run("does not call HeadRevision for non-consistency request", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		// Use a datastore that would fail if HeadRevision were called.
		ds := &fakeDataLayer{headErr: errors.New("should not be called")}
		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)

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
	})
}

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context { return m.ctx }
func (m *mockServerStream) RecvMsg(msg any) error    { return nil }

type failingRecvStream struct {
	grpc.ServerStream
	ctx context.Context
	err error
}

func (f *failingRecvStream) Context() context.Context { return f.ctx }
func (f *failingRecvStream) RecvMsg(_ any) error      { return f.err }

func TestForceFullConsistencyStreamServerInterceptor(t *testing.T) {
	t.Parallel()

	t.Run("wraps stream with recvWrapper for non-bypass method", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)

		ds := &fakeDataLayer{headRevision: headRev}
		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)
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

		wrapper, ok := capturedStream.(*recvWrapper)
		require.True(ok, "expected stream to be wrapped in recvWrapper")

		err = wrapper.RecvMsg(&v1.ReadRelationshipsRequest{})
		require.NoError(err)

		rev, _, err := RevisionFromContext(wrapper.Context())
		require.NoError(err)
		require.True(headRev.Equal(rev))
	})

	t.Run("recvWrapper returns error when HeadRevision fails", func(t *testing.T) {
		t.Parallel()

		ds := &fakeDataLayer{headErr: errors.New("datastore unavailable")}
		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)
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

		wrapper := capturedStream.(*recvWrapper)
		err = wrapper.RecvMsg(&v1.ReadRelationshipsRequest{})
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)
	})

	t.Run("recvWrapper propagates upstream RecvMsg errors", func(t *testing.T) {
		t.Parallel()

		ds := &fakeDataLayer{headErr: errors.New("should not be called")}
		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), ds)
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

func TestForceFullConsistencyUnaryBypassWhitelist(t *testing.T) {
	t.Parallel()

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

			var capturedCtx context.Context
			resp, err := interceptor(
				context.Background(),
				&v1.ReadRelationshipsRequest{},
				&grpc.UnaryServerInfo{FullMethod: tc.method},
				func(ctx context.Context, req any) (any, error) {
					capturedCtx = ctx
					return "bypassed", nil
				},
			)
			require.NoError(err)
			require.Equal("bypassed", resp)

			rev, _, err := RevisionFromContext(capturedCtx)
			require.Error(err)
			require.Nil(rev)
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
			originalStream := &mockServerStream{ctx: context.Background()}

			var capturedStream grpc.ServerStream
			err := interceptor(
				nil,
				originalStream,
				&grpc.StreamServerInfo{FullMethod: tc.method},
				func(srv any, stream grpc.ServerStream) error {
					capturedStream = stream
					return nil
				},
			)
			require.NoError(err)
			require.Equal(originalStream, capturedStream, "expected handler to receive original stream, not a wrapper")
		})
	}
}

func TestSetFullConsistencyRevisionToContextWithReadonlyError(t *testing.T) {
	t.Parallel()

	ds := &fakeDataLayer{headErr: datastore.NewReadonlyErr()}
	ctx := ContextWithHandle(t.Context())
	ctx = datalayer.ContextWithDataLayer(ctx, ds)

	err := setFullConsistencyRevisionToContext(ctx, &v1.ReadRelationshipsRequest{}, ds, "somelabel", TreatMismatchingTokensAsFullConsistency)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unavailable, err)
}

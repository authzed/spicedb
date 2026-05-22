package consistency

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/datalayer"
	mock_datalayer "github.com/authzed/spicedb/pkg/datalayer/mocks"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
)

type (
	requestWithoutConsistency struct{}
	requestWithConsistency    struct{}
)

var _ hasConsistency = (*requestWithConsistency)(nil)

func (r requestWithConsistency) GetConsistency() *v1.Consistency {
	return &v1.Consistency{}
}

// NOTE: testpb.InterceptorTestSuite cannot be used here because testpb requests don't
// implement hasConsistency. The unit tests below cover the behavior by testing interceptors directly.

func TestSetFullConsistencyRevisionToContext(t *testing.T) {
	t.Run("returns nil when no handle in context", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		ctx := t.Context()

		err := setFullConsistencyRevisionToContext(ctx, &requestWithConsistency{}, dl, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)

		rev, _, _, err := RevisionFromContext(ctx)
		require.Error(t, err)
		require.Nil(t, rev)
	})

	t.Run("sets head revision for request with consistency", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		mockRev := mocks.NewMockRevision(ctrl)
		mockRev.EXPECT().String().Return("a revision").Times(1)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(mockRev, datalayer.NoSchemaHashInLegacyMode, nil).Times(1)
		dl.EXPECT().UniqueID(gomock.Any()).Return("uniqueid", nil).Times(1)

		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, dl)

		err := setFullConsistencyRevisionToContext(ctx, &requestWithConsistency{}, dl, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)

		rev, _, _, err := RevisionFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t, mockRev, rev)
	})

	t.Run("sets head revision without service label", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		mockRev := mocks.NewMockRevision(ctrl)
		mockRev.EXPECT().String().Return("a revision").Times(1)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(mockRev, datalayer.NoSchemaHashInLegacyMode, nil).Times(1)
		dl.EXPECT().UniqueID(gomock.Any()).Return("uniqueid", nil).Times(1)

		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, dl)

		err := setFullConsistencyRevisionToContext(ctx, &requestWithConsistency{}, dl, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)

		rev, _, _, err := RevisionFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t, mockRev, rev)
	})

	t.Run("returns nil for request without consistency interface", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)

		ctx := ContextWithHandle(t.Context())

		err := setFullConsistencyRevisionToContext(ctx, &requestWithoutConsistency{}, dl, "", TreatMismatchingTokensAsFullConsistency)
		require.NoError(t, err)
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(nil, datalayer.NoSchemaHashInLegacyMode, errors.New("some error")).Times(1)

		ctx := ContextWithHandle(t.Context())
		ctx = datalayer.ContextWithDataLayer(ctx, dl)

		err := setFullConsistencyRevisionToContext(ctx, &requestWithConsistency{}, dl, "somelabel", TreatMismatchingTokensAsFullConsistency)
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)
	})
}

func TestForceFullConsistencyUnaryServerInterceptor(t *testing.T) {
	t.Run("sets full consistency revision for non-bypass method", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		mockRev := mocks.NewMockRevision(ctrl)
		mockRev.EXPECT().String().Return("a revision").Times(1)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(mockRev, datalayer.NoSchemaHashInLegacyMode, nil).Times(1)
		dl.EXPECT().UniqueID(gomock.Any()).Return("uniqueid", nil).Times(1)
		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)

		var capturedCtx context.Context
		handler := func(ctx context.Context, req any) (any, error) {
			capturedCtx = ctx
			return "response", nil
		}

		resp, err := interceptor(
			ctx,
			&requestWithConsistency{},
			&grpc.UnaryServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/ReadRelationships"},
			handler,
		)
		require.NoError(t, err)
		require.Equal(t, "response", resp)

		rev, _, _, err := RevisionFromContext(capturedCtx)
		require.NoError(t, err)
		require.Equal(t, mockRev, rev)
	})

	t.Run("returns error when HeadRevision fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(nil, datalayer.NoSchemaHashInLegacyMode, errors.New("some error")).Times(1)

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)

		_, err := interceptor(
			ctx,
			&requestWithConsistency{},
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
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)

		interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)

		handlerCalled := false
		handler := func(ctx context.Context, req any) (any, error) {
			handlerCalled = true
			return "response", nil
		}

		resp, err := interceptor(
			ctx,
			&requestWithoutConsistency{},
			&grpc.UnaryServerInfo{FullMethod: "/authzed.api.v1.PermissionsService/SomeMethod"},
			handler,
		)
		require.NoError(t, err)
		require.Equal(t, "response", resp)
		require.True(t, handlerCalled)
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
	t.Run("wraps stream with recvWrapper for non-bypass method", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		mockRev := mocks.NewMockRevision(ctrl)
		mockRev.EXPECT().String().Return("a revision").Times(1)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(mockRev, datalayer.NoSchemaHashInLegacyMode, nil).Times(1)
		dl.EXPECT().UniqueID(gomock.Any()).Return("uniqueid", nil).Times(1)

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
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

		wrapper, ok := capturedStream.(*recvWrapper)
		require.True(t, ok, "expected stream to be wrapped in recvWrapper")

		err = wrapper.RecvMsg(&requestWithConsistency{})
		require.NoError(t, err)

		rev, _, _, err := RevisionFromContext(wrapper.Context())
		require.NoError(t, err)
		require.Equal(t, mockRev, rev)
	})

	t.Run("recvWrapper returns error when HeadRevision fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)
		dl.EXPECT().HeadRevision(gomock.Any()).Return(nil, datalayer.NoSchemaHashInLegacyMode, errors.New("some error")).Times(1)

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
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
		err = wrapper.RecvMsg(&requestWithConsistency{})
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Internal, err)
	})

	t.Run("recvWrapper propagates upstream RecvMsg errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		dl := mock_datalayer.NewMockDataLayer(ctrl)

		interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
		ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
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
		err = wrapper.RecvMsg(&requestWithConsistency{})
		require.ErrorIs(t, err, upstreamErr)
	})
}

func TestForceFullConsistencyUnaryBypassWhitelist(t *testing.T) {
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
			interceptor := ForceFullConsistencyUnaryServerInterceptor("somelabel")

			var capturedCtx context.Context
			resp, err := interceptor(
				t.Context(),
				&requestWithConsistency{},
				&grpc.UnaryServerInfo{FullMethod: tc.method},
				func(ctx context.Context, req any) (any, error) {
					capturedCtx = ctx
					return "bypassed", nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, "bypassed", resp)

			rev, _, _, err := RevisionFromContext(capturedCtx)
			require.Error(t, err)
			require.Nil(t, rev)
		})
	}
}

func TestForceFullConsistencyStreamBypassWhitelist(t *testing.T) {
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
			interceptor := ForceFullConsistencyStreamServerInterceptor("somelabel")
			originalStream := &mockServerStream{ctx: t.Context()}

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
			require.NoError(t, err)
			require.Equal(t, originalStream, capturedStream, "expected handler to receive original stream, not a wrapper")
		})
	}
}

func TestSetFullConsistencyRevisionToContextWithReadonlyError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dl := mock_datalayer.NewMockDataLayer(ctrl)
	dl.EXPECT().HeadRevision(gomock.Any()).Return(nil, datalayer.NoSchemaHashInLegacyMode, datastore.NewReadonlyErr()).Times(1)

	ctx := ContextWithHandle(t.Context())
	ctx = datalayer.ContextWithDataLayer(ctx, dl)

	err := setFullConsistencyRevisionToContext(ctx, &requestWithConsistency{}, dl, "somelabel", TreatMismatchingTokensAsFullConsistency)
	require.Error(t, err)
	grpcutil.RequireStatus(t, codes.Unavailable, err)
}

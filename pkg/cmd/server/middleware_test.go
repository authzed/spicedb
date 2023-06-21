package server

import (
	"context"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestInvalidModification(t *testing.T) {
	for _, tt := range []struct {
		name string
		mod  MiddlewareModification[grpc.UnaryServerInterceptor]
		err  string
	}{
		{
			name: "invalid operation without dependency",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation: OperationReplace,
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Middleware: nil,
					},
				},
			},
			err: "without a dependency",
		},
		{
			name: "valid replace all without dependency",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation: OperationReplaceAllUnsafe,
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "foobar",
						Middleware: nil,
					},
				},
			},
		},
		{
			name: "invalid unnamed middleware",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "foobar",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Middleware: nil,
					},
				},
			},
			err: "unnamed middleware",
		},
		{
			name: "invalid modification with duplicate middlewares",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "foobar",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "foo",
						Middleware: nil,
					},
					{
						Name:       "foo",
						Middleware: nil,
					},
				},
			},
			err: "duplicate names in middleware modification",
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mod.validate()
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewMiddlewareChain(t *testing.T) {
	mc, err := NewMiddlewareChain(ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		Name:       "foobar",
		Middleware: nil,
	})
	require.NoError(t, err)
	require.Len(t, mc.chain, 1)

	_, err = NewMiddlewareChain(ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		Middleware: nil,
	})
	require.ErrorContains(t, err, "unnamed middleware")
}

func TestChainValidate(t *testing.T) {
	mc, err := NewMiddlewareChain(ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		Name:       "public",
		Middleware: nil,
	}, ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		Name:       "internal",
		Internal:   true,
		Middleware: nil,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name string
		mod  MiddlewareModification[grpc.UnaryServerInterceptor]
		err  string
	}{
		{
			name: "invalid modification on non-existing dependency",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "doesnotexist",
			},
			err: "dependency does not exist",
		},
		{
			name: "invalid modification that causes a duplicate",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "public",
						Middleware: nil,
					},
				},
			},
			err: "will cause a duplicate",
		},
		{
			name: "invalid replace of an internal middlewares",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "internal",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "foobar",
						Middleware: nil,
					},
				},
			},
			err: "attempts to replace an internal middleware",
		},
		{
			name: "valid replace of a public middleware",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "foobar",
						Middleware: nil,
					},
				},
			},
		},
		{
			name: "valid replace of a public middleware with same name",
			mod: MiddlewareModification[grpc.UnaryServerInterceptor]{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "public",
						Middleware: nil,
					},
				},
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := mc.validate(tt.mod)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReplaceAllMiddleware(t *testing.T) {
	// Test fully replacing default Middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	mod := MiddlewareModification[grpc.UnaryServerInterceptor]{
		Operation: OperationReplaceAllUnsafe,
		Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "foobar",
				Middleware: replaceUnary,
			},
		},
	}

	mc, err := NewMiddlewareChain[grpc.UnaryServerInterceptor]()
	require.NoError(t, err)

	err = mc.modify(mod)
	require.NoError(t, err)

	outUnary := mc.ToGRPCInterceptors()
	require.NoError(t, err)

	expectedReplaceUnary, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplaceUnary, _ := outUnary[0](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplaceUnary, receivedReplaceUnary)
}

func TestPrependMiddleware(t *testing.T) {
	// Test prepending and appending to Default middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	var prependUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 2}.unaryIntercept
	defaultMiddleware, err := NewMiddlewareChain(
		ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			Name:       "foobar",
			Middleware: replaceUnary,
		})
	require.NoError(t, err)

	mod := MiddlewareModification[grpc.UnaryServerInterceptor]{
		Operation:                OperationPrepend,
		DependencyMiddlewareName: "foobar",
		Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "prepended",
				Middleware: prependUnary,
			},
		},
	}
	err = defaultMiddleware.modify(mod)
	require.NoError(t, err)

	// testing function equality is not possible, so we test the results of executing the functions
	outUnary := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 2)

	expectedPrepend, _ := prependUnary(context.Background(), nil, nil, nil)
	receivedPrepend, _ := outUnary[0](context.Background(), nil, nil, nil)
	require.Equal(t, expectedPrepend, receivedPrepend)

	expectedReplace, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplace, _ := outUnary[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.NotEqual(t, receivedPrepend, receivedReplace)
}

func TestAppendMiddleware(t *testing.T) {
	// Test prepending and appending to Default middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	var appendUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 3}.unaryIntercept
	defaultMiddleware := &MiddlewareChain[grpc.UnaryServerInterceptor]{
		chain: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "foobar",
				Middleware: replaceUnary,
			},
		},
	}
	mod := MiddlewareModification[grpc.UnaryServerInterceptor]{
		Operation:                OperationAppend,
		DependencyMiddlewareName: "foobar",
		Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "appended",
				Middleware: appendUnary,
			},
		},
	}
	err := defaultMiddleware.modify(mod)
	require.NoError(t, err)

	// testing function equality is not possible, so we test the results of executing the functions
	outUnary := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 2)

	expectedReplace, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplace, _ := outUnary[0](context.Background(), nil, nil, nil)
	expectedAppend, _ := appendUnary(context.Background(), nil, nil, nil)
	receivedAppend, _ := outUnary[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.Equal(t, expectedAppend, receivedAppend)
}

func TestDeleteMiddleware(t *testing.T) {
	defaultMiddleware := &MiddlewareChain[grpc.UnaryServerInterceptor]{
		chain: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "foobar",
				Middleware: mockUnaryInterceptor{}.unaryIntercept,
			},
		},
	}
	mod := MiddlewareModification[grpc.UnaryServerInterceptor]{
		Operation:                OperationReplace,
		DependencyMiddlewareName: "foobar",
	}
	err := defaultMiddleware.modify(mod)
	require.NoError(t, err)

	outUnary := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 0)
}

func TestCannotReplaceInternalMiddleware(t *testing.T) {
	defaultMiddleware := &MiddlewareChain[grpc.UnaryServerInterceptor]{
		chain: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
			{
				Name:       "foobar",
				Internal:   true,
				Middleware: mockUnaryInterceptor{}.unaryIntercept,
			},
		},
	}
	mod := MiddlewareModification[grpc.UnaryServerInterceptor]{
		Operation:                OperationReplace,
		DependencyMiddlewareName: "foobar",
	}
	err := defaultMiddleware.modify(mod)
	require.ErrorContains(t, err, "replace an internal middleware")
}

type mockUnaryInterceptor struct {
	val int
}

func (m mockUnaryInterceptor) unaryIntercept(_ context.Context, _ interface{}, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (resp interface{}, err error) {
	return m.val, nil
}

type mockStreamInterceptor struct {
	val error
}

func (m mockStreamInterceptor) streamIntercept(_ interface{}, _ grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
	return m.val
}

func TestMiddlewareOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithBootstrapFiles("testdata/test_schema.yaml"),
		datastore.WithRequestHedgingEnabled(false),
	)
	require.NoError(t, err)

	c := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
	)
	rs, err := c.Complete(ctx)
	require.NoError(t, err)

	clientConn, err := rs.GRPCDialContext(ctx)
	require.NoError(t, err)

	psc := v1.NewPermissionsServiceClient(clientConn)

	go func() {
		_ = rs.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	req := &v1.CheckPermissionRequest{
		Resource: &v1.ObjectReference{
			ObjectType: "resource",
			ObjectId:   "resource1",
		},
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "user1",
			},
		},
		Permission: "read",
	}

	_, err = psc.CheckPermission(ctx, req)
	require.NoError(t, err)

	lrreq := &v1.LookupResourcesRequest{
		ResourceObjectType: "resource",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "user1",
			},
		},
		Permission: "read",
	}
	lrc, err := psc.LookupResources(ctx, lrreq)
	require.NoError(t, err)

	_, err = lrc.Recv()
	require.NoError(t, err)
}

func TestIncorrectOrderAssertionFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithBootstrapFiles("testdata/test_schema.yaml"),
		datastore.WithRequestHedgingEnabled(false),
	)
	require.NoError(t, err)
	noopUnary := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		return nil, nil
	}
	noopStreaming := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}

	c := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		SetUnaryMiddlewareModification([]MiddlewareModification[grpc.UnaryServerInterceptor]{
			{
				Operation: OperationReplaceAllUnsafe,
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					NewUnaryMiddleware().
						WithName("test").
						WithInterceptor(noopUnary).
						EnsureAlreadyExecuted("does-not-exist").
						Done(),
				},
			},
		}),
		SetStreamingMiddlewareModification([]MiddlewareModification[grpc.StreamServerInterceptor]{
			{
				Operation: OperationReplaceAllUnsafe,
				Middlewares: []ReferenceableMiddleware[grpc.StreamServerInterceptor]{
					NewStreamMiddleware().
						WithName("test").
						WithInterceptor(noopStreaming).
						EnsureWrapperAlreadyExecuted("does-not-exist").
						Done(),
				},
			},
		}),
	)
	rs, err := c.Complete(ctx)
	require.NoError(t, err)

	clientConn, err := rs.GRPCDialContext(ctx)
	require.NoError(t, err)

	psc := v1.NewPermissionsServiceClient(clientConn)

	go func() {
		_ = rs.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	req := &v1.CheckPermissionRequest{
		Resource: &v1.ObjectReference{
			ObjectType: "resource",
			ObjectId:   "resource1",
		},
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "user1",
			},
		},
		Permission: "read",
	}

	_, err = psc.CheckPermission(ctx, req)
	require.ErrorContains(t, err, "expected interceptor does-not-exist to be already executed")

	lrreq := &v1.LookupResourcesRequest{
		ResourceObjectType: "resource",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "user1",
			},
		},
		Permission: "read",
	}

	lrc, err := psc.LookupResources(ctx, lrreq)
	require.NoError(t, err)

	_, err = lrc.Recv()
	require.ErrorContains(t, err, "expected interceptor does-not-exist to be already executed")
}

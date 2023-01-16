package server

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestInvalidModification(t *testing.T) {
	for _, tt := range []struct {
		name string
		mod  MiddlewareModification
		err  string
	}{
		{
			name: "invalid operation without dependency",
			mod: MiddlewareModification{
				Operation: OperationReplace,
				Middlewares: []ReferenceableMiddleware{
					{
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
			err: "without a dependency",
		},
		{
			name: "valid replace all without dependency",
			mod: MiddlewareModification{
				Operation: OperationReplaceAllUnsafe,
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "foobar",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
		},
		{
			name: "invalid unnamed middleware",
			mod: MiddlewareModification{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "foobar",
				Middlewares: []ReferenceableMiddleware{
					{
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
			err: "unnamed middleware",
		},
		{
			name: "invalid modification with duplicate middlewares",
			mod: MiddlewareModification{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "foobar",
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "foo",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
					{
						Name:                "foo",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
			err: "duplicate names in middleware modification",
		},
	} {
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
	mc, err := NewMiddlewareChain(ReferenceableMiddleware{
		Name:                "foobar",
		UnaryMiddleware:     nil,
		StreamingMiddleware: nil,
	})
	require.NoError(t, err)
	require.Len(t, mc.chain, 1)

	_, err = NewMiddlewareChain(ReferenceableMiddleware{
		UnaryMiddleware:     nil,
		StreamingMiddleware: nil,
	})
	require.ErrorContains(t, err, "unnamed middleware")
}

func TestChainValidate(t *testing.T) {
	mc, err := NewMiddlewareChain(ReferenceableMiddleware{
		Name:                "public",
		UnaryMiddleware:     nil,
		StreamingMiddleware: nil,
	}, ReferenceableMiddleware{
		Name:                "internal",
		Internal:            true,
		UnaryMiddleware:     nil,
		StreamingMiddleware: nil,
	})
	require.NoError(t, err)

	for _, tt := range []struct {
		name string
		mod  MiddlewareModification
		err  string
	}{
		{
			name: "invalid modification on non-existing dependency",
			mod: MiddlewareModification{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "doesnotexist",
			},
			err: "dependency does not exist",
		},
		{
			name: "invalid modification that causes a duplicate",
			mod: MiddlewareModification{
				Operation:                OperationAppend,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "public",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
			err: "will cause a duplicate",
		},
		{
			name: "invalid replace of an internal middlewares",
			mod: MiddlewareModification{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "internal",
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "foobar",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
			err: "attempts to replace an internal middleware",
		},
		{
			name: "valid replace of a public middleware",
			mod: MiddlewareModification{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "foobar",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
		},
		{
			name: "valid replace of a public middleware with same name",
			mod: MiddlewareModification{
				Operation:                OperationReplace,
				DependencyMiddlewareName: "public",
				Middlewares: []ReferenceableMiddleware{
					{
						Name:                "public",
						UnaryMiddleware:     nil,
						StreamingMiddleware: nil,
					},
				},
			},
		},
	} {
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
	var replaceStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("1")}.streamIntercept
	mod := MiddlewareModification{
		Operation: OperationReplaceAllUnsafe,
		Middlewares: []ReferenceableMiddleware{
			{
				Name:                "foobar",
				UnaryMiddleware:     replaceUnary,
				StreamingMiddleware: replaceStream,
			},
		},
	}

	mc, err := NewMiddlewareChain()
	require.NoError(t, err)

	err = mc.modify(mod)
	require.NoError(t, err)

	outUnary, outStream := mc.ToGRPCInterceptors()
	require.NoError(t, err)

	expectedReplaceUnary, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplaceUnary, _ := outUnary[0](context.Background(), nil, nil, nil)
	expectedReplaceStreaming := replaceStream(nil, nil, nil, nil)
	receivedReplaceStreaming := outStream[0](nil, nil, nil, nil)
	require.Equal(t, expectedReplaceUnary, receivedReplaceUnary)
	require.Equal(t, expectedReplaceStreaming, receivedReplaceStreaming)
}

func TestPrependMiddleware(t *testing.T) {
	// Test prepending and appending to Default middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	var replaceStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("1")}.streamIntercept
	var prependUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 2}.unaryIntercept
	var prependStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("2")}.streamIntercept
	defaultMiddleware, err := NewMiddlewareChain(
		ReferenceableMiddleware{
			Name:                "foobar",
			UnaryMiddleware:     replaceUnary,
			StreamingMiddleware: replaceStream,
		})
	require.NoError(t, err)

	mod := MiddlewareModification{
		Operation:                OperationPrepend,
		DependencyMiddlewareName: "foobar",
		Middlewares: []ReferenceableMiddleware{
			{
				Name:                "prepended",
				UnaryMiddleware:     prependUnary,
				StreamingMiddleware: prependStream,
			},
		},
	}
	err = defaultMiddleware.modify(mod)
	require.NoError(t, err)

	// testing function equality is not possible, so we test the results of executing the functions
	outUnary, outStream := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 2)
	require.Len(t, outStream, 2)

	expectedPrepend, _ := prependUnary(context.Background(), nil, nil, nil)
	receivedPrepend, _ := outUnary[0](context.Background(), nil, nil, nil)
	require.Equal(t, expectedPrepend, receivedPrepend)

	expectedReplace, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplace, _ := outUnary[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.NotEqual(t, receivedPrepend, receivedReplace)

	expectedPrepend = prependStream(nil, nil, nil, nil)
	receivedPrepend = outStream[0](nil, nil, nil, nil)
	require.Equal(t, expectedPrepend, receivedPrepend)

	expectedReplace = replaceStream(nil, nil, nil, nil)
	receivedReplace = outStream[1](nil, nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.NotEqual(t, receivedPrepend, receivedReplace)
}

func TestAppendMiddleware(t *testing.T) {
	// Test prepending and appending to Default middleware
	var replaceUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 1}.unaryIntercept
	var replaceStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("1")}.streamIntercept
	var appendUnary grpc.UnaryServerInterceptor = mockUnaryInterceptor{val: 3}.unaryIntercept
	var appendStream grpc.StreamServerInterceptor = mockStreamInterceptor{val: errors.New("3")}.streamIntercept
	defaultMiddleware := &MiddlewareChain{
		chain: []ReferenceableMiddleware{
			{
				Name:                "foobar",
				UnaryMiddleware:     replaceUnary,
				StreamingMiddleware: replaceStream,
			},
		},
	}
	mod := MiddlewareModification{
		Operation:                OperationAppend,
		DependencyMiddlewareName: "foobar",
		Middlewares: []ReferenceableMiddleware{
			{
				Name:                "appended",
				UnaryMiddleware:     appendUnary,
				StreamingMiddleware: appendStream,
			},
		},
	}
	err := defaultMiddleware.modify(mod)
	require.NoError(t, err)

	// testing function equality is not possible, so we test the results of executing the functions
	outUnary, outStream := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 2)
	require.Len(t, outStream, 2)

	expectedReplace, _ := replaceUnary(context.Background(), nil, nil, nil)
	receivedReplace, _ := outUnary[0](context.Background(), nil, nil, nil)
	expectedAppend, _ := appendUnary(context.Background(), nil, nil, nil)
	receivedAppend, _ := outUnary[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.Equal(t, expectedAppend, receivedAppend)

	expectedReplace = replaceStream(nil, nil, nil, nil)
	receivedReplace = outStream[0](nil, nil, nil, nil)
	expectedAppend = appendStream(nil, nil, nil, nil)
	receivedAppend = outStream[1](context.Background(), nil, nil, nil)
	require.Equal(t, expectedReplace, receivedReplace)
	require.Equal(t, expectedAppend, receivedAppend)
}

func TestDeleteMiddleware(t *testing.T) {
	defaultMiddleware := &MiddlewareChain{
		chain: []ReferenceableMiddleware{
			{
				Name:                "foobar",
				UnaryMiddleware:     mockUnaryInterceptor{}.unaryIntercept,
				StreamingMiddleware: mockStreamInterceptor{}.streamIntercept,
			},
		},
	}
	mod := MiddlewareModification{
		Operation:                OperationReplace,
		DependencyMiddlewareName: "foobar",
	}
	err := defaultMiddleware.modify(mod)
	require.NoError(t, err)

	outUnary, outStream := defaultMiddleware.ToGRPCInterceptors()
	require.NoError(t, err)
	require.Len(t, outUnary, 0)
	require.Len(t, outStream, 0)
}

func TestCannotReplaceInternalMiddleware(t *testing.T) {
	defaultMiddleware := &MiddlewareChain{
		chain: []ReferenceableMiddleware{
			{
				Name:                "foobar",
				Internal:            true,
				UnaryMiddleware:     mockUnaryInterceptor{}.unaryIntercept,
				StreamingMiddleware: mockStreamInterceptor{}.streamIntercept,
			},
		},
	}
	mod := MiddlewareModification{
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

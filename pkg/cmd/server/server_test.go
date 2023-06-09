package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/util"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		WithNamespaceCacheConfig(CacheConfig{Enabled: true}),
		WithDispatchCacheConfig(CacheConfig{Enabled: true}),
		WithClusterDispatchCacheConfig(CacheConfig{Enabled: true}),
		WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: true, HTTPAddress: ":"}),
		WithDashboardAPI(util.HTTPServerConfig{HTTPEnabled: true, HTTPAddress: ":"}),
		WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: true, HTTPAddress: ":"}),
	)
	rs, err := c.Complete(ctx)
	require.NoError(t, err)

	ch := make(chan struct{}, 1)
	st := make(chan struct{}, 1)
	go func() {
		st <- struct{}{}
		_ = rs.Run(ctx)
		ch <- struct{}{}
	}()
	<-st
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-ch
}

func TestServerGracefulTerminationOnError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithCancel(context.Background())
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(&Config{
		GRPCServer: util.GRPCServerConfig{
			Network: util.BufferedNetwork,
		},
	}, WithPresharedSecureKey("psk"), WithDatastore(ds))
	cancel()
	_, err = c.Complete(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReplaceUnaryMiddleware(t *testing.T) {
	c := Config{UnaryMiddlewareModification: []MiddlewareModification[grpc.UnaryServerInterceptor]{
		{
			Operation: OperationReplaceAllUnsafe,
			Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
				{
					Name:       "foobar",
					Middleware: mockUnaryInterceptor{val: 1}.unaryIntercept,
				},
			},
		},
	}}
	unary, err := c.buildUnaryMiddleware(nil)
	require.NoError(t, err)
	require.Len(t, unary, 1)

	val, _ := unary[0](context.Background(), nil, nil, nil)
	require.Equal(t, 1, val)
}

func TestReplaceStreamingMiddleware(t *testing.T) {
	c := Config{StreamingMiddlewareModification: []MiddlewareModification[grpc.StreamServerInterceptor]{
		{
			Operation: OperationReplaceAllUnsafe,
			Middlewares: []ReferenceableMiddleware[grpc.StreamServerInterceptor]{
				{
					Name:       "foobar",
					Middleware: mockStreamInterceptor{val: errors.New("hi")}.streamIntercept,
				},
			},
		},
	}}
	streaming, err := c.buildStreamingMiddleware(nil)
	require.NoError(t, err)
	require.Len(t, streaming, 1)

	err = streaming[0](context.Background(), nil, nil, nil)
	require.ErrorContains(t, err, "hi")
}

func TestModifyUnaryMiddleware(t *testing.T) {
	c := Config{UnaryMiddlewareModification: []MiddlewareModification[grpc.UnaryServerInterceptor]{
		{
			Operation:                OperationPrepend,
			DependencyMiddlewareName: DefaultMiddlewareLog,
			Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
				{
					Name:       "foobar",
					Middleware: mockUnaryInterceptor{val: 1}.unaryIntercept,
				},
			},
		},
	}}

	defaultMw, err := DefaultUnaryMiddleware(logging.Logger, nil, false, nil, nil)
	require.NoError(t, err)

	unary, err := c.buildUnaryMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, unary, len(defaultMw.chain)+1)

	val, _ := unary[1](context.Background(), nil, nil, nil)
	require.Equal(t, 1, val)
}

func TestModifyStreamingMiddleware(t *testing.T) {
	c := Config{StreamingMiddlewareModification: []MiddlewareModification[grpc.StreamServerInterceptor]{
		{
			Operation:                OperationPrepend,
			DependencyMiddlewareName: DefaultMiddlewareLog,
			Middlewares: []ReferenceableMiddleware[grpc.StreamServerInterceptor]{
				{
					Name:       "foobar",
					Middleware: mockStreamInterceptor{val: errors.New("hi")}.streamIntercept,
				},
			},
		},
	}}

	defaultMw, err := DefaultStreamingMiddleware(logging.Logger, nil, false, nil, nil)
	require.NoError(t, err)

	streaming, err := c.buildStreamingMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, streaming, len(defaultMw.chain)+1)

	err = streaming[1](context.Background(), nil, nil, nil)
	require.ErrorContains(t, err, "hi")
}

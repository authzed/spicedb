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
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(
		&Config{},
		WithPresharedKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		WithNamespaceCacheConfig(CacheConfig{Enabled: true}),
		WithDispatchCacheConfig(CacheConfig{Enabled: true}),
		WithClusterDispatchCacheConfig(CacheConfig{Enabled: true}),
		WithHTTPGateway(util.HTTPServerConfig{Enabled: true, Address: ":"}),
		WithDashboardAPI(util.HTTPServerConfig{Enabled: true, Address: ":"}),
		WithMetricsAPI(util.HTTPServerConfig{Enabled: true, Address: ":"}),
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

	c := ConfigWithOptions(&Config{}, WithPresharedKey("psk"), WithDatastore(ds))
	cancel()
	_, err = c.Complete(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReplaceMiddleware(t *testing.T) {
	c := Config{MiddlewareModification: []MiddlewareModification{
		{
			Operation: OperationReplaceAllUnsafe,
			Middlewares: []ReferenceableMiddleware{
				{
					Name:                "foobar",
					UnaryMiddleware:     mockUnaryInterceptor{val: 1}.unaryIntercept,
					StreamingMiddleware: mockStreamInterceptor{val: errors.New("hi")}.streamIntercept,
				},
			},
		},
	}}
	unary, streaming, err := c.buildMiddleware(nil)
	require.NoError(t, err)
	require.Len(t, unary, 1)
	require.Len(t, streaming, 1)

	val, _ := unary[0](context.Background(), nil, nil, nil)
	require.Equal(t, 1, val)

	err = streaming[0](context.Background(), nil, nil, nil)
	require.ErrorContains(t, err, "hi")
}

func TestModifyMiddleware(t *testing.T) {
	c := Config{MiddlewareModification: []MiddlewareModification{
		{
			Operation:                OperationPrepend,
			DependencyMiddlewareName: DefaultMiddlewareLog,
			Middlewares: []ReferenceableMiddleware{
				{
					Name:                "foobar",
					UnaryMiddleware:     mockUnaryInterceptor{val: 1}.unaryIntercept,
					StreamingMiddleware: mockStreamInterceptor{val: errors.New("hi")}.streamIntercept,
				},
			},
		},
	}}

	defaultMw, err := DefaultMiddleware(logging.Logger, nil, false, nil, nil)
	require.NoError(t, err)

	unary, streaming, err := c.buildMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, unary, len(defaultMw.chain)+1)
	require.Len(t, streaming, len(defaultMw.chain)+1)

	val, _ := unary[1](context.Background(), nil, nil, nil)
	require.Equal(t, 1, val)

	err = streaming[1](context.Background(), nil, nil, nil)
	require.ErrorContains(t, err, "hi")
}

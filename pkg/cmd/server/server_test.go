package server

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/testutil"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 1*time.Second, 10*time.Second)
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

func TestOTelReporting(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithRequestHedgingEnabled(false),
	)
	if err != nil {
		log.Fatalf("unable to start memdb datastore: %s", err)
	}

	configOpts := []ConfigOption{
		WithGRPCServer(util.GRPCServerConfig{
			Network: util.BufferedNetwork,
			Enabled: true,
		}),
		WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		WithDispatchCacheConfig(CacheConfig{Enabled: false, Metrics: false}),
		WithNamespaceCacheConfig(CacheConfig{Enabled: false, Metrics: false}),
		WithClusterDispatchCacheConfig(CacheConfig{Enabled: false, Metrics: false}),
		WithDatastore(ds),
	}

	srv, err := NewConfigWithOptionsAndDefaults(configOpts...).Complete(ctx)
	require.NoError(t, err)

	conn, err := srv.GRPCDialContext(ctx)
	require.NoError(t, err)
	defer conn.Close()

	schemaSrv := v1.NewSchemaServiceClient(conn)

	go func() {
		require.NoError(t, srv.Run(ctx))
	}()

	spanrecorder, restoreOtel := setupSpanRecorder()
	defer restoreOtel()

	// test unary OTel middleware
	_, err = schemaSrv.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
	requireSpanExists(t, spanrecorder, "authzed.api.v1.SchemaService/WriteSchema")

	// test streaming OTel middleware
	permSrv := v1.NewPermissionsServiceClient(conn)
	rrCli, err := permSrv.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{})
	require.NoError(t, err)

	_, err = rrCli.Recv()
	require.Error(t, err)

	requireSpanExists(t, spanrecorder, "authzed.api.v1.PermissionsService/ReadRelationships")

	lrCli, err := permSrv.LookupResources(ctx, &v1.LookupResourcesRequest{})
	require.NoError(t, err)

	_, err = lrCli.Recv()
	require.Error(t, err)

	requireSpanExists(t, spanrecorder, "authzed.api.v1.PermissionsService/LookupResources")
}

func requireSpanExists(t *testing.T, spanrecorder *tracetest.SpanRecorder, spanName string) {
	t.Helper()

	ended := spanrecorder.Ended()
	var present bool
	for _, span := range ended {
		if span.Name() == spanName {
			present = true
		}
	}

	require.True(t, present, "missing trace for Streaming gRPC call")
}

func setupSpanRecorder() (*tracetest.SpanRecorder, func()) {
	defaultProvider := otel.GetTracerProvider()

	provider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
	)
	spanrecorder := tracetest.NewSpanRecorder()
	provider.RegisterSpanProcessor(spanrecorder)
	otel.SetTracerProvider(provider)

	return spanrecorder, func() {
		otel.SetTracerProvider(defaultProvider)
	}
}

func TestServerGracefulTerminationOnError(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithCancel(context.Background())
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(&Config{
		GRPCServer: util.GRPCServerConfig{
			Network: util.BufferedNetwork,
		},
	}, WithPresharedSecureKey("psk"), WithDatastore(ds))
	cancel()
	_, err = c.Complete(ctx)
	require.NoError(t, err)
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

	opt := MiddlewareOption{logging.Logger, nil, false, nil, false, false, false, "testing", nil, nil}
	opt = opt.WithDatastore(nil)

	defaultMw, err := DefaultUnaryMiddleware(opt)
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

	opt := MiddlewareOption{logging.Logger, nil, false, nil, false, false, false, "testing", nil, nil}
	opt = opt.WithDatastore(nil)

	defaultMw, err := DefaultStreamingMiddleware(opt)
	require.NoError(t, err)

	streaming, err := c.buildStreamingMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, streaming, len(defaultMw.chain)+1)

	err = streaming[1](context.Background(), nil, nil, nil)
	require.ErrorContains(t, err, "hi")
}

func TestSupportOldAndNewReadReplicaConnectionPoolFlags(t *testing.T) {
	tests := []struct {
		name     string
		opts     Config
		expected datastore.ConnPoolConfig
	}{
		{
			name: "no flags set",
			opts: Config{
				DatastoreConfig: datastore.Config{
					ReadReplicaConnPool:    *datastore.DefaultReadConnPool(),
					OldReadReplicaConnPool: *datastore.DefaultReadConnPool(),
				},
			},
			expected: *datastore.DefaultReadConnPool(),
		},
		{
			name: "new flags set",
			opts: Config{
				DatastoreConfig: datastore.Config{
					ReadReplicaConnPool: datastore.ConnPoolConfig{
						MaxLifetime:         1 * time.Minute,
						MaxIdleTime:         1 * time.Minute,
						MaxOpenConns:        1,
						MinOpenConns:        1,
						HealthCheckInterval: 1 * time.Second,
					},
					OldReadReplicaConnPool: *datastore.DefaultReadConnPool(),
				},
			},
			expected: datastore.ConnPoolConfig{
				MaxLifetime:         1 * time.Minute,
				MaxIdleTime:         1 * time.Minute,
				MaxOpenConns:        1,
				MinOpenConns:        1,
				HealthCheckInterval: 1 * time.Second,
			},
		},
		{
			name: "old flags set",
			opts: Config{
				DatastoreConfig: datastore.Config{
					ReadReplicaConnPool: *datastore.DefaultReadConnPool(),
					OldReadReplicaConnPool: datastore.ConnPoolConfig{
						MaxLifetime:         2 * time.Minute,
						MaxIdleTime:         2 * time.Minute,
						MaxOpenConns:        2,
						MinOpenConns:        2,
						HealthCheckInterval: 2 * time.Second,
					},
				},
			},
			expected: datastore.ConnPoolConfig{
				MaxLifetime:         2 * time.Minute,
				MaxIdleTime:         2 * time.Minute,
				MaxOpenConns:        2,
				MinOpenConns:        2,
				HealthCheckInterval: 2 * time.Second,
			},
		},
		{
			name: "prefers new flags if both are set",
			opts: Config{
				DatastoreConfig: datastore.Config{
					ReadReplicaConnPool: datastore.ConnPoolConfig{
						MaxLifetime:         1 * time.Minute,
						MaxIdleTime:         1 * time.Minute,
						MaxOpenConns:        1,
						MinOpenConns:        2,
						HealthCheckInterval: 2 * time.Second,
					},
					OldReadReplicaConnPool: datastore.ConnPoolConfig{
						MaxLifetime:         2 * time.Minute,
						MaxIdleTime:         2 * time.Minute,
						MaxOpenConns:        2,
						MinOpenConns:        2,
						HealthCheckInterval: 2 * time.Second,
					},
				},
			},
			expected: datastore.ConnPoolConfig{
				MaxLifetime:         1 * time.Minute,
				MaxIdleTime:         1 * time.Minute,
				MaxOpenConns:        1,
				MinOpenConns:        2,
				HealthCheckInterval: 2 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.supportOldAndNewReadReplicaConnectionPoolFlags()
			require.Equal(t, tt.expected, tt.opts.DatastoreConfig.ReadReplicaConnPool)
		})
	}
}

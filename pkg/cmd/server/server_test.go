package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	dispatchmocks "github.com/authzed/spicedb/internal/dispatch/mocks"
	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/middleware/memoryprotection"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/util"
	dsmocks "github.com/authzed/spicedb/pkg/datastore/mocks"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestServerGracefulTermination(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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
		WithMemoryProtectionEnabled(false),
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

func TestServerDefaultOptions(t *testing.T) {
	cfg := NewConfigWithOptionsAndDefaults()

	require.True(t, cfg.EnableRelationshipExpiration)
}

func TestOTelReporting(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)
	spanrecorder, restoreOtel := setupSpanRecorder()
	defer restoreOtel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithRequestHedgingEnabled(false),
	)
	require.NoError(t, err, "unable to start memdb datastore")

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
		WithMemoryProtectionEnabled(false),
	}

	srv, err := NewConfigWithOptionsAndDefaults(configOpts...).Complete(ctx)
	require.NoError(t, err)

	conn, err := srv.GRPCDialContext(ctx)
	require.NoError(t, err)
	defer conn.Close()

	schemaSrv := v1.NewSchemaServiceClient(conn)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- srv.Run(ctx)
	}()

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
	require.NoError(t, <-runErrCh)
}

func TestDisableHealthCheckTracing(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)
	spanrecorder, restoreOtel := setupSpanRecorder()
	defer restoreOtel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithRequestHedgingEnabled(false),
	)
	require.NoError(t, err, "unable to start memdb datastore")

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

	go func() {
		require.NoError(t, srv.Run(ctx))
	}()

	// Poll for gRPC server readiness instead of sleeping
	healthClient := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		_, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "gRPC server did not become ready")

	// Call a regular API and verify it IS traced
	schemaSrv := v1.NewSchemaServiceClient(conn)
	_, err = schemaSrv.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)

	// Verify the schema call was traced
	requireSpanExists(t, spanrecorder, "authzed.api.v1.SchemaService/WriteSchema")

	// Verify the gRPC health check was NOT traced
	requireSpanDoesNotExist(t, spanrecorder, "/grpc.health.v1.Health/Check")

	// Note: HTTP gateway health check (/healthz) filtering is verified by the gateway
	// implementation itself. This test focuses on gRPC health check filtering since
	// BufferedNetwork mode disables the HTTP gateway.
}

func requireSpanExists(t *testing.T, spanrecorder *tracetest.SpanRecorder, spanName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		for _, span := range spanrecorder.Ended() {
			if span.Name() == spanName {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, fmt.Sprintf("missing span with name %q", spanName))
}

func requireSpanDoesNotExist(t *testing.T, spanrecorder *tracetest.SpanRecorder, spanName string) {
	t.Helper()

	// Actively verify that the span never appears over a short window
	require.Never(t, func() bool {
		for _, span := range spanrecorder.Ended() {
			if span.Name() == spanName {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond, "span %q should not exist", spanName)
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

type countingInterceptor struct {
	val int
}

func (m *countingInterceptor) unaryIntercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
	m.val++
	return h(ctx, req)
}

// TestRetryPolicy tests that the retry policy specified in the grpc.WithDefaultServiceConfig is respected.
// It does this by installing a unary interceptor that counts the number of calls, and then returning a
// FAILED_PRECONDITION error from the WriteRelationships call.
// This test is in place to make sure we don't regress this again by messing with grpc in our middlewares.
func TestRetryPolicy(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	ds, err := datastore.NewDatastore(ctx,
		datastore.DefaultDatastoreConfig().ToOption(),
		datastore.WithRequestHedgingEnabled(false),
	)
	if err != nil {
		t.Fatalf("unable to start memdb datastore: %s", err)
	}

	var interceptor countingInterceptor
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
		WithMemoryProtectionEnabled(false),
		SetUnaryMiddlewareModification([]MiddlewareModification[grpc.UnaryServerInterceptor]{
			{
				Operation:                OperationAppend,
				DependencyMiddlewareName: DefaultMiddlewareRequestID,
				Middlewares: []ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					NewUnaryMiddleware().
						WithName("foobar").
						WithInterceptor(interceptor.unaryIntercept).
						EnsureAlreadyExecuted(DefaultMiddlewareRequestID). // make sure requestID is executed because before we fixed it, it broke retry policies
						Done(),
				},
			},
		}),
	}

	srv, err := NewConfigWithOptionsAndDefaults(configOpts...).Complete(ctx)
	require.NoError(t, err)

	conn, err := srv.GRPCDialContext(ctx,
		grpc.WithDefaultServiceConfig(`{
                  "methodConfig": [
                    {
                      "name": [
                        {
                          "service": "authzed.api.v1.PermissionsService",
                          "method": "WriteRelationships"
                        }
                      ],
                      "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "0.01s",
                        "maxBackoff": "0.1s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": [
                          "FAILED_PRECONDITION"
                        ]
                      }
                    }
                  ]
                }`))
	require.NoError(t, err)
	defer func() {
		_ = conn.Close()
	}()

	schemaSrv := v1.NewSchemaServiceClient(conn)

	go func() {
		require.NoError(t, srv.Run(ctx))
	}()

	_, err = schemaSrv.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)

	interceptor.val = 0
	permSrv := v1.NewPermissionsServiceClient(conn)
	var trailer metadata.MD
	ctxWithRequestID := requestmeta.WithRequestID(ctx, "foobar")
	ctxWithServerVersion := metadata.AppendToOutgoingContext(ctxWithRequestID, string(requestmeta.RequestServerVersion), "t")
	_, err = permSrv.WriteRelationships(ctxWithServerVersion, &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.MustUpdateToV1RelationshipUpdate(tuple.Touch(tuple.MustParse("resource:resource#reader@user:user#..."))),
		},
	}, grpc.Trailer(&trailer))
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// validate that requestID was used, as it used to break retry policies before
	require.Equal(t, 5, interceptor.val)
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(t, requestIDs)
	require.Contains(t, requestIDs, "foobar")
}

func TestServerGracefulTerminationOnError(t *testing.T) {
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	ctx, cancel := context.WithCancel(t.Context())
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	c := ConfigWithOptions(&Config{
		GRPCServer: util.GRPCServerConfig{
			Network: util.BufferedNetwork,
		},
	}, WithPresharedSecureKey("psk"), WithDatastore(ds), WithMemoryProtectionEnabled(false))
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

	val, _ := unary[0](t.Context(), nil, nil, nil)
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

	err = streaming[0](t.Context(), nil, nil, nil)
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

	opt := MiddlewareOption{logging.Logger, nil, false, nil, false, false, false, "testing", consistency.TreatMismatchingTokensAsFullConsistency, memoryprotection.NewNoopMemoryUsageProvider(), nil, nil}
	opt = opt.WithDatastore(nil)

	defaultMw, err := DefaultUnaryMiddleware(opt)
	require.NoError(t, err)

	unary, err := c.buildUnaryMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, unary, len(defaultMw.chain)+1)

	val, _ := unary[1](t.Context(), nil, nil, nil)
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

	opt := MiddlewareOption{logging.Logger, nil, false, nil, false, false, false, "testing", consistency.TreatMismatchingTokensAsFullConsistency, memoryprotection.NewNoopMemoryUsageProvider(), nil, nil}
	opt = opt.WithDatastore(nil)

	defaultMw, err := DefaultStreamingMiddleware(opt)
	require.NoError(t, err)

	streaming, err := c.buildStreamingMiddleware(defaultMw)
	require.NoError(t, err)
	require.Len(t, streaming, len(defaultMw.chain)+1)

	err = streaming[1](t.Context(), nil, nil, nil)
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

func TestBuildDispatchServer(t *testing.T) {
	testcases := map[string]struct {
		config                              *Config
		expectedDispatchUnnaryMiddleware    int
		expectedDispatchStreamingMiddleware int
	}{
		`auth:preshared key`: {
			config: &Config{
				PresharedSecureKey: []string{"securekey"},
			},
			expectedDispatchUnnaryMiddleware:    8,
			expectedDispatchStreamingMiddleware: 6,
		},
		`auth:custom`: {
			config: &Config{
				GRPCAuthFunc: func(ctx context.Context) (context.Context, error) {
					return ctx, nil
				},
			},
			expectedDispatchUnnaryMiddleware:    8,
			expectedDispatchStreamingMiddleware: 6,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockDatastore := dsmocks.NewMockDatastore(ctrl)
			mockDispatcher := dispatchmocks.NewMockDispatcher(ctrl)

			closeables := closeableStack{}
			t.Cleanup(func() {
				_ = closeables.Close()
			})

			sampler := memoryprotection.NewNoopMemoryUsageProvider()

			srv, err := tc.config.buildDispatchServer(sampler, mockDatastore, mockDispatcher, &closeables, nil)
			require.NoError(t, err)
			require.NotNil(t, srv)
			require.Len(t, closeables.closers, 1)
			require.Len(t, tc.config.DispatchUnaryMiddleware, tc.expectedDispatchUnnaryMiddleware)
			require.Len(t, tc.config.DispatchStreamingMiddleware, tc.expectedDispatchStreamingMiddleware)
		})
	}
}

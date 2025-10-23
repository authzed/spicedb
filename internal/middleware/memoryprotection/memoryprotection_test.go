package memoryprotection

import (
	"context"
	"errors"
	"io"
	"runtime/debug"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name           string
		config         Config
		setupMemLimit  func() int64
		expectDisabled bool
	}{
		{
			name:   "default config with memory limit",
			config: DefaultConfig(),
			setupMemLimit: func() int64 {
				debug.SetMemoryLimit(1024 * 1024 * 100) // 100MB
				return 1024 * 1024 * 100
			},
			expectDisabled: false,
		},
		{
			name:   "no memory limit set",
			config: DefaultConfig(),
			setupMemLimit: func() int64 {
				debug.SetMemoryLimit(-1)        // Remove limit
				return debug.SetMemoryLimit(-1) // Get actual value
			},
			expectDisabled: true,
		},
		{
			name: "disabled via config",
			config: Config{
				ThresholdPercent:      0, // Disabled
				SampleIntervalSeconds: 1,
			},
			setupMemLimit: func() int64 {
				debug.SetMemoryLimit(1024 * 1024 * 100) // 100MB
				return 1024 * 1024 * 100
			},
			expectDisabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalLimit := debug.SetMemoryLimit(-1)
			defer debug.SetMemoryLimit(originalLimit)

			expectedLimit := tt.setupMemLimit()

			am := New(context.Background(), tt.config)
			require.NotNil(t, am)

			if tt.expectDisabled {
				assert.True(t, am.memoryLimit < 0 || am.memoryLimit == 9223372036854775807) // -1 or max int64
			} else {
				assert.Equal(t, expectedLimit, am.memoryLimit)
			}

			assert.Equal(t, tt.config, am.config)
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	assert.Equal(t, 90, config.ThresholdPercent)
	assert.Equal(t, 1, config.SampleIntervalSeconds)
}

func TestDefaultDispatchConfig(t *testing.T) {
	config := DefaultDispatchConfig()
	assert.Equal(t, 95, config.ThresholdPercent)
	assert.Equal(t, 1, config.SampleIntervalSeconds)
}

func TestAdmissionMiddleware_RecordRejection(t *testing.T) {
	// Create a custom registry for testing
	registry := prometheus.NewRegistry()

	// Register our metrics with the test registry
	testRejectedCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_overload_rejected_requests_total",
		Help:      "Total requests rejected due to memory pressure",
	}, []string{"endpoint"})
	registry.MustRegister(testRejectedCounter)

	am := &AdmissionMiddleware{
		config: DefaultConfig(),
	}

	// Replace the global counter with our test counter for this test
	originalCounter := RejectedRequestsCounter
	RejectedRequestsCounter = testRejectedCounter
	defer func() {
		RejectedRequestsCounter = originalCounter
	}()

	// Test API endpoint
	am.recordRejection("/authzed.api.v1.PermissionsService/CheckPermission")

	// Test dispatch endpoint
	am.recordRejection("/dispatch.v1.DispatchService/DispatchCheck")

	// Verify metrics
	apiMetric := testutil.ToFloat64(testRejectedCounter.WithLabelValues("api"))
	dispatchMetric := testutil.ToFloat64(testRejectedCounter.WithLabelValues("dispatch"))

	assert.Equal(t, float64(1), apiMetric, "API rejection should be recorded")
	assert.Equal(t, float64(1), dispatchMetric, "Dispatch rejection should be recorded")
}

func TestAdmissionMiddleware_SampleMemory(t *testing.T) {
	// Create a custom registry for testing
	registry := prometheus.NewRegistry()

	testMemoryGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_usage_percent",
		Help:      "Current memory usage as percentage of GOMEMLIMIT",
	})
	registry.MustRegister(testMemoryGauge)

	am := &AdmissionMiddleware{
		config:      DefaultConfig(),
		memoryLimit: 1024 * 1024,
		metricsSamples: []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		},
	}

	// Replace the global gauge with our test gauge
	originalGauge := MemoryUsageGauge
	MemoryUsageGauge = testMemoryGauge
	defer func() {
		MemoryUsageGauge = originalGauge
	}()

	err := am.sampleMemory()
	assert.NoError(t, err)

	usage := am.getCurrentMemoryUsage()
	assert.Greater(t, usage, int64(0))

	// Verify the gauge was updated
	gaugeValue := testutil.ToFloat64(testMemoryGauge)
	assert.Greater(t, gaugeValue, float64(0), "Memory usage gauge should be updated")
}

func TestAdmissionMiddleware_BackgroundSampling(t *testing.T) {
	config := DefaultConfig()
	config.SampleIntervalSeconds = 1 // Fast sampling for test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	am := &AdmissionMiddleware{
		config:      config,
		memoryLimit: 1024 * 1024,
		metricsSamples: []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		},
		ctx: ctx,
	}

	// Initialize with current memory usage
	err := am.sampleMemory()
	require.NoError(t, err)

	// Test that getCurrentMemoryUsage returns cached value
	usage1 := am.getCurrentMemoryUsage()
	usage2 := am.getCurrentMemoryUsage()

	// Both calls should return the same cached value
	assert.Equal(t, usage1, usage2)
	assert.Greater(t, usage1, int64(0))
}

// Test service for gRPC interceptor testing
type testRequest struct {
	Message string
}

func (r *testRequest) Reset()         {}
func (r *testRequest) String() string { return "testRequest{" + r.Message + "}" }
func (r *testRequest) ProtoMessage()  {}

type testResponse struct {
	Reply string
}

func (r *testResponse) Reset()         {}
func (r *testResponse) String() string { return "testResponse{" + r.Reply + "}" }
func (r *testResponse) ProtoMessage()  {}

type testServiceServer interface {
	UnaryCall(context.Context, *testRequest) (*testResponse, error)
	StreamCall(testServiceStreamServer) error
}

type testServiceStreamServer interface {
	Send(*testResponse) error
	Recv() (*testRequest, error)
	grpc.ServerStream
}

type testServiceImpl struct{}

func (s *testServiceImpl) UnaryCall(ctx context.Context, req *testRequest) (*testResponse, error) {
	return &testResponse{Reply: "response: " + req.Message}, nil
}

func (s *testServiceImpl) StreamCall(stream testServiceStreamServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		if err := stream.Send(&testResponse{Reply: "stream response: " + req.Message}); err != nil {
			return err
		}
	}
}

func testStreamHandler(srv any, stream grpc.ServerStream) error {
	return srv.(testServiceServer).StreamCall(&testStreamWrapper{stream})
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "test.TestService",
	HandlerType: (*testServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "UnaryCall",
			Handler:    testUnaryHandler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamCall",
			Handler:       testStreamHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "test.proto",
}

func testUnaryHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(testRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(testServiceServer).UnaryCall(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/test.TestService/UnaryCall",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(testServiceServer).UnaryCall(ctx, req.(*testRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type testStreamWrapper struct {
	grpc.ServerStream
}

func (w *testStreamWrapper) Send(resp *testResponse) error {
	return w.SendMsg(resp)
}

func (w *testStreamWrapper) Recv() (*testRequest, error) {
	req := new(testRequest)
	if err := w.RecvMsg(req); err != nil {
		return nil, err
	}
	return req, nil
}

func TestUnaryServerInterceptor_RequestBlocking(t *testing.T) {
	tests := []struct {
		name             string
		memoryUsage      int64
		memoryLimit      int64
		thresholdPercent int
		fullMethod       string
		expectBlocked    bool
		expectedEndpoint string
	}{
		{
			name:             "below threshold - allowed",
			memoryUsage:      50 * 1024 * 1024,  // 50MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			fullMethod:       "/authzed.api.v1.PermissionsService/CheckPermission",
			expectBlocked:    false,
			expectedEndpoint: "api",
		},
		{
			name:             "above threshold - blocked",
			memoryUsage:      95 * 1024 * 1024,  // 95MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			fullMethod:       "/authzed.api.v1.PermissionsService/CheckPermission",
			expectBlocked:    true,
			expectedEndpoint: "api",
		},
		{
			name:             "dispatch endpoint above threshold - blocked",
			memoryUsage:      96 * 1024 * 1024,  // 96MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 95,
			fullMethod:       "/dispatch.v1.DispatchService/DispatchCheck",
			expectBlocked:    true,
			expectedEndpoint: "dispatch",
		},
		{
			name:             "dispatch endpoint below threshold - allowed",
			memoryUsage:      94 * 1024 * 1024,  // 94MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 95,
			fullMethod:       "/dispatch.v1.DispatchService/DispatchCheck",
			expectBlocked:    false,
			expectedEndpoint: "dispatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create custom registry for metrics testing
			registry := prometheus.NewRegistry()
			testRejectedCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: "spicedb",
				Subsystem: "admission",
				Name:      "memory_overload_rejected_requests_total",
				Help:      "Total requests rejected due to memory pressure",
			}, []string{"endpoint"})
			registry.MustRegister(testRejectedCounter)

			// Create middleware with test configuration
			config := Config{
				ThresholdPercent:      tt.thresholdPercent,
				SampleIntervalSeconds: 10, // Slow sampling to avoid interference
			}

			am := &AdmissionMiddleware{
				config:      config,
				memoryLimit: tt.memoryLimit,
				metricsSamples: []metrics.Sample{
					{Name: "/memory/classes/heap/objects:bytes"},
				},
				ctx: context.Background(),
			}

			// Set the memory usage directly for testing
			am.lastMemoryUsage.Store(tt.memoryUsage)

			// Replace global counter with test counter
			originalCounter := RejectedRequestsCounter
			RejectedRequestsCounter = testRejectedCounter
			defer func() {
				RejectedRequestsCounter = originalCounter
			}()

			// Test the interceptor
			interceptor := am.UnaryServerInterceptor()

			info := &grpc.UnaryServerInfo{
				FullMethod: tt.fullMethod,
			}

			called := false
			handler := func(ctx context.Context, req any) (any, error) {
				called = true
				return &testResponse{Reply: "success"}, nil
			}

			resp, err := interceptor(context.Background(), &testRequest{Message: "test"}, info, handler)

			if tt.expectBlocked {
				// Request should be blocked
				assert.Error(t, err, "Request should be blocked due to memory pressure")
				assert.Nil(t, resp, "Response should be nil when blocked")
				assert.False(t, called, "Handler should not be called when request is blocked")

				// Check error details
				grpcErr, ok := status.FromError(err)
				require.True(t, ok, "Error should be a gRPC status error")
				assert.Equal(t, codes.ResourceExhausted, grpcErr.Code(), "Should return ResourceExhausted error")
				assert.Contains(t, grpcErr.Message(), "memory pressure", "Error message should mention memory pressure")

				// Check that rejection was recorded in metrics
				rejectionCount := testutil.ToFloat64(testRejectedCounter.WithLabelValues(tt.expectedEndpoint))
				assert.Equal(t, float64(1), rejectionCount, "Rejection should be recorded in metrics")
			} else {
				// Request should be allowed
				assert.NoError(t, err, "Request should be allowed")
				assert.NotNil(t, resp, "Response should not be nil when allowed")
				assert.True(t, called, "Handler should be called when request is allowed")

				// Check that no rejection was recorded in metrics
				rejectionCount := testutil.ToFloat64(testRejectedCounter.WithLabelValues(tt.expectedEndpoint))
				assert.Equal(t, float64(0), rejectionCount, "No rejection should be recorded in metrics")
			}
		})
	}
}

func TestStreamServerInterceptor_RequestBlocking(t *testing.T) {
	tests := []struct {
		name             string
		memoryUsage      int64
		memoryLimit      int64
		thresholdPercent int
		fullMethod       string
		expectBlocked    bool
		expectedEndpoint string
	}{
		{
			name:             "below threshold - allowed",
			memoryUsage:      50 * 1024 * 1024,  // 50MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			fullMethod:       "/authzed.api.v1.WatchService/Watch",
			expectBlocked:    false,
			expectedEndpoint: "api",
		},
		{
			name:             "above threshold - blocked",
			memoryUsage:      95 * 1024 * 1024,  // 95MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			fullMethod:       "/authzed.api.v1.WatchService/Watch",
			expectBlocked:    true,
			expectedEndpoint: "api",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create custom registry for metrics testing
			registry := prometheus.NewRegistry()
			testRejectedCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: "spicedb",
				Subsystem: "admission",
				Name:      "memory_overload_rejected_requests_total",
				Help:      "Total requests rejected due to memory pressure",
			}, []string{"endpoint"})
			registry.MustRegister(testRejectedCounter)

			// Create middleware with test configuration
			config := Config{
				ThresholdPercent:      tt.thresholdPercent,
				SampleIntervalSeconds: 10, // Slow sampling to avoid interference
			}

			am := &AdmissionMiddleware{
				config:      config,
				memoryLimit: tt.memoryLimit,
				metricsSamples: []metrics.Sample{
					{Name: "/memory/classes/heap/objects:bytes"},
				},
				ctx: context.Background(),
			}

			// Set the memory usage directly for testing
			am.lastMemoryUsage.Store(tt.memoryUsage)

			// Replace global counter with test counter
			originalCounter := RejectedRequestsCounter
			RejectedRequestsCounter = testRejectedCounter
			defer func() {
				RejectedRequestsCounter = originalCounter
			}()

			// Test the interceptor directly using checkAdmission
			err := am.checkAdmission(tt.fullMethod)

			if tt.expectBlocked {
				// Request should be blocked
				assert.Error(t, err, "Stream should be blocked due to memory pressure")

				// Check error details
				grpcErr, ok := status.FromError(err)
				require.True(t, ok, "Error should be a gRPC status error")
				assert.Equal(t, codes.ResourceExhausted, grpcErr.Code(), "Should return ResourceExhausted error")
				assert.Contains(t, grpcErr.Message(), "memory pressure", "Error message should mention memory pressure")

				// Record the rejection to test metrics
				am.recordRejection(tt.fullMethod)

				// Check that rejection was recorded in metrics
				rejectionCount := testutil.ToFloat64(testRejectedCounter.WithLabelValues(tt.expectedEndpoint))
				assert.Equal(t, float64(1), rejectionCount, "Rejection should be recorded in metrics")
			} else {
				// Request should be allowed
				assert.NoError(t, err, "Stream should be allowed")

				// Check that no rejection was recorded in metrics
				rejectionCount := testutil.ToFloat64(testRejectedCounter.WithLabelValues(tt.expectedEndpoint))
				assert.Equal(t, float64(0), rejectionCount, "No rejection should be recorded in metrics")
			}
		})
	}
}

func TestMemoryProtectionMiddleware_E2E(t *testing.T) {
	// Set up a test gRPC server with the memory protection middleware
	originalLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(originalLimit)

	// Set a test memory limit
	debug.SetMemoryLimit(100 * 1024 * 1024) // 100MB

	// Create custom registry for metrics testing
	registry := prometheus.NewRegistry()
	testRejectedCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_overload_rejected_requests_total",
		Help:      "Total requests rejected due to memory pressure",
	}, []string{"endpoint"})
	testMemoryGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "admission",
		Name:      "memory_usage_percent",
		Help:      "Current memory usage as percentage of GOMEMLIMIT",
	})
	registry.MustRegister(testRejectedCounter)
	registry.MustRegister(testMemoryGauge)

	// Replace global metrics with test metrics
	originalCounter := RejectedRequestsCounter
	originalGauge := MemoryUsageGauge
	RejectedRequestsCounter = testRejectedCounter
	MemoryUsageGauge = testMemoryGauge
	defer func() {
		RejectedRequestsCounter = originalCounter
		MemoryUsageGauge = originalGauge
	}()

	// Create middleware with reasonable threshold
	config := Config{
		ThresholdPercent:      85, // Reasonable threshold
		SampleIntervalSeconds: 10, // Slow sampling to avoid interference
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	am := New(ctx, config)
	require.NotNil(t, am)

	// Wait a moment for initial sampling
	time.Sleep(100 * time.Millisecond)

	// Verify that memory usage gauge was updated by background sampling
	gaugeValue := testutil.ToFloat64(testMemoryGauge)
	assert.Greater(t, gaugeValue, float64(0), "Memory usage gauge should be updated by background sampling")

	t.Logf("Current memory usage: %.2f%%", gaugeValue)

	// Test that the middleware functions correctly by testing admission directly
	// This avoids the complexity of setting up a full gRPC server
	err := am.checkAdmission("/authzed.api.v1.PermissionsService/CheckPermission")

	// With normal memory usage, this should be allowed
	if gaugeValue < float64(config.ThresholdPercent) {
		assert.NoError(t, err, "Request should be allowed under normal memory usage")
	} else {
		t.Logf("Memory usage %.2f%% exceeds threshold %d%%, request blocked as expected", gaugeValue, config.ThresholdPercent)
		assert.Error(t, err, "Request should be blocked due to high memory usage")
	}
}

func TestMemoryProtectionDisabled_AllowsAllRequests(t *testing.T) {
	// Test with memory protection disabled
	config := Config{
		ThresholdPercent:      0, // Disabled
		SampleIntervalSeconds: 1,
	}

	am := New(context.Background(), config)
	require.NotNil(t, am)
	require.True(t, am.memoryLimit < 0, "Memory protection should be disabled")

	// Test the interceptor directly - should always allow requests when disabled
	interceptor := am.UnaryServerInterceptor()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/authzed.api.v1.PermissionsService/CheckPermission",
	}

	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return &testResponse{Reply: "success"}, nil
	}

	resp, err := interceptor(context.Background(), &testRequest{Message: "test"}, info, handler)

	// Should always be allowed when disabled
	assert.NoError(t, err, "Request should be allowed when memory protection is disabled")
	assert.NotNil(t, resp, "Response should not be nil")
	assert.True(t, called, "Handler should be called when memory protection is disabled")

	// Test stream interceptor as well
	streamInterceptor := am.StreamServerInterceptor()
	streamInfo := &grpc.StreamServerInfo{
		FullMethod: "/authzed.api.v1.WatchService/Watch",
	}

	streamCalled := false
	streamHandler := func(srv any, stream grpc.ServerStream) error {
		streamCalled = true
		return nil
	}

	err = streamInterceptor(nil, nil, streamInfo, streamHandler)
	assert.NoError(t, err, "Stream should be allowed when memory protection is disabled")
	assert.True(t, streamCalled, "Stream handler should be called when memory protection is disabled")
}

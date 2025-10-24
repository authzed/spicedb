package memoryprotection

import (
	"context"
	"errors"
	"io"
	"math"
	"runtime/debug"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDefaultMemoryLimitProvider(t *testing.T) {
	dmlp := &DefaultMemoryLimitProvider{}

	get := dmlp.Get()
	require.Equal(t, int64(math.MaxInt64), get)

	dmlp.Set(int64(100))
	get = dmlp.Get()
	require.Equal(t, int64(100), get)
}

func TestNew(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name           string
		inputConfig    Config
		inputLimit     int64
		expectDisabled bool
	}{
		{
			name:           "some with memory limit",
			inputConfig:    DefaultConfig(),
			inputLimit:     1024 * 1024 * 100,
			expectDisabled: false,
		},
		{
			name:           "no memory limit set",
			inputConfig:    DefaultConfig(),
			inputLimit:     math.MaxInt64,
			expectDisabled: true,
		},
		{
			name:           "no memory limit set, part II",
			inputConfig:    DefaultConfig(),
			inputLimit:     -1,
			expectDisabled: true,
		},
		{
			name: "disabled via config",
			inputConfig: Config{
				ThresholdPercent:      0, // Disabled
				SampleIntervalSeconds: 1,
			},
			inputLimit:     1024 * 1024 * 100,
			expectDisabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp := HardCodedMemoryLimitProvider{Hardcodedlimit: tt.inputLimit}

			am := New(t.Context(), tt.inputConfig, &lp)
			require.NotNil(t, am)

			if tt.expectDisabled {
				require.True(t, am.memoryLimit < 0 || am.memoryLimit == math.MaxInt64)
			} else {
				require.Equal(t, tt.inputLimit, am.memoryLimit)
			}

			require.Equal(t, tt.inputConfig, am.config)
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	require.Equal(t, 90, config.ThresholdPercent)
	require.Equal(t, 1, config.SampleIntervalSeconds)
}

func TestDefaultDispatchConfig(t *testing.T) {
	config := DefaultDispatchConfig()
	require.Equal(t, 95, config.ThresholdPercent)
	require.Equal(t, 1, config.SampleIntervalSeconds)
}

func TestMemoryProtectionMiddleware_RecordRejection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	am := &MemoryAdmissionMiddleware{
		config: DefaultConfig(),
	}

	// Test API endpoint
	am.recordRejection("/authzed.api.v1.PermissionsService/CheckPermission")

	// Test dispatch endpoint
	am.recordRejection("/dispatch.v1.DispatchService/DispatchCheck")

	// Verify metrics
	apiMetric := testutil.ToFloat64(RejectedRequestsCounter.WithLabelValues("api"))
	dispatchMetric := testutil.ToFloat64(RejectedRequestsCounter.WithLabelValues("dispatch"))

	require.Equal(t, float64(1), apiMetric, "API rejection should be recorded")
	require.Equal(t, float64(1), dispatchMetric, "Dispatch rejection should be recorded")
}

func TestMemoryProtectionMiddleware_SampleMemory(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	am := New(t.Context(), DefaultConfig(), &HardCodedMemoryLimitProvider{Hardcodedlimit: 1024 * 1024})

	am.sampleMemory()

	usage := am.getLastMemorySampleInBytes()
	require.Greater(t, usage, uint64(0))

	// Verify the gauge was updated
	gaugeValue := testutil.ToFloat64(MemoryUsageGauge)
	require.Greater(t, gaugeValue, float64(0), "Memory usage gauge should be updated")
}

func TestMemoryProtectionMiddleware_BackgroundSampling(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Capture original limit
	originalLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(originalLimit)

	// Set a test memory limit
	debug.SetMemoryLimit(100 * 1024 * 1024) // 100MB

	// Create middleware with reasonable threshold
	config := Config{
		ThresholdPercent:      85, // Reasonable threshold
		SampleIntervalSeconds: 2,
	}

	am := New(t.Context(), config, &DefaultMemoryLimitProvider{})
	require.NotNil(t, am)

	now := time.Now()

	// Wait for the background sampling to run
	require.Eventually(t, func() bool {
		t.Log("checking timestamp")
		return am.getTimestampLastMemorySample().After(now)
	}, 3*time.Second, 2*time.Second)

	// Verify that memory usage gauge was updated by background sampling
	gaugeValue := testutil.ToFloat64(MemoryUsageGauge)
	require.Greater(t, gaugeValue, float64(0), "Memory usage gauge should be updated by background sampling")
}

type memoryProtectionTestServer struct {
	testpb.UnimplementedTestServiceServer
}

func (s *memoryProtectionTestServer) PingEmpty(_ context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	return &testpb.PingEmptyResponse{}, nil
}

func (s *memoryProtectionTestServer) PingStream(_ testpb.TestService_PingStreamServer) error {
	return nil
}

// unaryRequestBlockingTestSuite is a test suite for testing unary request blocking
type memoryProtectionMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
	expectBlocked bool
}

func TestMemoryProtectionMiddleware(t *testing.T) {
	tests := []struct {
		name             string
		memoryUsage      uint64
		memoryLimit      int64
		thresholdPercent int
		expectBlocked    bool
	}{
		{
			name:             "below threshold - allowed",
			memoryUsage:      50 * 1024 * 1024,  // 50MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			expectBlocked:    false,
		},
		{
			name:             "above threshold - blocked",
			memoryUsage:      95 * 1024 * 1024,  // 95MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			expectBlocked:    true,
		},
		{
			name:             "at threshold - blocked",
			memoryUsage:      90 * 1024 * 1024,  // 90MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 90,
			expectBlocked:    false,
		},
		{
			name:             "disabled - allows all",
			memoryUsage:      90 * 1024 * 1024,  // 90MB
			memoryLimit:      100 * 1024 * 1024, // 100MB
			thresholdPercent: 0,
			expectBlocked:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create middleware with test configuration
			config := Config{
				ThresholdPercent:      tt.thresholdPercent,
				SampleIntervalSeconds: 100, // no background process modifying current usage
			}

			am := New(t.Context(), config, &HardCodedMemoryLimitProvider{Hardcodedlimit: tt.memoryLimit})

			// Set the memory usage directly for testing
			am.lastMemorySampleInBytes.Store(tt.memoryUsage)

			testSrv := &memoryProtectionTestServer{}
			s := &memoryProtectionMiddlewareTestSuite{
				InterceptorTestSuite: &testpb.InterceptorTestSuite{
					TestService: testSrv,
					ServerOpts: []grpc.ServerOption{
						grpc.ChainUnaryInterceptor(
							am.UnaryServerInterceptor(),
						),
						grpc.ChainStreamInterceptor(
							am.StreamServerInterceptor(),
						),
					},
				},
				expectBlocked: tt.expectBlocked,
			}
			suite.Run(t, s)
		})
	}
}

func (s *memoryProtectionMiddlewareTestSuite) TestUnaryInterceptor_EnforcesMemoryProtection() {
	resp, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{})

	if s.expectBlocked {
		// Request should be blocked
		s.Require().Error(err, "Request should be blocked due to memory pressure")
		s.Require().Nil(resp, "Response should be nil when blocked")
		grpcErr, ok := status.FromError(err)
		s.Require().True(ok, "Error should be a gRPC status error")
		s.Require().Equal(codes.ResourceExhausted, grpcErr.Code(), "Should return ResourceExhausted error")
		s.Require().Contains(grpcErr.Message(), "memory pressure", "Error message should mention memory pressure")
	} else {
		// Request should be allowed
		s.Require().NoError(err, "Request should be allowed")
		s.Require().NotNil(resp, "Response should not be nil when allowed")
	}
}

func (s *memoryProtectionMiddlewareTestSuite) TestStreamingInterceptor_EnforcesMemoryProtection() {
	resp, err := s.Client.PingStream(s.SimpleCtx())
	require.NoError(s.T(), err, "Request should be allowed")

	res, err := resp.Recv()
	if errors.Is(err, io.EOF) {
		return
	}
	if s.expectBlocked {
		// Request should be blocked
		s.Require().Error(err, "Request should be blocked due to memory pressure")
		s.Require().Nil(res, "Response should be nil when blocked")
		grpcErr, ok := status.FromError(err)
		s.Require().True(ok, "Error should be a gRPC status error")
		s.Require().Equal(codes.ResourceExhausted, grpcErr.Code(), "Should return ResourceExhausted error")
		s.Require().Contains(grpcErr.Message(), "memory pressure", "Error message should mention memory pressure")
	} else {
		// Request should be allowed
		s.Require().NoError(err, "Request should be allowed")
		s.Require().NotNil(res, "Response should not be nil when allowed")
	}
}

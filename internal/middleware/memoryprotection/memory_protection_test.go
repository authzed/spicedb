package memoryprotection

import (
	"context"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/middleware/memoryprotection/mocks"
)

func TestDefaultMemoryLimitProvider(t *testing.T) {
	dmlp := &DefaultMemoryLimitProvider{}

	get := dmlp.GetInBytes()
	require.Equal(t, int64(math.MaxInt64), get)

	dmlp.SetInBytes(int64(100))
	get = dmlp.GetInBytes()
	require.Equal(t, int64(100), get)
}

func TestNew(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name           string
		inputConfig    Config
		expectDisabled bool
	}{
		{
			name:           "reasonable config",
			inputConfig:    DefaultConfig(),
			expectDisabled: false,
		},
		{
			name: "disabled via config",
			inputConfig: Config{
				ThresholdPercent: 0,
			},
			expectDisabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lp := HardCodedMemoryLimitProvider{Hardcodedlimit: 100}

			sampler := NewMemorySamplerOnInterval(100, &lp)
			t.Cleanup(sampler.Close)

			am := New(tt.inputConfig, sampler, "name")
			require.NotNil(t, am)
			require.Equal(t, am.disabled(), tt.expectDisabled)

			err := am.checkAdmission("some_method")
			if tt.expectDisabled {
				require.Nil(t, err) // if the middleware is off, every request is let through
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	require.Equal(t, 0.90, config.ThresholdPercent)
}

func TestDefaultDispatchConfig(t *testing.T) {
	config := DefaultDispatchConfig()
	require.Equal(t, 0.95, config.ThresholdPercent)
}

func TestMemoryProtectionMiddleware_RecordRejection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Create a custom registry for testing
	registry := prometheus.NewRegistry()

	// Register our metrics with the test registry
	testRequestsProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "memory_middleware",
		Name:      "requests_processed_total",
		Help:      "Total requests processed by the memory protection middleware (flag --memory-protection-enabled)",
	}, []string{"endpoint", "accepted"})
	registry.MustRegister(testRequestsProcessed)

	// Replace the global counter with our test counter for this test
	originalCounter := RequestsProcessed
	RequestsProcessed = testRequestsProcessed
	defer func() {
		RequestsProcessed = originalCounter
	}()

	lp := HardCodedMemoryLimitProvider{Hardcodedlimit: 100}

	sampler := NewMemorySamplerOnInterval(DefaultSampleIntervalSeconds, &lp)
	t.Cleanup(sampler.Close)

	am := New(DefaultConfig(), sampler, "test")

	// Test API endpoint
	endpointType := am.recordMetric("/authzed.api.v1.PermissionsService/CheckPermission", true)
	require.Equal(t, "api", endpointType)

	// Test dispatch endpoint
	endpointType = am.recordMetric("/dispatch.v1.DispatchService/DispatchCheck", false)
	require.Equal(t, "dispatch", endpointType)

	gaugeValue := testutil.ToFloat64(testRequestsProcessed.WithLabelValues("api", "true"))
	require.Equal(t, float64(1), gaugeValue)

	gaugeValue = testutil.ToFloat64(testRequestsProcessed.WithLabelValues("dispatch", "false"))
	require.Equal(t, float64(1), gaugeValue)
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
		name               string
		memoryUsagePercent float64
		memoryLimit        int64
		thresholdPercent   float64
		expectBlocked      bool
	}{
		{
			name:               "below threshold - allowed",
			memoryUsagePercent: 0.89,
			thresholdPercent:   0.9,
			expectBlocked:      false,
		},
		{
			name:               "above threshold - blocked",
			memoryUsagePercent: 0.91,
			thresholdPercent:   0.90,
			expectBlocked:      true,
		},
		{
			name:               "at threshold - blocked",
			memoryUsagePercent: 0.90,
			thresholdPercent:   0.90,
			expectBlocked:      true,
		},
		{
			name:               "disabled - allows all",
			memoryUsagePercent: 0.90,
			thresholdPercent:   0,
			expectBlocked:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				ThresholdPercent: tt.thresholdPercent,
			}

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mocksampler := mocks.NewMockMemorySampler(ctrl)
			mocksampler.EXPECT().GetMemoryUsagePercent().Return(tt.memoryUsagePercent).AnyTimes()

			am := New(config, mocksampler, "name")

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
	resp, err := s.Client.PingEmpty(context.Background(), &testpb.PingEmptyRequest{})

	if s.expectBlocked {
		// Request should be blocked
		s.Require().Error(err, "Request should be blocked due to memory pressure")
		s.Require().Nil(resp, "Response should be nil when blocked")
		grpcErr, ok := status.FromError(err)
		s.Require().True(ok, "Error should be a gRPC status error")
		s.Require().Equal(codes.ResourceExhausted, grpcErr.Code(), "Should return ResourceExhausted error")
	} else {
		// Request should be allowed
		s.Require().NoError(err, "Request should be allowed")
		s.Require().NotNil(resp, "Response should not be nil when allowed")
	}
}

func (s *memoryProtectionMiddlewareTestSuite) TestStreamingInterceptor_EnforcesMemoryProtection() {
	resp, err := s.Client.PingStream(context.Background())
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
	} else {
		// Request should be allowed
		s.Require().NoError(err, "Request should be allowed")
		s.Require().NotNil(res, "Response should not be nil when allowed")
	}
}

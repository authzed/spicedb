package memoryprotection

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNew(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                string
		inputProvider       MemoryUsageProvider
		expectReqLetThrough bool
	}{
		{
			name: "returns false",
			inputProvider: &HarcodedMemoryLimitProvider{
				AcceptAllRequests: true,
			},
			expectReqLetThrough: true,
		},
		{
			name: "returns true",
			inputProvider: &HarcodedMemoryLimitProvider{
				AcceptAllRequests: false,
			},
			expectReqLetThrough: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am := New(tt.inputProvider, "name")
			require.NotNil(t, am)

			err := am.checkAdmission("some_method")
			if tt.expectReqLetThrough {
				require.Nil(t, err) // if the middleware is off, every request is let through
			} else {
				require.NotNil(t, err)
			}
		})
	}
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

	lp := &HarcodedMemoryLimitProvider{AcceptAllRequests: true}

	am := New(lp, "test")

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
		name             string
		blockAllRequests bool
		expectBlocked    bool
	}{
		{
			name:             "not reached",
			blockAllRequests: false,
			expectBlocked:    false,
		},
		{
			name:             "reached",
			blockAllRequests: true,
			expectBlocked:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			am := New(&HarcodedMemoryLimitProvider{AcceptAllRequests: !tt.blockAllRequests}, "name")

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

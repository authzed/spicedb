package perfinsights

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestBuildLabels(t *testing.T) {
	tcs := []struct {
		name           string
		shape          APIShapeLabels
		expectedLabels []string
	}{
		{
			name:           "empty shape",
			shape:          APIShapeLabels{},
			expectedLabels: []string{"testMethod", "", "", "", "", "", ""},
		},
		{
			name: "full shape",
			shape: APIShapeLabels{
				ResourceTypeLabel:     "resource_type",
				ResourceRelationLabel: "resource_relation",
				SubjectTypeLabel:      "subject_type",
				SubjectRelationLabel:  "subject_relation",
				NameLabel:             "name",
				FilterLabel:           "filter",
			},
			expectedLabels: []string{"testMethod", "resource_type", "resource_relation", "subject_type", "subject_relation", "name", "filter"},
		},
		{
			name: "full shape with integers",
			shape: APIShapeLabels{
				ResourceTypeLabel:     "resource_type",
				ResourceRelationLabel: "resource_relation",
				SubjectTypeLabel:      "subject_type",
				SubjectRelationLabel:  int64(40),
				NameLabel:             uint32(41),
				FilterLabel:           42,
			},
			expectedLabels: []string{"testMethod", "resource_type", "resource_relation", "subject_type", "40", "41", "42"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			labels := buildLabels("testMethod", tc.shape)
			require.Equal(t, tc.expectedLabels, labels)
		})
	}
}

func TestObserveShapeLatency(t *testing.T) {
	reg := prometheus.NewRegistry()

	metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                   "spicedb",
		Subsystem:                   "perf_insights",
		Name:                        "api_shape_latency_seconds",
		Help:                        "The latency of API calls, by shape",
		Buckets:                     nil,
		NativeHistogramBucketFactor: 1.1,
	}, append([]string{"api_kind"}, allLabels...))
	require.NoError(t, reg.Register(metric))

	// Report some data.
	observeShapeLatency(t.Context(), metric, "testMethod", APIShapeLabels{
		ResourceTypeLabel:     "resource_type",
		ResourceRelationLabel: "resource_relation",
		SubjectTypeLabel:      42,
	}, 100*time.Millisecond)

	// Ensure it was added to the metric.
	metrics, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metrics)

	for _, metric := range metrics {
		if !strings.HasSuffix(metric.GetName(), "api_shape_latency_seconds") {
			continue
		}

		require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, metric.GetType())

		require.Equal(t, uint64(1), metric.GetMetric()[0].Histogram.GetSampleCount())
		require.Equal(t, float64(0.1), metric.GetMetric()[0].Histogram.GetSampleSum()) //nolint:testifylint this value is set directly by test code
		require.Equal(t, "testMethod", metric.GetMetric()[0].Label[0].GetValue())

		for _, label := range metric.GetMetric()[0].Label {
			if label.GetName() == "api_kind" {
				require.Equal(t, "testMethod", label.GetValue())
				continue
			}

			if label.GetName() == "resource_type" {
				require.Equal(t, "resource_type", label.GetValue())
				continue
			}

			if label.GetName() == "resource_relation" {
				require.Equal(t, "resource_relation", label.GetValue())
				continue
			}

			if label.GetName() == "subject_type" {
				require.Equal(t, "42", label.GetValue())
				continue
			}

			require.Empty(t, label.GetValue())
		}
	}
}

func TestNoLabels(t *testing.T) {
	labels := NoLabels()
	require.NotNil(t, labels)
	require.Empty(t, labels)
}

func TestSetInContext(t *testing.T) {
	ctx := contextWithHandle(t.Context())
	builder := func() APIShapeLabels {
		return APIShapeLabels{ResourceTypeLabel: "test"}
	}

	SetInContext(ctx, builder)

	result := ctxKey.Value(ctx)
	require.NotNil(t, result)

	shape := result()
	require.Equal(t, "test", shape[ResourceTypeLabel])
}

func TestContextWithHandle(t *testing.T) {
	ctx := t.Context()
	newCtx := contextWithHandle(ctx)
	require.NotNil(t, newCtx)
}

func TestUnaryServerInterceptorDisabled(t *testing.T) {
	interceptor := UnaryServerInterceptor(true)
	require.NotNil(t, interceptor)

	disabledInterceptor := UnaryServerInterceptor(false)
	require.NotNil(t, disabledInterceptor)
}

func TestStreamServerInterceptorDisabled(t *testing.T) {
	interceptor := StreamServerInterceptor(true)
	require.NotNil(t, interceptor)

	disabledInterceptor := StreamServerInterceptor(false)
	require.NotNil(t, disabledInterceptor)
}

// Test gRPC service definitions
type TestRequest struct {
	Message string
}

func (r *TestRequest) Reset()         {}
func (r *TestRequest) String() string { return "TestRequest{" + r.Message + "}" }
func (r *TestRequest) ProtoMessage()  {}

type TestResponse struct {
	Reply string
}

func (r *TestResponse) Reset()         {}
func (r *TestResponse) String() string { return "TestResponse{" + r.Reply + "}" }
func (r *TestResponse) ProtoMessage()  {}

// TestService interface
type TestServiceServer interface {
	UnaryCall(context.Context, *TestRequest) (*TestResponse, error)
	StreamCall(TestServiceStreamCallServer) error
}

// Stream interface for testing
type TestServiceStreamCallServer interface {
	Send(*TestResponse) error
	Recv() (*TestRequest, error)
	grpc.ServerStream
}

// Implementation of the test service
type testServiceImpl struct{}

func (s *testServiceImpl) UnaryCall(ctx context.Context, req *TestRequest) (*TestResponse, error) {
	// Set shape in context to simulate real usage
	SetInContext(ctx, func() APIShapeLabels {
		return APIShapeLabels{
			ResourceTypeLabel:     "document",
			ResourceRelationLabel: "viewer",
			SubjectTypeLabel:      "user",
		}
	})

	// Add some delay to test latency measurement
	time.Sleep(10 * time.Millisecond)
	response := &TestResponse{Reply: "unary response: " + req.Message}
	return response, nil
}

func (s *testServiceImpl) StreamCall(stream TestServiceStreamCallServer) error {
	// Set shape in context to simulate real usage
	SetInContext(stream.Context(), func() APIShapeLabels {
		return APIShapeLabels{
			ResourceTypeLabel:     "folder",
			ResourceRelationLabel: "reader",
			SubjectTypeLabel:      "group",
		}
	})

	// Read from stream and echo back
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		if err := stream.Send(&TestResponse{Reply: "stream response: " + req.Message}); err != nil {
			return err
		}
	}
}

// gRPC service descriptor for the test service
var TestServiceServiceDesc = grpc.ServiceDesc{
	ServiceName: "perfinsights.TestService",
	HandlerType: (*TestServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "UnaryCall",
			Handler:    ServiceUnaryCallHandlerForTesting,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamCall",
			Handler:       ServiceStreamCallHandlerForTesting,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "test.proto",
}

func ServiceUnaryCallHandlerForTesting(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(TestRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TestServiceServer).UnaryCall(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/perfinsights.TestService/UnaryCall",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(TestServiceServer).UnaryCall(ctx, req.(*TestRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func ServiceStreamCallHandlerForTesting(srv any, stream grpc.ServerStream) error {
	return srv.(TestServiceServer).StreamCall(&testServiceStreamCallServer{stream})
}

type testServiceStreamCallServer struct {
	grpc.ServerStream
}

func (x *testServiceStreamCallServer) Send(m *TestResponse) error {
	return x.SendMsg(m)
}

func (x *testServiceStreamCallServer) Recv() (*TestRequest, error) {
	m := new(TestRequest)
	if err := x.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func TestUnaryRPCMetricReporting(t *testing.T) {
	// Test that unary interceptor works by testing the interceptor directly
	service := &testServiceImpl{}

	// Create a context with the middleware's context handling
	ctx := contextWithHandle(t.Context())

	// Create a mock unary server info
	info := &grpc.UnaryServerInfo{
		FullMethod: "/perfinsights.TestService/UnaryCall",
	}

	// Create the interceptor
	interceptor := UnaryServerInterceptor(true)
	require.NotNil(t, interceptor)

	// Create a handler that calls our service
	handler := func(ctx context.Context, req any) (any, error) {
		return service.UnaryCall(ctx, req.(*TestRequest))
	}

	// Call through the interceptor
	req := &TestRequest{Message: "test"}
	resp, err := interceptor(ctx, req, info, handler)
	require.NoError(t, err)

	// Verify response
	testResp, ok := resp.(*TestResponse)
	require.True(t, ok)
	require.Equal(t, "unary response: test", testResp.Reply)
}

func TestStreamRPCMetricReporting(t *testing.T) {
	// Test that stream interceptor works by creating a mock stream
	service := &testServiceImpl{}

	// Create the interceptor
	interceptor := StreamServerInterceptor(true)
	require.NotNil(t, interceptor)

	// Create a mock server stream
	ctx := contextWithHandle(t.Context())
	mockStream := &fakeServerStream{
		ctx:      ctx,
		recvMsgs: []*TestRequest{{Message: "stream test"}},
		recvIdx:  0,
		sentMsgs: []*TestResponse{},
	}

	// Create a handler that calls our service
	handler := func(srv any, stream grpc.ServerStream) error {
		return service.StreamCall(&testServiceStreamCallServer{stream})
	}

	// Create stream server info
	info := &grpc.StreamServerInfo{
		FullMethod:     "/perfinsights.TestService/StreamCall",
		IsClientStream: true,
		IsServerStream: true,
	}

	// Call through the interceptor
	err := interceptor(service, mockStream, info, handler)
	require.NoError(t, err)

	// Verify response was sent
	require.Len(t, mockStream.sentMsgs, 1)
	require.Equal(t, "stream response: stream test", mockStream.sentMsgs[0].Reply)
}

// Fake server stream for testing
type fakeServerStream struct {
	ctx      context.Context
	recvMsgs []*TestRequest
	recvIdx  int
	sentMsgs []*TestResponse
}

func (m *fakeServerStream) Context() context.Context {
	return m.ctx
}

func (m *fakeServerStream) SendMsg(msg any) error {
	if resp, ok := msg.(*TestResponse); ok {
		m.sentMsgs = append(m.sentMsgs, resp)
	}
	return nil
}

func (m *fakeServerStream) RecvMsg(msg any) error {
	if m.recvIdx >= len(m.recvMsgs) {
		return io.EOF
	}
	if req, ok := msg.(*TestRequest); ok {
		*req = *m.recvMsgs[m.recvIdx]
		m.recvIdx++
	}
	return nil
}

func (m *fakeServerStream) SetHeader(md metadata.MD) error  { return nil }
func (m *fakeServerStream) SendHeader(md metadata.MD) error { return nil }
func (m *fakeServerStream) SetTrailer(md metadata.MD)       {}
func (m *fakeServerStream) Method() string                  { return "/perfinsights.TestService/StreamCall" }

// TestPerfInsightsMiddlewareWithBuffcon demonstrates the perfinsights middleware
// integration using buffcon for full gRPC server testing
func TestPerfInsightsMiddlewareWithBuffcon(t *testing.T) {
	// Create buffcon listener for in-memory gRPC communication
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	// Create gRPC server with perfinsights middleware installed
	s := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryServerInterceptor(true)),
		grpc.StreamInterceptor(StreamServerInterceptor(true)),
	)
	defer s.Stop()

	// Register the test service we defined earlier
	service := &testServiceImpl{}
	s.RegisterService(&TestServiceServiceDesc, service)

	// Start the server in a goroutine
	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server failed to serve: %v", err)
		}
	}()

	// Create client connection using buffcon
	conn, err := grpc.NewClient("localhost:fake",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	// Make a unary call to the test service.
	var resp TestResponse
	err = conn.Invoke(t.Context(), "/perfinsights.TestService/UnaryCall", &TestRequest{Message: "buffcon test"}, &resp)
	require.NoError(t, err)

	// Make a stream call to the test service.
	stream, err := conn.NewStream(t.Context(), &TestServiceServiceDesc.Streams[0], "/perfinsights.TestService/StreamCall")
	require.NoError(t, err)

	// Send a request through the stream
	err = stream.SendMsg(&TestRequest{Message: "stream buffcon test"})
	require.NoError(t, err)

	// Receive the response from the stream
	var streamResp TestResponse
	err = stream.RecvMsg(&streamResp)
	require.NoError(t, err)
}

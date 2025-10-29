package requestid

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/authzed-go/pkg/requestmeta"
)

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t *testServer) PingEmpty(ctx context.Context, req *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	return &testpb.PingEmptyResponse{}, nil
}

func (t *testServer) Ping(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	// Check if this is an error test request
	if req.GetValue() == "ERROR" { // Use value as error trigger
		return nil, status.Errorf(codes.Internal, "test error")
	}
	return &testpb.PingResponse{Counter: 1}, nil
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Send a few responses to test server streaming
	for i := 0; i < 3; i++ {
		if err := server.Send(&testpb.PingListResponse{Value: "ping"}); err != nil {
			return err
		}
	}
	return nil
}

func (t *testServer) PingStream(server testpb.TestService_PingStreamServer) error {
	// Handle bidirectional streaming - echo back what we receive
	for {
		req, err := server.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		if err := server.Send(&testpb.PingStreamResponse{Value: req.Value}); err != nil {
			return err
		}
	}
}

// requestIDMiddlewareTestSuite is a test suite for the requestid middleware
type requestIDMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestRequestIDMiddleware(t *testing.T) {
	s := &requestIDMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(GenerateIfMissing(true)),
				),
				grpc.ChainStreamInterceptor(
					StreamServerInterceptor(GenerateIfMissing(true)),
				),
			},
			ClientOpts: []grpc.DialOption{
				grpc.WithChainUnaryInterceptor(
					UnaryClientInterceptor(GenerateIfMissing(true)),
				),
				grpc.WithChainStreamInterceptor(
					StreamClientInterceptor(GenerateIfMissing(true)),
				),
			},
		},
	}
	suite.Run(t, s)
}

func (s *requestIDMiddlewareTestSuite) TestUnaryInterceptor_GeneratesAndReturnsRequestID() {
	// Test unary RPC - should generate request ID and return it in response trailers
	var trailer metadata.MD
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	// Check that response metadata contains request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs, "Request ID should be present in response trailers")
	require.Len(s.T(), requestIDs, 1, "Should have exactly one request ID")
	require.NotEmpty(s.T(), requestIDs[0], "Request ID should not be empty")
}

func (s *requestIDMiddlewareTestSuite) TestUnaryInterceptor_PreservesExistingRequestID() {
	// Test unary RPC with existing request ID in incoming metadata
	existingRequestID := "test-request-123"

	ctx := requestmeta.WithRequestID(s.SimpleCtx(), existingRequestID)

	var trailer metadata.MD
	_, err := s.Client.PingEmpty(ctx, &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	// Check that response metadata contains the same request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs)
	require.Equal(s.T(), existingRequestID, requestIDs[0], "Should preserve existing request ID")
}

func (s *requestIDMiddlewareTestSuite) TestServerStreamingInterceptor_ReturnsRequestID() {
	// Test server streaming RPC - should return request ID in response trailers
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	// Receive all messages to complete the stream
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(s.T(), err)
	}

	// Get trailers after stream completion
	trailer := stream.Trailer()

	// Check that response metadata contains request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs, "Request ID should be present in response trailers")
	require.NotEmpty(s.T(), requestIDs[0], "Request ID should not be empty")
}

func (s *requestIDMiddlewareTestSuite) TestServerStreamingInterceptor_PreservesExistingRequestID() {
	// Test server streaming RPC with existing request ID
	existingRequestID := "test-stream-456"
	ctx := requestmeta.WithRequestID(s.SimpleCtx(), existingRequestID)

	stream, err := s.Client.PingList(ctx, &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	// Complete the stream
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(s.T(), err)
	}

	// Get trailers after stream completion
	trailer := stream.Trailer()

	// Check that response metadata contains the same request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs)
	require.Equal(s.T(), existingRequestID, requestIDs[0], "Should preserve existing request ID")
}

func (s *requestIDMiddlewareTestSuite) TestBidirectionalStreamingInterceptor_ReturnsRequestID() {
	// Test bidirectional streaming RPC - should return request ID in response trailers
	stream, err := s.Client.PingStream(s.SimpleCtx())
	require.NoError(s.T(), err)

	// Send a message first
	err = stream.Send(&testpb.PingStreamRequest{Value: "test"})
	require.NoError(s.T(), err)

	// Receive the echoed message
	resp, err := stream.Recv()
	require.NoError(s.T(), err)
	require.Equal(s.T(), "test", resp.Value)

	// Close the stream
	err = stream.CloseSend()
	require.NoError(s.T(), err)

	// need to call receive on bidirectional after close, otherwise you get no trailer frame
	_, err = stream.Recv()
	require.ErrorIs(s.T(), err, io.EOF)

	// Get trailers after stream completion
	trailer := stream.Trailer()

	// Check that response metadata contains request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs, "Request ID should be present in response trailers")
	require.NotEmpty(s.T(), requestIDs[0], "Request ID should not be empty")
}

func (s *requestIDMiddlewareTestSuite) TestBidirectionalStreamingInterceptor_PreservesExistingRequestID() {
	// Test bidirectional streaming RPC with existing request ID
	existingRequestID := "test-bidi-789"
	ctx := requestmeta.WithRequestID(s.SimpleCtx(), existingRequestID)

	stream, err := s.Client.PingStream(ctx)
	require.NoError(s.T(), err)

	// Send a message first
	err = stream.Send(&testpb.PingStreamRequest{Value: "test"})
	require.NoError(s.T(), err)

	// Complete the stream
	_, err = stream.Recv()
	require.NoError(s.T(), err)

	err = stream.CloseSend()
	require.NoError(s.T(), err)

	// need to call receive on bidirectional after close, otherwise you get no trailer frame
	_, err = stream.Recv()
	require.ErrorIs(s.T(), err, io.EOF)

	// Get trailers after stream completion
	trailer := stream.Trailer()

	// Check that response metadata contains the same request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs)
	require.Equal(s.T(), existingRequestID, requestIDs[0], "Should preserve existing request ID")
}

func (s *requestIDMiddlewareTestSuite) TestUnaryInterceptor_ReturnsRequestIDOnError() {
	// Test unary RPC that returns an error - should still return request ID in response trailers
	var trailer metadata.MD
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "ERROR"}, grpc.Trailer(&trailer))
	require.Error(s.T(), err, "Should return error for Value='ERROR'")

	// Check that response metadata contains request ID even when there's an error
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs, "Request ID should be present in response trailers even on error")
	require.NotEmpty(s.T(), requestIDs[0], "Request ID should not be empty even on error")
}

// Test suite for middleware without generation enabled
type requestIDNoGenerateMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestRequestIDMiddlewareNoGenerate(t *testing.T) {
	s := &requestIDNoGenerateMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(), // No generation enabled
				),
				grpc.ChainStreamInterceptor(
					StreamServerInterceptor(),
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
	}
	suite.Run(t, s)
}

func (s *requestIDNoGenerateMiddlewareTestSuite) TestUnaryInterceptor_NoRequestIDWithoutGeneration() {
	// Test unary RPC without generation - should not add request ID if none provided
	var trailer metadata.MD
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	// Check that response metadata does not contain request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.Empty(s.T(), requestIDs, "Request ID should not be present when generation is disabled")
}

func (s *requestIDNoGenerateMiddlewareTestSuite) TestUnaryInterceptor_PreservesProvidedRequestID() {
	// Test unary RPC with provided request ID - should still preserve it even without generation
	existingRequestID := "provided-request-id"
	ctx := requestmeta.WithRequestID(s.SimpleCtx(), existingRequestID)

	var trailer metadata.MD
	_, err := s.Client.PingEmpty(ctx, &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	// Check that response metadata contains the provided request ID
	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs)
	require.Equal(s.T(), existingRequestID, requestIDs[0], "Should preserve provided request ID")
}

// Test suite for custom ID generator
type requestIDCustomGeneratorTestSuite struct {
	*testpb.InterceptorTestSuite
	customIDPrefix string
}

func TestRequestIDMiddlewareCustomGenerator(t *testing.T) {
	customIDPrefix := "custom-"
	customGenerator := func() string {
		return customIDPrefix + GenerateRequestID()
	}

	customGeneratorOption := func(h *handleRequestID) {
		h.requestIDGenerator = customGenerator
	}

	s := &requestIDCustomGeneratorTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(
						GenerateIfMissing(true),
						customGeneratorOption,
					),
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
		customIDPrefix: customIDPrefix,
	}
	suite.Run(t, s)
}

func (s *requestIDCustomGeneratorTestSuite) TestCustomGenerator() {
	var trailer metadata.MD
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	requestIDs := trailer.Get("io.spicedb.respmeta.requestid")
	require.NotEmpty(s.T(), requestIDs)
	require.Contains(s.T(), requestIDs[0], s.customIDPrefix, "Custom generator should be used")
}

package serverversion

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
)

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t *testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	return &testpb.PingEmptyResponse{}, nil
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	return server.Send(&testpb.PingListResponse{Value: "ping"})
}

// serverVersionMiddlewareTestSuite is a test suite for the server version middleware
type serverVersionMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestServerVersionMiddleware(t *testing.T) {
	s := &serverVersionMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(true),
				),
				grpc.ChainStreamInterceptor(
					StreamServerInterceptor(true),
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
	}
	suite.Run(t, s)
}

func (s *serverVersionMiddlewareTestSuite) TestUnaryInterceptor_WithVersionRequest() {
	// Create context with server version request header
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), string(requestmeta.RequestServerVersion), "true")

	_, err := s.Client.PingEmpty(ctx, &testpb.PingEmptyRequest{})
	require.NoError(s.T(), err)

	// Check that response metadata contains server version
	var trailer metadata.MD
	_, err = s.Client.PingEmpty(ctx, &testpb.PingEmptyRequest{}, grpc.Trailer(&trailer))
	require.NoError(s.T(), err)

	serverVersion := trailer.Get(string(responsemeta.ServerVersion))
	require.NotEmpty(s.T(), serverVersion)
}

func (s *serverVersionMiddlewareTestSuite) TestUnaryInterceptor_WithoutVersionRequest() {
	// Call without server version request header
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{})
	require.NoError(s.T(), err)

	// Check that response metadata does not contain server version
	var header metadata.MD
	_, err = s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{}, grpc.Header(&header))
	require.NoError(s.T(), err)

	serverVersion := header.Get(string(responsemeta.ServerVersion))
	require.Empty(s.T(), serverVersion)
}

func (s *serverVersionMiddlewareTestSuite) TestStreamInterceptor_WithVersionRequest() {
	// Create context with server version request header
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), string(requestmeta.RequestServerVersion), "true")

	stream, err := s.Client.PingList(ctx, &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	// Receive the response, force trailer to be sent
	_, err = stream.Recv()
	require.NoError(s.T(), err)

	// Check that response metadata contains server version
	header := stream.Trailer()

	serverVersion := header.Get(string(responsemeta.ServerVersion))
	require.NotEmpty(s.T(), serverVersion)
}

func (s *serverVersionMiddlewareTestSuite) TestStreamInterceptor_WithoutVersionRequest() {
	// Call without server version request header
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	// Check that response metadata does not contain server version
	header, err := stream.Header()
	require.NoError(s.T(), err)

	serverVersion := header.Get(string(responsemeta.ServerVersion))
	require.Empty(s.T(), serverVersion)

	// Receive the response
	_, err = stream.Recv()
	require.NoError(s.T(), err)
}

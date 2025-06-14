package nodeid

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
	lastNodeID string // Store the last node ID for verification
}

func (t *testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	// Extract node ID from context and store it
	nodeID, err := FromContext(ctx)
	if err != nil {
		return nil, err
	}

	t.lastNodeID = nodeID
	return &testpb.PingEmptyResponse{}, nil
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Extract node ID from context
	nodeID, err := FromContext(server.Context())
	if err != nil {
		return err
	}

	// Send the node ID in the response
	return server.Send(&testpb.PingListResponse{Value: nodeID})
}

// nodeIDMiddlewareTestSuite is a test suite for the nodeid middleware
type nodeIDMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
	testNodeID string
	testSrv    *testServer
}

func TestNodeIDMiddleware(t *testing.T) {
	testNodeID := "test-node-123"
	testSrv := &testServer{}

	s := &nodeIDMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: testSrv,
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(testNodeID),
				),
				grpc.ChainStreamInterceptor(
					StreamServerInterceptor(testNodeID),
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
		testNodeID: testNodeID,
		testSrv:    testSrv,
	}
	suite.Run(t, s)
}

func (s *nodeIDMiddlewareTestSuite) TestUnaryInterceptor_SetsNodeID() {
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{})
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.testNodeID, s.testSrv.lastNodeID)
}

func (s *nodeIDMiddlewareTestSuite) TestStreamInterceptor_SetsNodeID() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	resp, err := stream.Recv()
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.testNodeID, resp.Value)
}

// Test suite for default node ID behavior
type nodeIDDefaultMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
	testSrv *testServer
}

func TestNodeIDMiddlewareDefault(t *testing.T) {
	testSrv := &testServer{}
	s := &nodeIDDefaultMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: testSrv,
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					UnaryServerInterceptor(""), // Empty node ID should use default
				),
				grpc.ChainStreamInterceptor(
					StreamServerInterceptor(""), // Empty node ID should use default
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
		testSrv: testSrv,
	}
	suite.Run(t, s)
}

func (s *nodeIDDefaultMiddlewareTestSuite) TestUnaryInterceptor_UsesDefaultNodeID() {
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{})
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), s.testSrv.lastNodeID)
	require.Contains(s.T(), s.testSrv.lastNodeID, spiceDBPrefix)
}

func (s *nodeIDDefaultMiddlewareTestSuite) TestStreamInterceptor_UsesDefaultNodeID() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{})
	require.NoError(s.T(), err)

	resp, err := stream.Recv()
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), resp.Value)
	require.Contains(s.T(), resp.Value, spiceDBPrefix)
}

// Test ContextWithHandle and FromContext functions directly
func TestContextFunctions(t *testing.T) {
	ctx := t.Context()

	// Test context without handle
	_, err := FromContext(ctx)
	require.NoError(t, err) // Should return default node ID

	// Test context with handle but no node ID set
	ctxWithHandle := ContextWithHandle(ctx)
	nodeID, err := FromContext(ctxWithHandle)
	require.NoError(t, err)
	require.NotEmpty(t, nodeID)
	require.Contains(t, nodeID, spiceDBPrefix)

	// Test setting node ID in context
	testNodeID := "test-node-456"
	err = setInContext(ctxWithHandle, testNodeID)
	require.NoError(t, err)

	retrievedNodeID, err := FromContext(ctxWithHandle)
	require.NoError(t, err)
	require.Equal(t, testNodeID, retrievedNodeID)
}

func TestSetInContextWithoutHandle(t *testing.T) {
	ctx := t.Context()
	err := setInContext(ctx, "test-node")
	require.NoError(t, err) // Should not error when no handle present
}

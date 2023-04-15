package streamtimeout

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t testServer) PingEmpty(_ context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	return &testpb.PingEmptyResponse{}, nil
}

func (t testServer) Ping(_ context.Context, _ *testpb.PingRequest) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Value: ""}, nil
}

func (t testServer) PingError(_ context.Context, _ *testpb.PingErrorRequest) (*testpb.PingErrorResponse, error) {
	return nil, fmt.Errorf("err")
}

func (t testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	var counter int32
	for {
		// Produce ping responses until the context is canceled.
		select {
		case <-server.Context().Done():
			return server.Context().Err()

		default:
			counter++
			err := server.Send(&testpb.PingListResponse{Counter: counter})
			if err != nil {
				return err
			}
			time.Sleep(time.Duration(counter*10) * time.Millisecond)
		}
	}
}

func (t testServer) PingStream(_ testpb.TestService_PingStreamServer) error {
	return fmt.Errorf("unused")
}

type testSuite struct {
	*testpb.InterceptorTestSuite
}

func TestStreamTimeoutMiddleware(t *testing.T) {
	s := &testSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.StreamInterceptor(MustStreamServerInterceptor(50 * time.Millisecond)),
			},
			ClientOpts: []grpc.DialOption{},
		},
	}
	suite.Run(t, s)
}

func (s *testSuite) TestStreamTimeout() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "something"})
	require.NoError(s.T(), err)

	var maxCounter int32

	for {
		// Ensure if we get an error, it is because the context was canceled.
		resp, err := stream.Recv()
		if err != nil {
			require.ErrorContains(s.T(), err, "context canceled")
			return
		}

		// Ensure that we produced a *maximum* of 6 responses (timeout is 50ms and each response
		// should take 10ms * counter). This ensures that we timed out (roughly) when expected.
		maxCounter = resp.Counter
		require.LessOrEqual(s.T(), maxCounter, int32(6), "stream was not properly canceled: %d", maxCounter)
	}
}

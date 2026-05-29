package streamtimeout

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
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
	s.Require().NoError(err)

	var maxCounter int32

	for {
		// Ensure if we get an error, it is because the context was canceled.
		resp, err := stream.Recv()
		if err != nil {
			s.Require().ErrorContains(err, "context canceled")
			return
		}

		// Ensure that we produced a *maximum* of 6 responses (timeout is 50ms and each response
		// should take 10ms * counter). This ensures that we timed out (roughly) when expected.
		maxCounter = resp.Counter
		s.Require().LessOrEqual(maxCounter, int32(6), "stream was not properly canceled: %d", maxCounter)
	}
}

type exemptTestServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t exemptTestServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Sleep well past the 50ms timeout before sending anything. If the
	// interceptor exemption works, the handler receives the original stream
	// whose context is not bound to the timer, so Context().Err() stays nil.
	// If the exemption is broken, the interceptor wraps the stream with a
	// cancelable context that fires at 50ms — Context().Err() then returns
	// the DeadlineExceeded cause and this test fails (the smoking gun that
	// distinguishes the fix from a no-op).
	time.Sleep(150 * time.Millisecond)
	if err := server.Context().Err(); err != nil {
		return fmt.Errorf("expected uncanceled context on exempt method, got %w", err)
	}
	if err := server.Send(&testpb.PingListResponse{Counter: 1}); err != nil {
		return err
	}
	return nil
}

type exemptTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestStreamTimeoutExemptedMethods(t *testing.T) {
	s := &exemptTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &exemptTestServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.StreamInterceptor(MustStreamServerInterceptor(
					50*time.Millisecond,
					WithExemptMethods(testpb.TestService_PingList_FullMethodName),
				)),
			},
		},
	}
	suite.Run(t, s)
}

func (s *exemptTestSuite) TestExemptedMethodIsNotCanceled() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "exempt"})
	s.Require().NoError(err)

	resp, err := stream.Recv()
	s.Require().NoError(err, "exempt method must not be canceled by streamtimeout")
	s.Require().Equal(int32(1), resp.Counter)

	_, err = stream.Recv()
	s.Require().ErrorContains(err, "EOF")
}

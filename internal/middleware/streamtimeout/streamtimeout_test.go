package streamtimeout

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type recvActivityTestServer struct {
	testpb.UnimplementedTestServiceServer
}

// PingClientStream mirrors the shape of ImportBulkRelationships: it consumes
// batches from the client and does not send anything until SendAndClose.
func (t recvActivityTestServer) PingClientStream(server testpb.TestService_PingClientStreamServer) error {
	var received int32
	for {
		_, err := server.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		received++
	}

	// The client has half-closed, so nothing further will arrive to reset the timer.
	// Committing the received work must not be bounded by the timeout either, so stay
	// quiet well past it before responding.
	time.Sleep(150 * time.Millisecond)
	if err := server.Context().Err(); err != nil {
		return fmt.Errorf("context canceled after client half-close: %w", err)
	}

	return server.SendAndClose(&testpb.PingClientStreamResponse{Counter: received})
}

type recvActivityTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestStreamTimeoutResetByReceives(t *testing.T) {
	s := &recvActivityTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &recvActivityTestServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.StreamInterceptor(MustStreamServerInterceptor(50 * time.Millisecond)),
			},
		},
	}
	suite.Run(t, s)
}

func (s *recvActivityTestSuite) TestReceivesResetTheTimeout() {
	stream, err := s.Client.PingClientStream(s.SimpleCtx())
	s.Require().NoError(err)

	// Send batches spaced under the 50ms timeout but totaling well over it. Each
	// receive has to reset the timer, otherwise the call is canceled partway through
	// even though the client never stopped sending.
	for range 5 {
		s.Require().NoError(stream.Send(&testpb.PingClientStreamRequest{Value: "batch"}))
		time.Sleep(30 * time.Millisecond)
	}

	resp, err := stream.CloseAndRecv()
	s.Require().NoError(err, "client-streaming call must not be canceled while the client is sending")
	s.Require().Equal(int32(5), resp.Counter)
}

type mixedTestServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t mixedTestServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	time.Sleep(150 * time.Millisecond)
	if err := server.Context().Err(); err != nil {
		return fmt.Errorf("exempt method was canceled: %w", err)
	}
	return server.Send(&testpb.PingListResponse{Counter: 1})
}

// PingStream is quiet in both directions and is not exempt, so the timer must fire.
// It waits on the context rather than blocking in Recv, because canceling the
// interceptor's derived context does not unblock a receive already in flight.
func (t mixedTestServer) PingStream(server testpb.TestService_PingStreamServer) error {
	select {
	case <-server.Context().Done():
		return server.Context().Err()
	case <-time.After(time.Second):
		return fmt.Errorf("non-exempt method was not canceled by streamtimeout")
	}
}

type mixedTestSuite struct {
	*testpb.InterceptorTestSuite
}

// TestStreamTimeoutMixedExemptions guards against an exemption that applies too
// broadly: one interceptor exempts PingList while PingStream must still time out.
func TestStreamTimeoutMixedExemptions(t *testing.T) {
	s := &mixedTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &mixedTestServer{},
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

func (s *mixedTestSuite) TestExemptMethodSurvives() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "exempt"})
	s.Require().NoError(err)

	resp, err := stream.Recv()
	s.Require().NoError(err)
	s.Require().Equal(int32(1), resp.Counter)
}

func (s *mixedTestSuite) TestNonExemptMethodStillTimesOut() {
	stream, err := s.Client.PingStream(s.SimpleCtx())
	s.Require().NoError(err)

	_, err = stream.Recv()
	s.Require().Error(err, "non-exempt method must still be canceled by streamtimeout")
	s.Require().ErrorContains(err, "context canceled")
}

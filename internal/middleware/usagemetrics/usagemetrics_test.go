package usagemetrics

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/authzed-go/pkg/responsemeta"

	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t *testServer) Ping(ctx context.Context, _ *testpb.PingRequest) (*testpb.PingResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return &testpb.PingResponse{Value: ""}, nil
}

// PingError returns the context error
func (t *testServer) PingError(ctx context.Context, _ *testpb.PingErrorRequest) (*testpb.PingErrorResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	SetInContext(server.Context(), &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return nil
}

type metricsMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
}

func TestMetricsMiddleware(t *testing.T) {
	s := &metricsMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.UnaryInterceptor(UnaryServerInterceptor()),
				grpc.StreamInterceptor(StreamServerInterceptor()),
			},
			ClientOpts: []grpc.DialOption{},
		},
	}
	suite.Run(t, s)
}

func (s *metricsMiddlewareTestSuite) TestTrailers_Unary() {
	var trailerMD metadata.MD
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "something"}, grpc.Trailer(&trailerMD))
	s.Require().NoError(err)

	dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.DispatchedOperationsCount,
	)
	s.Require().NoError(err)
	s.Require().Equal(1, dispatchCount)

	cachedCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.CachedOperationsCount,
	)
	s.Require().NoError(err)
	s.Require().Equal(1, cachedCount)
}

func (s *metricsMiddlewareTestSuite) TestTrailers_Stream() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "something"})
	s.Require().NoError(err)
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		s.Require().NoError(err, "no error on messages sent occurred")
	}

	dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(
		stream.Trailer(),
		responsemeta.DispatchedOperationsCount,
	)
	s.Require().NoError(err)
	s.Require().Equal(1, dispatchCount)

	cachedCount, err := responsemeta.GetIntResponseTrailerMetadata(
		stream.Trailer(),
		responsemeta.CachedOperationsCount,
	)
	s.Require().NoError(err)
	s.Require().Equal(1, cachedCount)
}

func (s *metricsMiddlewareTestSuite) TestErrCtx() {
	var trailerMD metadata.MD

	// SimpleCtx times out after two seconds
	_, err := s.Client.PingError(s.SimpleCtx(), &testpb.PingErrorRequest{}, grpc.Trailer(&trailerMD))

	// the error may come from the grpc framework (client-side timeout) or from the API itself (it's a race)
	s.Require().Equal(codes.DeadlineExceeded, status.Code(err))

	// TODO ideally, this test would assert that no error log has been written
	// but right now we have no way of capturing the logs

	// No metadata should have been sent
	dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.DispatchedOperationsCount,
	)
	s.Require().ErrorContains(err, "key `io.spicedb.respmeta.dispatchedoperationscount` not found in trailer")
	s.Require().Equal(0, dispatchCount)

	cachedCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.CachedOperationsCount,
	)
	s.Require().ErrorContains(err, "key `io.spicedb.respmeta.cachedoperationscount` not found in trailer")
	s.Require().Equal(0, cachedCount)
}

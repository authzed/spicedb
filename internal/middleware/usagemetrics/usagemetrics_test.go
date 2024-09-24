package usagemetrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return &testpb.PingEmptyResponse{}, nil
}

func (t testServer) Ping(ctx context.Context, _ *testpb.PingRequest) (*testpb.PingResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return &testpb.PingResponse{Value: ""}, nil
}

func (t testServer) PingError(ctx context.Context, _ *testpb.PingErrorRequest) (*testpb.PingErrorResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return nil, fmt.Errorf("err")
}

func (t testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	SetInContext(server.Context(), &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return nil
}

func (t testServer) PingStream(stream testpb.TestService_PingStreamServer) error {
	count := int32(0)
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		_ = stream.Send(&testpb.PingStreamResponse{Value: "", Counter: count})
		count++
	}
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
	require.NoError(s.T(), err)

	dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.DispatchedOperationsCount,
	)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, dispatchCount)

	cachedCount, err := responsemeta.GetIntResponseTrailerMetadata(
		trailerMD,
		responsemeta.CachedOperationsCount,
	)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, cachedCount)
}

func (s *metricsMiddlewareTestSuite) TestTrailers_Stream() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "something"})
	require.NoError(s.T(), err)
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(s.T(), err, "no error on messages sent occurred")
	}

	dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(
		stream.Trailer(),
		responsemeta.DispatchedOperationsCount,
	)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, dispatchCount)

	cachedCount, err := responsemeta.GetIntResponseTrailerMetadata(
		stream.Trailer(),
		responsemeta.CachedOperationsCount,
	)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, cachedCount)
}

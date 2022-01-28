package usagemetrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	grpc_testing "github.com/grpc-ecosystem/go-grpc-middleware/testing"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type testServer struct{}

func (t testServer) PingEmpty(ctx context.Context, empty *pb_testproto.Empty) (*pb_testproto.PingResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return &pb_testproto.PingResponse{Value: ""}, nil
}

func (t testServer) Ping(ctx context.Context, request *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return &pb_testproto.PingResponse{Value: ""}, nil
}

func (t testServer) PingError(ctx context.Context, request *pb_testproto.PingRequest) (*pb_testproto.Empty, error) {
	SetInContext(ctx, &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return nil, fmt.Errorf("err")
}

func (t testServer) PingList(request *pb_testproto.PingRequest, server pb_testproto.TestService_PingListServer) error {
	SetInContext(server.Context(), &dispatch.ResponseMeta{
		DispatchCount:       1,
		CachedDispatchCount: 1,
	})
	return nil
}

func (t testServer) PingStream(stream pb_testproto.TestService_PingStreamServer) error {
	count := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		_ = stream.Send(&pb_testproto.PingResponse{Value: "", Counter: int32(count)})
		count++
	}
	return nil
}

type metricsMiddlewareTestSuite struct {
	*grpc_testing.InterceptorTestSuite
}

func TestMetricsMiddleware(t *testing.T) {
	s := &metricsMiddlewareTestSuite{
		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
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
	_, err := s.Client.Ping(s.SimpleCtx(), &pb_testproto.PingRequest{Value: "something"}, grpc.Trailer(&trailerMD))
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
	stream, err := s.Client.PingList(s.SimpleCtx(), &pb_testproto.PingRequest{Value: "something"})
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

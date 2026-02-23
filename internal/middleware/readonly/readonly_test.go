package readonly

import (
	"context"
	"errors"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
)

// fakeDatastore implements the ReadOnlyDatastore interface for testing.
type fakeDatastore struct {
	datastore.ReadOnlyDatastore
}

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t *testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	// Try to use ReadWriteTx which should be blocked by readonly middleware
	dl := datalayer.FromContext(ctx)
	if dl == nil {
		return nil, errors.New("no datastore in context")
	}

	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &testpb.PingEmptyResponse{}, nil
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Try to use ReadWriteTx which should be blocked by readonly middleware
	dl := datalayer.FromContext(server.Context())
	if dl == nil {
		return errors.New("no datastore in context")
	}

	_, err := dl.ReadWriteTx(server.Context(), func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
		return nil
	})
	if err != nil {
		return err
	}

	return server.Send(&testpb.PingListResponse{Value: "ping"})
}

// readonlyMiddlewareTestSuite is a test suite for the readonly middleware
type readonlyMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
	fakeDS *fakeDatastore
}

func TestReadonlyMiddleware(t *testing.T) {
	fakeDS := &fakeDatastore{}

	s := &readonlyMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.ChainUnaryInterceptor(
					datalayer.UnaryServerInterceptor(datalayer.NewReadOnlyDataLayer(fakeDS)),
					UnaryServerInterceptor(),
				),
				grpc.ChainStreamInterceptor(
					datalayer.StreamServerInterceptor(datalayer.NewReadOnlyDataLayer(fakeDS)),
					StreamServerInterceptor(),
				),
			},
			ClientOpts: []grpc.DialOption{},
		},
		fakeDS: fakeDS,
	}
	suite.Run(t, s)
}

func (s *readonlyMiddlewareTestSuite) TestUnaryInterceptor_BlocksReadWriteTx() {
	// This should return an error from the readonly proxy, not panic
	_, err := s.Client.PingEmpty(s.SimpleCtx(), &testpb.PingEmptyRequest{})
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "datastore is in read-only mode")
}

func (s *readonlyMiddlewareTestSuite) TestStreamInterceptor_BlocksReadWriteTx() {
	// This should return an error from the readonly proxy, not panic
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{})
	s.Require().NoError(err)

	_, err = stream.Recv()
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "datastore is in read-only mode")
}

package readonly

import (
	"context"
	"errors"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// fakeDatastore implements the datastore interface but panics on ReadWriteTx calls
type fakeDatastore struct {
	datastore.ReadOnlyDatastore
}

func (f *fakeDatastore) ReadWriteTx(ctx context.Context, fn datastore.TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	panic("ReadWriteTx should not be called in readonly mode")
}

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t *testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	// Try to use ReadWriteTx which should be blocked by readonly middleware
	ds := datastoremw.FromContext(ctx)
	if ds == nil {
		return nil, errors.New("no datastore in context")
	}

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &testpb.PingEmptyResponse{}, nil
}

func (t *testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Try to use ReadWriteTx which should be blocked by readonly middleware
	ds := datastoremw.FromContext(server.Context())
	if ds == nil {
		return errors.New("no datastore in context")
	}

	_, err := ds.ReadWriteTx(server.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
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
					datastoremw.UnaryServerInterceptor(fakeDS),
					UnaryServerInterceptor(),
				),
				grpc.ChainStreamInterceptor(
					datastoremw.StreamServerInterceptor(fakeDS),
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

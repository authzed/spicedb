package consistency

import (
	"context"
	"errors"
	"io"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/common/revisions"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var (
	zero      = revisions.NewFromDecimal(decimal.NewFromInt(0))
	optimized = revisions.NewFromDecimal(decimal.NewFromInt(100))
	exact     = revisions.NewFromDecimal(decimal.NewFromInt(123))
	head      = revisions.NewFromDecimal(decimal.NewFromInt(145))
)

func TestAddRevisionToContextNoneSupplied(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds)
	require.NoError(err)
	require.True(optimized.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextMinimizeLatency(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds)
	require.NoError(err)
	require.True(optimized.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds)
	require.NoError(err)
	require.True(head.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtLeastAsFresh(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("OptimizedRevision").Return(optimized, nil).Once()
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(exact),
			},
		},
	}, ds)
	require.NoError(err)
	require.True(exact.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtValidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", exact).Return(nil).Times(1)
	ds.On("RevisionFromString", exact.String()).Return(exact, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.NewFromRevision(exact),
			},
		},
	}, ds)
	require.NoError(err)
	require.True(exact.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAtInvalidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("CheckRevision", zero).Return(errors.New("bad revision")).Times(1)
	ds.On("RevisionFromString", zero.String()).Return(zero, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.NewFromRevision(zero),
			},
		},
	}, ds)
	require.Error(err)
	ds.AssertExpectations(t)
}

func TestAddRevisionToContextAPIAlwaysFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, nil).Once()

	updated := ContextWithHandle(context.Background())
	err := AddRevisionToContext(updated, &v1.WriteSchemaRequest{}, ds)
	require.NoError(err)
	require.True(head.Equal(RevisionFromContext(updated)))
	ds.AssertExpectations(t)
}

func TestMiddlewareConsistencyTestSuite(t *testing.T) {
	ds := &proxy_test.MockDatastore{}
	ds.On("HeadRevision").Return(head, nil)

	s := &ConsistencyTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			ServerOpts: []grpc.ServerOption{
				grpc.ChainStreamInterceptor(
					datastoremw.StreamServerInterceptor(ds),
					StreamServerInterceptor(),
				),
				grpc.ChainUnaryInterceptor(
					datastoremw.UnaryServerInterceptor(ds),
					UnaryServerInterceptor(),
				),
			},
		},
	}
	suite.Run(t, s)
	ds.AssertExpectations(t)
}

var (
	goodPing = &testpb.PingRequest{Value: "something"}
	goodList = &testpb.PingListRequest{Value: "something"}
)

type ConsistencyTestSuite struct {
	*testpb.InterceptorTestSuite
}

func (s *ConsistencyTestSuite) TestValidPasses_Unary() {
	require := require.New(s.T())
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	require.NoError(err)
}

func (s *ConsistencyTestSuite) TestValidPasses_ServerStream() {
	require := require.New(s.T())
	stream, err := s.Client.PingList(s.SimpleCtx(), goodList)
	require.NoError(err)
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(s.T(), err, "no error on messages sent occurred")
	}
}

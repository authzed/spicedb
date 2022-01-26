package consistency

import (
	"context"
	"errors"
	"io"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	grpc_testing "github.com/grpc-ecosystem/go-grpc-middleware/testing"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/zedtoken"
	"github.com/authzed/spicedb/pkg/zookie"
)

func TestAddRevisionToContextNoneSupplied(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.OptimizedRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextMinimizeLatency(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.OptimizedRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		},
	}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		},
	}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextAtLeastAsFresh(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(decimal.Zero),
			},
		},
	}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextAtValidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.NewFromRevision(databaseRev),
			},
		},
	}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextAtInvalidExactSnapshot(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: zedtoken.NewFromRevision(decimal.Zero),
			},
		},
	}, ds)
	require.Error(err)
}

func TestAddRevisionToContextV0AtRevision(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v0.ReadRequest{AtRevision: zookie.NewFromRevision(databaseRev)}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextV0NoAtRevision(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v0.ReadRequest{}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestAddRevisionToContextAPIAlwaysFullyConsistent(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	databaseRev, err := ds.HeadRevision(context.Background())
	require.NoError(err)

	updated := ContextWithHandle(context.Background())
	err = AddRevisionToContext(updated, &v1.WriteSchemaRequest{}, ds)
	require.NoError(err)
	require.Equal(databaseRev.BigInt(), RevisionFromContext(updated).BigInt())
}

func TestConsistencyTestSuite(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	s := &ConsistencyTestSuite{
		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
			ServerOpts: []grpc.ServerOption{
				grpc.StreamInterceptor(StreamServerInterceptor(ds)),
				grpc.UnaryInterceptor(UnaryServerInterceptor(ds)),
			},
		},
	}
	suite.Run(t, s)
}

var goodPing = &pb_testproto.PingRequest{Value: "something"}

type ConsistencyTestSuite struct {
	*grpc_testing.InterceptorTestSuite
}

func (s *ConsistencyTestSuite) TestValidPasses_Unary() {
	require := require.New(s.T())
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	require.NoError(err)
}

func (s *ConsistencyTestSuite) TestValidPasses_ServerStream() {
	require := require.New(s.T())
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing)
	require.NoError(err)
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(s.T(), err, "no error on messages sent occurred")
	}
}

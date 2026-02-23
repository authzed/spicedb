package pertoken

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// testServer implements the test service for middleware testing
type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (t testServer) PingEmpty(ctx context.Context, _ *testpb.PingEmptyRequest) (*testpb.PingEmptyResponse, error) {
	// Verify that a datastore is available in context
	dl := datalayer.FromContext(ctx)
	if dl == nil {
		return nil, errors.New("no datastore in context")
	}
	return &testpb.PingEmptyResponse{}, nil
}

func (t testServer) Ping(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	// Verify that a datastore is available in context
	dl := datalayer.FromContext(ctx)
	if dl == nil {
		return nil, errors.New("no datastore in context")
	}

	if req.Value == "createrel" {
		// Create a new relationship in the datastore
		_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, tx datalayer.ReadWriteTransaction) error {
			return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Create(tuple.MustParse("example/project:pied_piper#writer@example/user:tom")),
			})
		})
		if err != nil {
			return nil, err
		}
	}

	headRev, err := dl.HeadRevision(ctx)
	if err != nil {
		return nil, err
	}

	reader := dl.SnapshotReader(headRev)
	if reader == nil {
		return nil, errors.New("no snapshot reader available")
	}

	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: "example/project",
	})
	if err != nil {
		return nil, err
	}

	rels, err := datastore.IteratorToSlice(it)
	if err != nil {
		return nil, err
	}

	relCountStr := strconv.Itoa(len(rels))
	return &testpb.PingResponse{Value: relCountStr}, nil
}

func (t testServer) PingError(ctx context.Context, _ *testpb.PingErrorRequest) (*testpb.PingErrorResponse, error) {
	return nil, errors.New("test error")
}

func (t testServer) PingList(_ *testpb.PingListRequest, server testpb.TestService_PingListServer) error {
	// Verify that a datastore is available in context
	dl := datalayer.FromContext(server.Context())
	if dl == nil {
		return errors.New("no datastore in context")
	}
	return server.Send(&testpb.PingListResponse{Value: "ping"})
}

func (t testServer) PingStream(stream testpb.TestService_PingStreamServer) error {
	_, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return stream.Send(&testpb.PingStreamResponse{Value: "pong", Counter: 1})
}

// perTokenMiddlewareTestSuite is a test suite for the pertoken middleware
type perTokenMiddlewareTestSuite struct {
	*testpb.InterceptorTestSuite
	middleware *MiddlewareForTesting
	tempDir    string
}

func TestPerTokenMiddleware(t *testing.T) {
	// Create a test config file
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yaml")
	configContent := `---
schema: >-
  definition example/user {}

  definition example/project {
      relation reader: example/user
      relation writer: example/user
      relation owner: example/user

      permission read = reader + write
      permission write = writer + admin
      permission admin = owner
  }
relationships: >-
  example/project:pied_piper#owner@example/user:milburga

  example/project:pied_piper#reader@example/user:tarben

  example/project:pied_piper#writer@example/user:freyja
assertions:
  assertTrue: []
  assertFalse: []
validation: null
`
	err := os.WriteFile(configFile, []byte(configContent), 0o600)
	require.NoError(t, err)

	// Create middleware
	caveatTypeSet := caveattypes.MustNewStandardTypeSet()
	middleware := NewMiddleware([]string{configFile}, caveatTypeSet.TypeSet)

	s := &perTokenMiddlewareTestSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &testServer{},
			ServerOpts: []grpc.ServerOption{
				grpc.UnaryInterceptor(middleware.UnaryServerInterceptor()),
				grpc.StreamInterceptor(middleware.StreamServerInterceptor()),
			},
			ClientOpts: []grpc.DialOption{},
		},
		middleware: middleware,
		tempDir:    tempDir,
	}
	suite.Run(t, s)
}

func (s *perTokenMiddlewareTestSuite) TestUnaryInterceptor_WithMissingToken() {
	resp, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "test"})
	s.Require().NoError(err)
	s.Require().Equal("3", resp.Value)
}

func (s *perTokenMiddlewareTestSuite) TestUnaryInterceptor_WithEmptyToken() {
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), "authorization", "")
	resp, err := s.Client.Ping(ctx, &testpb.PingRequest{Value: "test"})
	s.Require().NoError(err)
	s.Require().Equal("3", resp.Value)
}

func (s *perTokenMiddlewareTestSuite) TestUnaryInterceptor_WithIncorrectToken() {
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), "authorization", "missingbearer")
	resp, err := s.Client.Ping(ctx, &testpb.PingRequest{Value: "test"})
	s.Require().NoError(err)
	s.Require().Equal("3", resp.Value)
}

func (s *perTokenMiddlewareTestSuite) TestUnaryInterceptor_WithDefinedToken() {
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), "authorization", "bearer sometoken")
	resp, err := s.Client.Ping(ctx, &testpb.PingRequest{Value: "test"})
	s.Require().NoError(err)
	s.Require().Equal("3", resp.Value)
}

func (s *perTokenMiddlewareTestSuite) TestUnaryInterceptor_WithDifferentToken() {
	// Connect with sometoken, creating an additional relationship.
	ctx := metadata.AppendToOutgoingContext(s.SimpleCtx(), "authorization", "bearer sometoken")
	resp, err := s.Client.Ping(ctx, &testpb.PingRequest{Value: "createrel"})
	s.Require().NoError(err)
	s.Require().Equal("4", resp.Value)

	// Connect with a different token, which should not see the new relationship.
	ctx = metadata.AppendToOutgoingContext(s.SimpleCtx(), "authorization", "bearer differenttoken")
	resp, err = s.Client.Ping(ctx, &testpb.PingRequest{Value: "test"})
	s.Require().NoError(err)
	s.Require().Equal("3", resp.Value)
}

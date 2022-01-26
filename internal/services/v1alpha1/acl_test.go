package v1alpha1

import (
	"context"
	"net"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestAttemptWriteRelationshipToPermission(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)
	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(ns, ds)

	s := testfixtures.NewTestServer(ds)
	v1alpha1.RegisterSchemaServiceServer(s, NewSchemaServer(ds, PrefixRequired))
	v0.RegisterACLServiceServer(s, v0svc.NewACLServer(ds, ns, dispatch, 50))
	lis := bufconn.Listen(1024 * 1024)
	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	t.Cleanup(func() {
		s.Stop()
		lis.Close()
	})

	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := v1alpha1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write the schema.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}

		definition example/document {
			relation reader: example/user
			permission read = reader
		}
		`,
	})
	require.NoError(t, err)

	// Write a relationship to the relation.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#reader@example/user:someuser#..."),
		)},
	})
	require.NoError(t, err)

	// Attempt to write a relation to the permission, which should fail.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#read@example/user:someuser#..."),
		)},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

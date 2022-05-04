package v1alpha1_test

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestAttemptWriteRelationshipToPermission(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write the schema.
	_, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
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
		Updates: []*v0.RelationTupleUpdate{core.ToV0RelationTupleUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#reader@example/user:someuser#..."),
		))},
	})
	require.NoError(t, err)

	// Attempt to write a relation to the permission, which should fail.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{core.ToV0RelationTupleUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#read@example/user:someuser#..."),
		))},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

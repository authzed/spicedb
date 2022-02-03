package v1_test

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaWriteNoPrefix(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, 0, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, 0, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `invalid example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, 0, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	_, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	userSchema := `definition example/user {}`

	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: userSchema,
	})
	require.NoError(t, err)

	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, userSchema, readback.SchemaText)
}

func TestSchemaDeleteRelation(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, 0, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation anotherrelation: example/user
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Attempt to delete the `anotherrelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
		}`,
	})
	require.Nil(t, err)

	// Delete the relationship.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)
}

func TestSchemaDeleteDefinition(t *testing.T) {
	conn, cleanup, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, 0, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v0client := v0.NewACLServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `document` type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Delete the relationship.
	_, err = v0client.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to  delete the `document` type, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, `definition example/user {}`, readback.SchemaText)
}

package v1_test

import (
	"context"
	"testing"

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
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
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
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
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
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

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
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
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
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)
}

func TestSchemaDeletePermission(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
			permission someperm = somerelation + anotherrelation
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the `someperm` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.Nil(t, err)
}

func TestSchemaChangeRelationToPermission(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
			permission someperm = somerelation + anotherrelation
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#anotherrelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to change `anotherrelation` into a permission, which should fail since it has data.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			permission anotherrelation = nil
			permission someperm = somerelation + anotherrelation
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#anotherrelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to change `anotherrelation` into a permission, which should now succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			permission anotherrelation = nil
			permission someperm = somerelation + anotherrelation
		}`,
	})
	require.Nil(t, err)
}

func TestSchemaDeleteDefinition(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

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
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the `document` type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
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

func TestSchemaRemoveWildcard(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user:*
		}`,
	})
	require.NoError(t, err)

	// Write the wildcard relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:*"),
		))},
	})
	require.Nil(t, err)

	newSchema := `definition example/document {
	relation somerelation: example/organization#user
}

definition example/organization {
	relation user: example/user
}

definition example/user {}`

	// Attempt to change the wildcard type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.Equal(t, "rpc error: code = InvalidArgument desc = cannot remove allowed wildcard type `example/user:*` from Relation `somerelation` in Object Definition `example/document`, as a Relationship exists with it", err.Error())

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:*"),
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the wildcard type, which should work now.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, newSchema, readback.SchemaText)
}

func TestSchemaEmpty(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

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
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to empty the schema, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: ``,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.UpdateToRelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to empty the schema, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: ``,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	_, err = client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaTypeRedefined(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	// Write a schema that redefines the same type.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

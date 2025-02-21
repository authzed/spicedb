package v1_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaWriteNoPrefix(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	resp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.WrittenAt)
	require.NotEmpty(t, resp.WrittenAt.Token)
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

func TestSchemaWriteInvalidNamespace(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}
		
		definition document {
			relation viewer: user | somemissingdef
		}
	`,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	_, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	userSchema := "caveat someCaveat(somecondition int) {\n\tsomecondition == 42\n}\n\ndefinition example/document {\n\trelation viewer: example/user | example/user with someCaveat\n}\n\ndefinition example/user {}"

	writeResp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: userSchema,
	})
	require.NoError(t, err)
	require.NotNil(t, writeResp.WrittenAt)
	require.NotEmpty(t, writeResp.WrittenAt.Token)

	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, userSchema, readback.SchemaText)
	require.NotNil(t, readback.ReadAt)
	require.NotEmpty(t, readback.ReadAt.Token)
}

func TestSchemaDeleteRelation(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	writeResp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)
	require.NotNil(t, writeResp.WrittenAt)
	require.NotEmpty(t, writeResp.WrittenAt.Token)

	// Write a relationship for one of the relations.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
	updateResp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
		}`,
	})
	require.Nil(t, err)
	require.NotNil(t, updateResp.WrittenAt)
	require.NotEmpty(t, updateResp.WrittenAt.Token)

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should succeed.
	deleteRelResp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)
	require.NotNil(t, deleteRelResp.WrittenAt)
	require.NotEmpty(t, deleteRelResp.WrittenAt.Token)
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
	require.Equal(t, "rpc error: code = InvalidArgument desc = cannot remove allowed type `example/user:*` from relation `somerelation` in object definition `example/document`, as a relationship exists with it", err.Error())

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
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
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		))},
	})
	require.Nil(t, err)

	// Attempt to empty the schema, which should succeed.
	emptyResp, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: ``,
	})
	require.Nil(t, err)
	require.NotNil(t, emptyResp.WrittenAt)
	require.NotEmpty(t, emptyResp.WrittenAt.Token)

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
	spiceerrors.RequireReason(t, v1.ErrorReason_ERROR_REASON_SCHEMA_PARSE_ERROR, err,
		"source_code",
		"start_line_number",
		"start_column_position",
		"end_line_number",
		"end_column_position",
	)
}

func TestSchemaTypeInvalid(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	// Write a schema that references an invalid type.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation viewer: hiya
		}`,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)
	spiceerrors.RequireReason(t, v1.ErrorReason_ERROR_REASON_SCHEMA_TYPE_ERROR, err, "definition_name")
}

func TestSchemaRemoveCaveat(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}

		caveat somecaveat(a int, b int) {
			a + b == 42
		}

		definition document {
			relation somerelation: user with somecaveat
		}`,
	})
	require.NoError(t, err)

	// Write the relationship referencing the caveat.
	caveatCtx, err := structpb.NewStruct(map[string]any{"a": 1, "b": 2})
	require.NoError(t, err)

	toWrite := tuple.MustParse("document:somedoc#somerelation@user:tom")
	toWrite.OptionalCaveat = &core.ContextualizedCaveat{
		CaveatName: "somecaveat",
		Context:    caveatCtx,
	}

	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
			toWrite,
		))},
	})
	require.Nil(t, err)

	newSchema := `definition document {
	relation somerelation: user
}

definition user {}`

	// Attempt to change the relation type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.Equal(t, "rpc error: code = InvalidArgument desc = cannot remove allowed type `user with somecaveat` from relation `somerelation` in object definition `document`, as a relationship exists with it", err.Error())

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
			toWrite,
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the caveated type, which should work now.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, newSchema, readback.SchemaText)
}

func TestSchemaUnchangedNamespaces(t *testing.T) {
	conn, cleanup, ds, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)

	client := v1.NewSchemaServiceClient(conn)

	// Write a schema.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}
	
		definition document {
			relation editor: user
			relation viewer: user
		}`,
	})
	require.NoError(t, err)

	// Update the schema.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition user {}
	
		definition document {
			relation viewer: user
		}`,
	})
	require.NoError(t, err)

	// Ensure the `user` definition was not modified.
	rev, err := ds.HeadRevision(context.Background())
	require.NoError(t, err)

	reader := ds.SnapshotReader(rev)

	_, userRevision, err := reader.ReadNamespaceByName(context.Background(), "user")
	require.NoError(t, err)

	_, docRevision, err := reader.ReadNamespaceByName(context.Background(), "document")
	require.NoError(t, err)

	require.True(t, docRevision.GreaterThan(userRevision))
}

func TestSchemaInvalid(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)

	// Write a schema that references an invalid type.
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `definition org {
			relation admin: user
			relation member: user
		
			permission read = admin + member
			permission create = admin
			permission update = admin
			permission delete = admin
			permission * = read + create + update + delete // <= crash case
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.ErrorContains(t, err, "found token TokenTypeStar")
}

func TestSchemaChangeExpiration(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema with expiration.
	originalSchema := `
		use expiration
		
		definition user {}

		definition document {
			relation somerelation: user with expiration
		}`
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: originalSchema,
	})
	require.NoError(t, err)

	// Write the relationship referencing the expiration.
	toWrite := tuple.MustParse("document:somedoc#somerelation@user:tom[expiration:2300-01-01T00:00:00Z]")
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
			toWrite,
		))},
	})
	require.Nil(t, err)

	newSchema := "definition document {\n\trelation somerelation: user\n}\n\ndefinition user {}"

	// Attempt to change the relation type, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.Equal(t, "rpc error: code = InvalidArgument desc = cannot remove allowed type `user with expiration` from relation `somerelation` in object definition `document`, as a relationship exists with it", err.Error())

	// Delete the relationship.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Delete(
			toWrite,
		))},
	})
	require.Nil(t, err)

	// Attempt to delete the relation type, which should work now.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, newSchema, readback.SchemaText)

	// Add the relationship back without expiration.
	toWriteWithoutExp := tuple.MustParse("document:somedoc#somerelation@user:tom")
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
			toWriteWithoutExp,
		))},
	})
	require.Nil(t, err)

	// Attempt to change the relation type back to including expiration, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: originalSchema,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.Equal(t, "rpc error: code = InvalidArgument desc = cannot remove allowed type `user` from relation `somerelation` in object definition `document`, as a relationship exists with it", err.Error())
}

func TestSchemaChangeExpirationAllowed(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema with expiration.
	originalSchema := `
		use expiration
		
		definition user {}

		definition document {
			relation somerelation: user | user with expiration
		}`
	_, err := client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: originalSchema,
	})
	require.NoError(t, err)

	// Write the relationship without referencing the expiration.
	toWrite := tuple.MustParse("document:somedoc#somerelation@user:tom")
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{tuple.MustUpdateToV1RelationshipUpdate(tuple.Create(
			toWrite,
		))},
	})
	require.Nil(t, err)

	newSchema := "definition document {\n\trelation somerelation: user\n}\n\ndefinition user {}"

	// Attempt to change the schema to remove the expiration, which should work.
	_, err = client.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: newSchema,
	})
	require.Nil(t, err)

	// Ensure it was deleted.
	readback, err := client.ReadSchema(context.Background(), &v1.ReadSchemaRequest{})
	require.NoError(t, err)
	require.Equal(t, newSchema, readback.SchemaText)
}

func TestSchemaDiff(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	schemaClient := v1.NewSchemaServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name             string
		existingSchema   string
		comparisonSchema string
		expectedError    string
		expectedCode     codes.Code
		expectedResponse *v1.DiffSchemaResponse
	}{
		{
			name:             "no changes",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user {}`,
			expectedResponse: &v1.DiffSchemaResponse{},
		},
		{
			name:             "addition from existing schema",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user {} definition document {}`,
			expectedResponse: &v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_DefinitionAdded{
							DefinitionAdded: &v1.ReflectionDefinition{
								Name:    "document",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			name:             "removal from existing schema",
			existingSchema:   `definition user {} definition document {}`,
			comparisonSchema: `definition user {}`,
			expectedResponse: &v1.DiffSchemaResponse{
				Diffs: []*v1.ReflectionSchemaDiff{
					{
						Diff: &v1.ReflectionSchemaDiff_DefinitionRemoved{
							DefinitionRemoved: &v1.ReflectionDefinition{
								Name:    "document",
								Comment: "",
							},
						},
					},
				},
			},
		},
		{
			name:             "invalid comparison schema",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user { invalid`,
			expectedCode:     codes.InvalidArgument,
			expectedError:    "Expected end of statement or definition, found: TokenTypeIdentifier",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Write the existing schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tt.existingSchema,
			})
			require.NoError(t, err)

			actual, err := schemaClient.DiffSchema(context.Background(), &v1.DiffSchemaRequest{
				ComparisonSchema: tt.comparisonSchema,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				grpcutil.RequireStatus(t, tt.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, tt.expectedResponse, actual, "mismatch in response")
			}
		})
	}
}

func TestReflectSchema(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
	schemaClient := v1.NewSchemaServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name             string
		schema           string
		filters          []*v1.ReflectionSchemaFilter
		expectedCode     codes.Code
		expectedError    string
		expectedResponse *v1.ReflectSchemaResponse
	}{
		{
			name:   "simple schema",
			schema: `definition user {}`,
			expectedResponse: &v1.ReflectSchemaResponse{
				Definitions: []*v1.ReflectionDefinition{
					{
						Name:    "user",
						Comment: "",
					},
				},
			},
		},
		{
			name: "schema with comment",
			schema: `// this is a user
definition user {}`,
			expectedResponse: &v1.ReflectSchemaResponse{
				Definitions: []*v1.ReflectionDefinition{
					{
						Name:    "user",
						Comment: "// this is a user",
					},
				},
			},
		},
		{
			name:   "invalid filter",
			schema: `definition user {}`,
			filters: []*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalCaveatNameFilter:     "invalid",
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "cannot filter by both definition and caveat name",
		},
		{
			name:   "another invalid filter",
			schema: `definition user {}`,
			filters: []*v1.ReflectionSchemaFilter{
				{
					OptionalRelationNameFilter: "doc",
				},
			},
			expectedCode:  codes.InvalidArgument,
			expectedError: "relation name match requires definition name match",
		},
		{
			name: "full schema",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				/** somecaveat is a caveat */
				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member | user:*

					// read all the things
					permission read = viewer + editor
				}
			`,
			expectedResponse: &v1.ReflectSchemaResponse{
				Definitions: []*v1.ReflectionDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ReflectionRelation{
							{
								Name:                 "editor",
								Comment:              "// editor is a relation",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ReflectionTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ReflectionTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
									{
										SubjectDefinitionName: "user",
										Typeref: &v1.ReflectionTypeReference_IsPublicWildcard{
											IsPublicWildcard: true,
										},
									},
								},
							},
						},
						Permissions: []*v1.ReflectionPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
					{
						Name:    "group",
						Comment: "/** group represents a group */",
						Relations: []*v1.ReflectionRelation{
							{
								Name:                 "direct_member",
								Comment:              "",
								ParentDefinitionName: "group",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref:               &v1.ReflectionTypeReference_OptionalRelationName{OptionalRelationName: "member"},
									},
								},
							},
							{
								Name:                 "admin",
								Comment:              "",
								ParentDefinitionName: "group",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
								},
							},
						},
						Permissions: []*v1.ReflectionPermission{
							{
								Name:                 "member",
								Comment:              "",
								ParentDefinitionName: "group",
							},
						},
					},
					{
						Name:    "user",
						Comment: "/** user represents a user */",
					},
				},
				Caveats: []*v1.ReflectionCaveat{
					{
						Name:       "somecaveat",
						Comment:    "/** somecaveat is a caveat */",
						Expression: "first == 1 && second == \"two\"",
						Parameters: []*v1.ReflectionCaveatParameter{
							{
								Name:             "first",
								Type:             "int",
								ParentCaveatName: "somecaveat",
							},
							{
								Name:             "second",
								Type:             "string",
								ParentCaveatName: "somecaveat",
							},
						},
					},
				},
			},
		},
		{
			name: "full schema with definition filter",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member

					// read all the things
					permission read = viewer + editor
				}
			`,
			filters: []*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
				},
			},
			expectedResponse: &v1.ReflectSchemaResponse{
				Definitions: []*v1.ReflectionDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ReflectionRelation{
							{
								Name:                 "editor",
								Comment:              "// editor is a relation",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ReflectionTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ReflectionTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
						},
						Permissions: []*v1.ReflectionPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
				},
			},
		},
		{
			name: "full schema with definition, relation and permission filters",
			schema: `
				/** user represents a user */
				definition user {}

				/** group represents a group */
				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				caveat somecaveat(first int, second string) {
					first == 1 && second == "two"
				}

				/** document is a protected document */
				definition document {
					// editor is a relation
					relation editor: user | group#member
					relation viewer: user | user with somecaveat | group#member

					// read all the things
					permission read = viewer + editor
				}
			`,
			filters: []*v1.ReflectionSchemaFilter{
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalRelationNameFilter:   "viewer",
				},
				{
					OptionalDefinitionNameFilter: "doc",
					OptionalPermissionNameFilter: "read",
				},
			},
			expectedResponse: &v1.ReflectSchemaResponse{
				Definitions: []*v1.ReflectionDefinition{
					{
						Name:    "document",
						Comment: "/** document is a protected document */",
						Relations: []*v1.ReflectionRelation{
							{
								Name:                 "viewer",
								Comment:              "",
								ParentDefinitionName: "document",
								SubjectTypes: []*v1.ReflectionTypeReference{
									{
										SubjectDefinitionName: "user",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "user",
										OptionalCaveatName:    "somecaveat",
										Typeref:               &v1.ReflectionTypeReference_IsTerminalSubject{},
									},
									{
										SubjectDefinitionName: "group",
										Typeref: &v1.ReflectionTypeReference_OptionalRelationName{
											OptionalRelationName: "member",
										},
									},
								},
							},
						},
						Permissions: []*v1.ReflectionPermission{
							{
								Name:                 "read",
								Comment:              "// read all the things",
								ParentDefinitionName: "document",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tt.schema,
			})
			require.NoError(t, err)

			actual, err := schemaClient.ReflectSchema(context.Background(), &v1.ReflectSchemaRequest{
				OptionalFilters: tt.filters,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				grpcutil.RequireStatus(t, tt.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, tt.expectedResponse, actual, "mismatch in response")
			}
		})
	}
}

func TestDependentRelations(t *testing.T) {
	tcs := []struct {
		name             string
		schema           string
		definitionName   string
		permissionName   string
		expectedCode     codes.Code
		expectedError    string
		expectedResponse []*v1.ReflectionRelationReference
	}{
		{
			name:           "invalid definition",
			schema:         `definition user {}`,
			definitionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `invalid` not found",
		},
		{
			name:           "invalid permission",
			schema:         `definition user {}`,
			definitionName: "user",
			permissionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "permission `invalid` not found",
		},
		{
			name: "specified relation",
			schema: `
				definition user {}

				definition document {
					relation editor: user
				}
			`,
			definitionName: "document",
			permissionName: "editor",
			expectedCode:   codes.InvalidArgument,
			expectedError:  "is not a permission",
		},
		{
			name: "simple schema",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "editor",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "schema with nested relation",
			schema: `
				definition user {}

				definition group {
					relation direct_member: user | group#member
					relation admin: user
					permission member = direct_member + admin
				}

				definition document {
					relation unused: user
					relation viewer: user | group#member
					permission view = viewer
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "admin",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "direct_member",
					IsPermission:   false,
				},
				{
					DefinitionName: "group",
					RelationName:   "member",
					IsPermission:   true,
				},
			},
		},
		{
			name: "schema with arrow",
			schema: `
				definition user {}

				definition folder {
					relation alsounused: user
					relation viewer: user
					permission view = viewer
				}

				definition document {
					relation unused: user
					relation parent: folder
					relation viewer: user
					permission view = viewer + parent->view
				}
			`,
			definitionName: "document",
			permissionName: "view",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "parent",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
				{
					DefinitionName: "folder",
					RelationName:   "view",
					IsPermission:   true,
				},
				{
					DefinitionName: "folder",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "empty response",
			schema: `
				definition user {}

				definition folder {
					relation alsounused: user
					relation viewer: user
					permission view = viewer
				}

				definition document {
					relation unused: user
					relation parent: folder
					relation viewer: user
					permission view = viewer + parent->view
					permission empty = nil
				}
			`,
			definitionName:   "document",
			permissionName:   "empty",
			expectedResponse: []*v1.ReflectionRelationReference{},
		},
		{
			name: "empty definition",
			schema: `
				definition user {}
			`,
			definitionName: "",
			permissionName: "empty",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `` not found",
		},
		{
			name: "empty permission",
			schema: `
				definition user {}
			`,
			definitionName: "user",
			permissionName: "",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "permission `` not found",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
			schemaClient := v1.NewSchemaServiceClient(conn)
			defer cleanup()

			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tc.schema,
			})
			require.NoError(t, err)

			actual, err := schemaClient.DependentRelations(context.Background(), &v1.DependentRelationsRequest{
				DefinitionName: tc.definitionName,
				PermissionName: tc.permissionName,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				grpcutil.RequireStatus(t, tc.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, &v1.DependentRelationsResponse{
					Relations: tc.expectedResponse,
				}, actual, "mismatch in response")
			}
		})
	}
}

func TestComputablePermissions(t *testing.T) {
	tcs := []struct {
		name             string
		schema           string
		definitionName   string
		relationName     string
		filter           string
		expectedCode     codes.Code
		expectedError    string
		expectedResponse []*v1.ReflectionRelationReference
	}{
		{
			name:           "invalid definition",
			schema:         `definition user {}`,
			definitionName: "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "object definition `invalid` not found",
		},
		{
			name:           "invalid relation",
			schema:         `definition user {}`,
			definitionName: "user",
			relationName:   "invalid",
			expectedCode:   codes.FailedPrecondition,
			expectedError:  "relation/permission `invalid` not found",
		},
		{
			name: "basic",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "user",
			relationName:   "",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "another",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "editor",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "unused",
					IsPermission:   false,
				},
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "filtered",
			schema: `
				definition user {}

				definition folder {
					relation viewer: user
				}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "user",
			relationName:   "",
			filter:         "folder",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "folder",
					RelationName:   "viewer",
					IsPermission:   false,
				},
			},
		},
		{
			name: "basic relation",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission another = unused
				}`,
			definitionName: "document",
			relationName:   "viewer",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
			},
		},
		{
			name: "multiple permissions",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
					permission only_view = viewer
					permission another = unused
				}`,
			definitionName: "document",
			relationName:   "viewer",
			expectedResponse: []*v1.ReflectionRelationReference{
				{
					DefinitionName: "document",
					RelationName:   "only_view",
					IsPermission:   true,
				},
				{
					DefinitionName: "document",
					RelationName:   "view",
					IsPermission:   true,
				},
			},
		},
		{
			name: "empty response",
			schema: `
				definition user {}

				definition document {
					relation unused: user
					permission empty = nil
				}
			`,
			definitionName:   "document",
			relationName:     "unused",
			expectedResponse: []*v1.ReflectionRelationReference{},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.EmptyDatastore)
			schemaClient := v1.NewSchemaServiceClient(conn)
			defer cleanup()

			// Write the schema.
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: tc.schema,
			})
			require.NoError(t, err)

			actual, err := schemaClient.ComputablePermissions(context.Background(), &v1.ComputablePermissionsRequest{
				DefinitionName:               tc.definitionName,
				RelationName:                 tc.relationName,
				OptionalDefinitionNameFilter: tc.filter,
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
			})

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				grpcutil.RequireStatus(t, tc.expectedCode, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, actual.ReadAt)
				actual.ReadAt = nil

				testutil.RequireProtoEqual(t, &v1.ComputablePermissionsResponse{
					Permissions: tc.expectedResponse,
				}, actual, "mismatch in response")
			}
		})
	}
}

package v1alpha1_test

import (
	"context"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaReadNoPrefix(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	_, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteNoPrefix(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	_, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `define user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaWriteNoPrefixNotRequired(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	resp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)

	rev, err := nspkg.DecodeV1Alpha1Revision(resp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 1)
}

func TestSchemaReadInvalidName(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	_, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"誤り"},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	_, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `invalid example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	_, err = client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"example/user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	requestedObjectDefNames := []string{"example/user"}

	_, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	userSchema := `definition example/user {}`

	writeResp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: userSchema,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	rev, err := nspkg.DecodeV1Alpha1Revision(writeResp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 1)

	readback, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: writeResp.GetObjectDefinitionsNames(),
	})
	require.NoError(t, err)
	require.Equal(t, []string{userSchema}, readback.GetObjectDefinitions())
}

func TestSchemaDeleteRelation(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)
	v1client := v1.NewPermissionsServiceClient(conn)

	// Write a basic schema.
	_, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	_, err = v1client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.UpdateToRelationshipUpdate(tuple.Create(
				tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
			)),
		},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should fail.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation anotherrelation: example/user
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Attempt to delete the `anotherrelation` relation, which should succeed.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
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
	writeResp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}

			definition example/document {}`,
	})
	require.Nil(t, err)

	rev, err := nspkg.DecodeV1Alpha1Revision(writeResp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 2)
}

func TestSchemaReadUpdateAndFailWrite(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	requestedObjectDefNames := []string{"example/user"}

	// Issue a write to create the schema's namespaces.
	writeResp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	// Read the schema.
	resp, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)

	// Issue a write with the precondition and ensure it succeeds.
	updateResp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo1: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: resp.ComputedDefinitionsRevision,
	})
	require.NoError(t, err)

	// Issue another write out of band to update the namespace.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo2: example/user
		}`,
	})
	require.NoError(t, err)

	// Try to write using the previous revision and ensure it fails.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo3: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: updateResp.ComputedDefinitionsRevision,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// Read the schema and ensure it did not change.
	readResp, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)
	require.Contains(t, readResp.ObjectDefinitions[0], "foo2")
}

func TestSchemaReadDeleteAndFailWrite(t *testing.T) {
	conn, cleanup, ds, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	requestedObjectDefNames := []string{"example/user"}

	// Issue a write to create the schema's namespaces.
	writeResp, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo1: example/user
		}`,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	// Read the schema.
	resp, err := client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)

	// Issue a delete out of band for the namespace.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		for _, nsName := range requestedObjectDefNames {
			derr := rwt.DeleteNamespace(nsName)
			if derr != nil {
				return derr
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Try to write using the previous revision and ensure it fails.
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo3: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: resp.ComputedDefinitionsRevision,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// Read the schema and ensure it was not written.
	_, err = client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaTypeRedefined(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, false, testfixtures.EmptyDatastore)
	t.Cleanup(cleanup)
	client := v1alpha1.NewSchemaServiceClient(conn)

	// Write a schema that redefines the same type.
	_, err := client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

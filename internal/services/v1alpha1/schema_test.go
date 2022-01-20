package v1alpha1

import (
	"context"
	"net"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
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
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaReadNoPrefix(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)
	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteNoPrefix(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `define user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaWriteNoPrefixNotRequired(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixNotRequired)
	resp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)

	rev, err := nspkg.DecodeV1Alpha1Revision(resp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 1)
}

func TestSchemaReadInvalidName(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := testfixtures.NewTestServer()
	v1alpha1.RegisterSchemaServiceServer(s, NewSchemaServer(ds, PrefixRequired))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := v1alpha1.NewSchemaServiceClient(conn)
	_, err = client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"誤り"},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := testfixtures.NewTestServer()
	v1alpha1.RegisterSchemaServiceServer(s, NewSchemaServer(ds, PrefixRequired))

	go func() {
		if err := s.Serve(lis); err != nil {
			panic("failed to shutdown cleanly: " + err.Error())
		}
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := v1alpha1.NewSchemaServiceClient(conn)
	_, err = client.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `invalid example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	_, err = client.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"example/user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)
	requestedObjectDefNames := []string{"example/user"}

	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	userSchema := `definition example/user {}`

	writeResp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: userSchema,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	rev, err := nspkg.DecodeV1Alpha1Revision(writeResp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 1)

	readback, err := srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: writeResp.GetObjectDefinitionsNames(),
	})
	require.NoError(t, err)
	require.Equal(t, []string{userSchema}, readback.GetObjectDefinitions())
}

func TestSchemaReadUpgradeValid(t *testing.T) {
	_, err := upgrade(t, []*v0.NamespaceDefinition{testfixtures.UserNS})
	require.NoError(t, err)
}

func TestSchemaReadUpgradeInvalid(t *testing.T) {
	_, err := upgrade(t, []*v0.NamespaceDefinition{testfixtures.UserNS, testfixtures.DocumentNS, testfixtures.FolderNS})
	require.NoError(t, err)
}

func upgrade(t *testing.T, nsdefs []*v0.NamespaceDefinition) (*v1alpha1.ReadSchemaResponse, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	schemaSrv := NewSchemaServer(ds, PrefixRequired)
	namespaceSrv := v0svc.NewNamespaceServer(ds)

	_, err = namespaceSrv.WriteConfig(context.Background(), &v0.WriteConfigRequest{
		Configs: nsdefs,
	})
	require.NoError(t, err)

	nsdefNames := make([]string, 0, len(nsdefs))
	for _, nsdef := range nsdefs {
		nsdefNames = append(nsdefNames, nsdef.Name)
	}

	return schemaSrv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: nsdefNames,
	})
}

func TestSchemaDeleteRelation(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)

	// Write a basic schema.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
			relation anotherrelation: example/user
		}`,
	})
	require.NoError(t, err)

	// Write a relationship for one of the relations.
	ns, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, nil)
	require.NoError(t, err)

	dispatch := graph.NewLocalOnlyDispatcher(ns, ds)
	aclSrv := v0svc.NewACLServer(ds, ns, dispatch, 50)

	_, err = aclSrv.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Create(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should fail.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation anotherrelation: example/user
		}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	// Attempt to delete the `anotherrelation` relation, which should succeed.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
	
		definition example/document {
			relation somerelation: example/user
		}`,
	})
	require.Nil(t, err)

	// Delete the relationship.
	_, err = aclSrv.Write(context.Background(), &v0.WriteRequest{
		Updates: []*v0.RelationTupleUpdate{tuple.Delete(
			tuple.MustParse("example/document:somedoc#somerelation@example/user:someuser#..."),
		)},
	})
	require.Nil(t, err)

	// Attempt to delete the `somerelation` relation, which should succeed.
	writeResp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)

	rev, err := nspkg.DecodeV1Alpha1Revision(writeResp.ComputedDefinitionsRevision)
	require.NoError(t, err)
	require.Len(t, rev, 2)
}

func TestSchemaReadUpdateAndFailWrite(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)
	requestedObjectDefNames := []string{"example/user"}

	// Issue a write to create the schema's namespaces.
	writeResp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}`,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	// Read the schema.
	resp, err := srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)

	// Issue a write with the precondition and ensure it succeeds.
	updateResp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo1: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: resp.ComputedDefinitionsRevision,
	})
	require.NoError(t, err)

	// Issue another write out of band to update the namespace.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo2: example/user
		}`,
	})
	require.NoError(t, err)

	// Try to write using the previous revision and ensure it fails.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo3: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: updateResp.ComputedDefinitionsRevision,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// Read the schema and ensure it did not change.
	readResp, err := srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)
	require.Contains(t, readResp.ObjectDefinitions[0], "foo2")
}

func TestSchemaReadDeleteAndFailWrite(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds, PrefixRequired)
	requestedObjectDefNames := []string{"example/user"}

	// Issue a write to create the schema's namespaces.
	writeResp, err := srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo1: example/user
		}`,
	})
	require.NoError(t, err)
	require.Equal(t, requestedObjectDefNames, writeResp.GetObjectDefinitionsNames())

	// Read the schema.
	resp, err := srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	require.NoError(t, err)

	// Issue a delete out of band for the namespace.
	namespaceSrv := v0svc.NewNamespaceServer(ds)
	_, err = namespaceSrv.DeleteConfigs(context.Background(), &v0.DeleteConfigsRequest{
		Namespaces: requestedObjectDefNames,
	})
	require.NoError(t, err)

	// Try to write using the previous revision and ensure it fails.
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {
			relation foo3: example/user
		}`,
		OptionalDefinitionsRevisionPrecondition: resp.ComputedDefinitionsRevision,
	})
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	// Read the schema and ensure it was not written.
	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: requestedObjectDefNames,
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

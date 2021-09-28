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
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/internal/testfixtures"
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
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition user {}`,
	})
	require.NoError(t, err)
}

func TestSchemaReadInvalidName(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	// test relies on middleware
	lis := bufconn.Listen(1024 * 1024)
	s := testfixtures.NewTestServer()
	v1alpha1.RegisterSchemaServiceServer(s, NewSchemaServer(ds, PrefixRequired))

	go s.Serve(lis)
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
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

	go s.Serve(lis)
	defer func() {
		s.Stop()
		lis.Close()
	}()
	conn, err := grpc.Dial("", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithInsecure())
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

	var nsdefNames []string
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
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `definition example/user {}
		
			definition example/document {}`,
	})
	require.Nil(t, err)
}

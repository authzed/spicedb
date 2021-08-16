package v1alpha1

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/grpcutil"
)

func TestSchemaReadNoPrefix(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds)
	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteNoPrefix(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds)
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `define user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaReadInvalidName(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds)
	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"誤り"},
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestSchemaWriteInvalidSchema(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds)
	_, err = srv.WriteSchema(context.Background(), &v1alpha1.WriteSchemaRequest{
		Schema: `define example/user {}`,
	})
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)

	_, err = srv.ReadSchema(context.Background(), &v1alpha1.ReadSchemaRequest{
		ObjectDefinitionsNames: []string{"example/user"},
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestSchemaWriteAndReadBack(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	srv := NewSchemaServer(ds)
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

	schemaSrv := NewSchemaServer(ds)
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

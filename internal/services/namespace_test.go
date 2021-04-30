package services

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func TestNamespace(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	nsm, err := namespace.NewCachingNamespaceManager(ds, 0*time.Second, nil)
	require.NoError(err)

	srv := NewNamespaceServer(ds, nsm)

	_, err = srv.ReadConfig(context.Background(), &api.ReadConfigRequest{
		Namespace: testfixtures.DocumentNS.Name,
	})
	requireGRPCStatus(codes.NotFound, err, require)

	_, err = srv.WriteConfig(context.Background(), &api.WriteConfigRequest{
		Configs: []*api.NamespaceDefinition{testfixtures.UserNS, testfixtures.FolderNS, testfixtures.DocumentNS},
	})
	require.NoError(err)

	readBack, err := srv.ReadConfig(context.Background(), &api.ReadConfigRequest{
		Namespace: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	require.Equal(testfixtures.DocumentNS.Name, readBack.Namespace)

	if diff := cmp.Diff(testfixtures.DocumentNS, readBack.Config, protocmp.Transform()); diff != "" {
		require.Fail("should have read back the same config")
	}

	_, err = srv.ReadConfig(context.Background(), &api.ReadConfigRequest{
		Namespace: "fake",
	})
	requireGRPCStatus(codes.NotFound, err, require)
}

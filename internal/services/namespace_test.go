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
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
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

func TestNamespaceChanged(t *testing.T) {
	testCases := []struct {
		name             string
		initialNamespace *api.NamespaceDefinition
		tuples           []*api.RelationTuple
		updatedNamespace *api.NamespaceDefinition
		expectedError    string
	}{
		{
			"relation without tuples",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{},
			ns.Namespace(
				"folder",
			),
			"",
		},
		{
			"relation with tuples",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{tuple.Scan("folder:somefolder#viewer@user:someuser#...")},
			ns.Namespace(
				"folder",
			),
			"cannot delete relation `viewer` in namespace `folder`, as a tuple exists under it",
		},
		{
			"relation referenced by tuples",
			ns.Namespace(
				"folder",
				ns.Relation("anotherrel",
					nil,
					ns.RelationReference("folder", "..."),
				),
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{tuple.Scan("folder:somefolder#anotherrel@folder:somefolder#viewer")},
			ns.Namespace(
				"folder",
				ns.Relation("anotherrel",
					nil,
					ns.RelationReference("folder", "..."),
				),
			),
			"cannot delete relation `viewer` in namespace `folder`, as a tuple references it",
		},
		{
			"direct type removed",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{tuple.Scan("folder:somefolder#viewer@user:someuser#...")},
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("folder", "..."),
				),
			),
			"cannot remove allowed direct relation `user#...` from relation `viewer` in namespace `folder`, as a tuple exists with it",
		},
		{
			"direct type removed with no references",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{},
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("folder", "..."),
				),
			),
			"",
		},
		{
			"direct type added",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]*api.RelationTuple{tuple.Scan("folder:somefolder#viewer@user:someuser#...")},
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
					ns.RelationReference("folder", "..."),
				),
			),
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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
				Configs: []*api.NamespaceDefinition{testfixtures.UserNS, tc.initialNamespace},
			})
			require.NoError(err)

			// Write a tuple into the relation.
			updates := []*api.RelationTupleUpdate{}
			for _, tpl := range tc.tuples {
				updates = append(updates, tuple.Create(tpl))
			}

			_, err = ds.WriteTuples(context.Background(), nil, updates)
			require.NoError(err)

			_, err = srv.WriteConfig(context.Background(), &api.WriteConfigRequest{
				Configs: []*api.NamespaceDefinition{tc.updatedNamespace},
			})

			if tc.expectedError != "" {
				require.Error(err)
				requireGRPCStatus(codes.InvalidArgument, err, require)

				require.Contains(err.Error(), tc.expectedError)
			} else {
				require.Nil(err)
			}
		})
	}
}

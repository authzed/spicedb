package v0

import (
	"context"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestNamespace(t *testing.T) {
	require := require.New(t)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)
	defer ds.Close()

	srv := NewNamespaceServer(ds)

	_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
		Namespace: testfixtures.DocumentNS.Name,
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)

	_, err = srv.WriteConfig(context.Background(), &v0.WriteConfigRequest{
		Configs: []*v0.NamespaceDefinition{testfixtures.UserNS, testfixtures.FolderNS, testfixtures.DocumentNS},
	})
	require.NoError(err)

	readBack, err := srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
		Namespace: testfixtures.DocumentNS.Name,
	})
	require.NoError(err)
	require.Equal(testfixtures.DocumentNS.Name, readBack.Namespace)

	if diff := cmp.Diff(testfixtures.DocumentNS, readBack.Config, protocmp.Transform()); diff != "" {
		require.Fail("should have read back the same config")
	}

	_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
		Namespace: "fake",
	})
	grpcutil.RequireStatus(t, codes.NotFound, err)
}

func TestNamespaceChanged(t *testing.T) {
	testCases := []struct {
		name             string
		initialNamespace *v0.NamespaceDefinition
		tuples           []*v0.RelationTuple
		updatedNamespace *v0.NamespaceDefinition
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
			[]*v0.RelationTuple{},
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
			[]*v0.RelationTuple{tuple.MustParse("folder:somefolder#viewer@user:someuser#...")},
			ns.Namespace(
				"folder",
			),
			"cannot delete relation `viewer` in definition `folder`, as a relationship exists under it",
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
			[]*v0.RelationTuple{tuple.MustParse("folder:somefolder#anotherrel@folder:somefolder#viewer")},
			ns.Namespace(
				"folder",
				ns.Relation("anotherrel",
					nil,
					ns.RelationReference("folder", "..."),
				),
			),
			"cannot delete relation `viewer` in definition `folder`, as a relationship references it",
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
			[]*v0.RelationTuple{tuple.MustParse("folder:somefolder#viewer@user:someuser#...")},
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("folder", "..."),
				),
			),
			"cannot remove allowed relation/permission `user#...` from relation `viewer` in definition `folder`, as a relationship exists with it",
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
			[]*v0.RelationTuple{},
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
			[]*v0.RelationTuple{tuple.MustParse("folder:somefolder#viewer@user:someuser#...")},
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
			defer ds.Close()

			srv := NewNamespaceServer(ds)

			_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
				Namespace: testfixtures.DocumentNS.Name,
			})
			grpcutil.RequireStatus(t, codes.NotFound, err)

			_, err = srv.WriteConfig(context.Background(), &v0.WriteConfigRequest{
				Configs: []*v0.NamespaceDefinition{testfixtures.UserNS, tc.initialNamespace},
			})
			require.NoError(err)

			// Write a tuple into the relation.
			updates := make([]*v1.RelationshipUpdate, 0, len(tc.tuples))
			for _, tpl := range tc.tuples {
				updates = append(updates, &v1.RelationshipUpdate{
					Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
					Relationship: tuple.MustToRelationship(tpl),
				})
			}

			_, err = ds.WriteTuples(context.Background(), nil, updates)
			require.NoError(err)

			_, err = srv.WriteConfig(context.Background(), &v0.WriteConfigRequest{
				Configs: []*v0.NamespaceDefinition{tc.updatedNamespace},
			})

			if tc.expectedError != "" {
				require.Error(err)
				grpcutil.RequireStatus(t, codes.InvalidArgument, err)

				require.Contains(err.Error(), tc.expectedError)
			} else {
				require.Nil(err)
			}
		})
	}
}

func TestDeleteNamespace(t *testing.T) {
	testCases := []struct {
		name               string
		initialNamespace   *v0.NamespaceDefinition
		namespacesToDelete []string
		tuples             []*v0.RelationTuple
		expectedError      string
	}{
		{
			"namespace without relationships",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]string{"folder", "user"},
			[]*v0.RelationTuple{},
			"",
		},
		{
			"namespace with relationships",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]string{"folder"},
			[]*v0.RelationTuple{
				tuple.MustParse("folder:somefolder#viewer@user:someuser#..."),
			},
			"cannot delete definition `folder`, as a relationship exists under it",
		},
		{
			"namespace with subject relationships",
			ns.Namespace(
				"folder",
				ns.Relation("viewer",
					nil,
					ns.RelationReference("user", "..."),
				),
			),
			[]string{"user"},
			[]*v0.RelationTuple{
				tuple.MustParse("folder:somefolder#viewer@user:someuser#..."),
			},
			"cannot delete definition `user`, as a relationship references it",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)
			defer ds.Close()

			srv := NewNamespaceServer(ds)

			_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
				Namespace: testfixtures.DocumentNS.Name,
			})
			grpcutil.RequireStatus(t, codes.NotFound, err)

			_, err = srv.WriteConfig(context.Background(), &v0.WriteConfigRequest{
				Configs: []*v0.NamespaceDefinition{testfixtures.UserNS, tc.initialNamespace},
			})
			require.NoError(err)

			// Write the relationships.
			updates := []*v1.RelationshipUpdate{}
			for _, tpl := range tc.tuples {
				updates = append(updates, tuple.UpdateToRelationshipUpdate(tuple.Create(tpl)))
			}

			_, err = ds.WriteTuples(context.Background(), nil, updates)
			require.NoError(err)

			_, err = srv.DeleteConfigs(context.Background(), &v0.DeleteConfigsRequest{
				Namespaces: tc.namespacesToDelete,
			})

			if tc.expectedError != "" {
				require.Error(err)
				grpcutil.RequireStatus(t, codes.InvalidArgument, err)

				require.Contains(err.Error(), tc.expectedError)

				for _, nsName := range tc.namespacesToDelete {
					_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
						Namespace: nsName,
					})
					require.NoError(err)
				}
			} else {
				require.Nil(err)

				for _, nsName := range tc.namespacesToDelete {
					_, err = srv.ReadConfig(context.Background(), &v0.ReadConfigRequest{
						Namespace: nsName,
					})
					grpcutil.RequireStatus(t, codes.NotFound, err)
				}
			}
		})
	}
}

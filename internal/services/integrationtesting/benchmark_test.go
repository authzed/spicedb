//go:build !skipintegrationtests

package integrationtesting_test

import (
	"context"
	"embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

//go:embed benchconfigs/*.yaml testconfigs/*.yaml
var testFiles embed.FS

func BenchmarkServices(b *testing.B) {
	enginesToBenchmark := []string{
		crdb.Engine,
		postgres.Engine,
		"memory",
		// spanner is a simulator so not useful
	}

	bts := []struct {
		title    string
		fileName string
		runner   func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error
	}{
		{
			"basic lookup of view for a user",
			"testconfigs/basicrbac.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(ctx, tuple.RelationReference{
					ObjectType: "example/document",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "example/user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.NotEmpty(b, results)
				return err
			},
		},
		{
			"recursively through groups",
			"testconfigs/simplerecursive.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(ctx, tuple.RelationReference{
					ObjectType: "srrr/resource",
					Relation:   "viewer",
				}, tuple.ObjectAndRelation{
					ObjectType: "srrr/user",
					ObjectID:   "someguy",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.NotEmpty(b, results)
				return err
			},
		},
		{
			"recursively through wide groups",
			"benchconfigs/widegroups.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(ctx, tuple.RelationReference{
					ObjectType: "resource",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.NotEmpty(b, results)
				return err
			},
		},
		{
			"lookup with intersection",
			"benchconfigs/lookupintersection.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(ctx, tuple.RelationReference{
					ObjectType: "resource",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.Len(b, results, 499)
				return err
			},
		},
		{
			"basic check for a user",
			"testconfigs/basicrbac.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(ctx, tuple.ObjectAndRelation{
					ObjectType: "example/document",
					ObjectID:   "firstdoc",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "example/user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
		{
			"recursive check for a user",
			"testconfigs/quay.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(ctx, tuple.ObjectAndRelation{
					ObjectType: "repo",
					ObjectID:   "buynlarge/orgrepo",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "cto",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
		{
			"wide groups check for a user",
			"benchconfigs/checkwidegroups.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(ctx, tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "someresource",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
		{
			"wide direct relation check",
			"benchconfigs/checkwidedirect.yaml",
			func(ctx context.Context, b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(ctx, tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "someresource",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
	}

	for _, bt := range bts {
		b.Run(bt.title, func(b *testing.B) {
			for _, engineID := range enginesToBenchmark {
				b.Run(engineID, func(b *testing.B) {
					b.StopTimer()

					brequire := require.New(b)

					rde := testdatastore.RunDatastoreEngine(b, engineID)
					ds := rde.NewDatastore(b, config.DatastoreConfigInitFunc(b,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10),
						dsconfig.WithMaxRetries(50),
						dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
					))

					contents, err := testFiles.ReadFile(bt.fileName)
					require.NoError(b, err)

					_, revision, err := validationfile.PopulateFromFilesContents(context.Background(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, map[string][]byte{
						"testfile": contents,
					})
					brequire.NoError(err)

					conn, cleanup := testserver.TestClusterWithDispatchAndCacheConfig(b, 1, ds)
					b.Cleanup(cleanup)

					dsCtx := datalayer.ContextWithHandle(context.Background())
					brequire.NoError(datalayer.SetInContext(dsCtx, datalayer.NewDataLayer(ds)))

					testers := consistencytestutil.ServiceTesters(conn[0])

					b.StartTimer()

					for _, tester := range testers {
						b.Run(tester.Name(), func(b *testing.B) {
							require := require.New(b)
							for n := 0; n < b.N; n++ {
								require.NoError(bt.runner(dsCtx, b, tester, revision))
							}
						})
					}
				})
			}
		})
	}
}

package services_test

import (
	"context"
	"embed"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

//go:embed benchconfigs/*.yaml testconfigs/*.yaml
var testFiles embed.FS

func BenchmarkServices(b *testing.B) {
	bts := []struct {
		title    string
		fileName string
		runner   func(ctx context.Context, b *testing.B, tester serviceTester, revision decimal.Decimal) error
	}{
		{
			"basic lookup of view for a user",
			"testconfigs/basicrbac.yaml",
			func(ctx context.Context, b *testing.B, tester serviceTester, revision decimal.Decimal) error {
				results, err := tester.Lookup(ctx, &core.RelationReference{
					Namespace: "example/document",
					Relation:  "view",
				}, &core.ObjectAndRelation{
					Namespace: "example/user",
					ObjectId:  "tom",
					Relation:  tuple.Ellipsis,
				}, revision)
				require.GreaterOrEqual(b, len(results), 0)
				return err
			},
		},
		{
			"recursively through groups",
			"testconfigs/simplerecursive.yaml",
			func(ctx context.Context, b *testing.B, tester serviceTester, revision decimal.Decimal) error {
				results, err := tester.Lookup(ctx, &core.RelationReference{
					Namespace: "srrr/resource",
					Relation:  "viewer",
				}, &core.ObjectAndRelation{
					Namespace: "srrr/user",
					ObjectId:  "someguy",
					Relation:  tuple.Ellipsis,
				}, revision)
				require.GreaterOrEqual(b, len(results), 0)
				return err
			},
		},
		{
			"recursively through wide groups",
			"benchconfigs/widegroups.yaml",
			func(ctx context.Context, b *testing.B, tester serviceTester, revision decimal.Decimal) error {
				results, err := tester.Lookup(ctx, &core.RelationReference{
					Namespace: "resource",
					Relation:  "view",
				}, &core.ObjectAndRelation{
					Namespace: "user",
					ObjectId:  "tom",
					Relation:  tuple.Ellipsis,
				}, revision)
				require.GreaterOrEqual(b, len(results), 0)
				return err
			},
		},
	}

	for _, bt := range bts {
		b.Run(bt.title, func(b *testing.B) {
			for _, engineID := range datastore.Engines {
				b.Run(engineID, func(b *testing.B) {
					brequire := require.New(b)

					rde := testdatastore.RunDatastoreEngine(b, engineID)
					ds := rde.NewDatastore(b, config.DatastoreConfigInitFunc(b,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10)))

					contents, err := testFiles.ReadFile(bt.fileName)
					require.NoError(b, err)

					_, revision, err := validationfile.PopulateFromFilesContents(ds, map[string][]byte{
						"testfile": contents,
					})
					brequire.NoError(err)

					conn, cleanup := testserver.TestClusterWithDispatchAndCacheConfig(b, 1, ds, false /* no cache */)
					b.Cleanup(cleanup)

					dsCtx := datastoremw.ContextWithHandle(context.Background())
					brequire.NoError(datastoremw.SetInContext(dsCtx, ds))

					testers := []serviceTester{
						v1ServiceTester{v1.NewPermissionsServiceClient(conn[0])},
					}

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

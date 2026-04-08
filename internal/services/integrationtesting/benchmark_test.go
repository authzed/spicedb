//go:build !skipintegrationtests

package integrationtesting_test

import (
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
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

//go:embed testconfigs/*.yaml
var testFiles embed.FS

var enginesToBenchmark = []string{
	crdb.Engine,
	postgres.Engine,
	"memory",
	// spanner is a simulator so not useful
	// mysql is not that popular
}

type benchmarkTest struct {
	title string

	// Either fileName (for YAML-based) or benchmarkName (for registry-based) is set.
	fileName      string
	benchmarkName string

	runner func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error
}

func BenchmarkCheck(b *testing.B) {
	bts := []benchmarkTest{
		{
			title:    "basic check for a user",
			fileName: "testconfigs/basicrbac.yaml",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
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
			title:    "recursive check for a user",
			fileName: "testconfigs/quay.yaml",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
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
			title:         "wide groups check for a user",
			benchmarkName: "CheckWideGroups",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
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
			title:         "wide direct relation check",
			benchmarkName: "CheckWideDirect",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
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
			title:         "deep arrow check",
			benchmarkName: "DeepArrow",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "document",
					ObjectID:   "target",
					Relation:   "viewer",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "slow",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
		{
			title:         "wide arrow check",
			benchmarkName: "WideArrow",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "file",
					ObjectID:   "file0",
					Relation:   "viewer",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "user15",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
		{
			title:         "double wide arrow check",
			benchmarkName: "DoubleWideArrow",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "file",
					ObjectID:   "file0",
					Relation:   "viewer",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "user181",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
				return err
			},
		},
	}

	runBenchmarks(b, bts)
}

func BenchmarkLookupResources(b *testing.B) {
	bts := []benchmarkTest{
		{
			title:    "basic lookup of view for a user",
			fileName: "testconfigs/basicrbac.yaml",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(b.Context(), tuple.RelationReference{
					ObjectType: "example/document",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "example/user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.Len(b, results, 2)
				return err
			},
		},
		{
			title:    "recursively through groups",
			fileName: "testconfigs/simplerecursive.yaml",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(b.Context(), tuple.RelationReference{
					ObjectType: "srrr/resource",
					Relation:   "viewer",
				}, tuple.ObjectAndRelation{
					ObjectType: "srrr/user",
					ObjectID:   "someguy",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.Len(b, results, 1)
				return err
			},
		},
		{
			title:         "recursively through wide groups",
			benchmarkName: "WideGroups",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(b.Context(), tuple.RelationReference{
					ObjectType: "resource",
					Relation:   "view",
				}, tuple.ObjectAndRelation{
					ObjectType: "user",
					ObjectID:   "tom",
					Relation:   tuple.Ellipsis,
				}, revision, nil, 0, nil)
				require.Len(b, results, 2)
				return err
			},
		},
		{
			title:         "lookup with intersection",
			benchmarkName: "LookupIntersection",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, _, err := tester.LookupResources(b.Context(), tuple.RelationReference{
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
	}

	runBenchmarks(b, bts)
}

func BenchmarkLookupSubjects(b *testing.B) {
	bts := []benchmarkTest{
		{
			title:         "wide groups subjects",
			benchmarkName: "WideGroups",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, err := tester.LookupSubjects(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "someresource",
					Relation:   "view",
				}, tuple.RelationReference{
					ObjectType: "user",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Len(b, results, 1)
				return err
			},
		},
		{
			title:         "intersection subjects",
			benchmarkName: "LookupIntersection",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, err := tester.LookupSubjects(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "resource1",
					Relation:   "view",
				}, tuple.RelationReference{
					ObjectType: "user",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Len(b, results, 1)
				return err
			},
		},
		{
			title:         "wide groups check subjects",
			benchmarkName: "CheckWideGroups",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, err := tester.LookupSubjects(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "someresource",
					Relation:   "view",
				}, tuple.RelationReference{
					ObjectType: "user",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Len(b, results, 1)
				return err
			},
		},
		{
			title:         "same types subjects",
			benchmarkName: "LookupSameTypes",
			runner: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
				results, err := tester.LookupSubjects(b.Context(), tuple.ObjectAndRelation{
					ObjectType: "resource",
					ObjectID:   "promserver",
					Relation:   "view",
				}, tuple.RelationReference{
					ObjectType: "user",
					Relation:   tuple.Ellipsis,
				}, revision, nil)
				require.Len(b, results, 6)
				return err
			},
		},
	}

	runBenchmarks(b, bts)
}

func runBenchmarks(b *testing.B, bts []benchmarkTest) {
	for _, bt := range bts {
		b.Run(bt.title, func(b *testing.B) {
			for _, engineID := range enginesToBenchmark {
				b.Run(engineID, func(b *testing.B) {
					b.StopTimer()

					rde := testdatastore.RunDatastoreEngine(b, engineID)
					ds := rde.NewDatastore(b, config.DatastoreConfigInitFunc(b,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10),
						dsconfig.WithMaxRetries(50),
						dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
					))

					b.Cleanup(func() {
						ds.Close()
					})

					var revision datastore.Revision
					if bt.fileName != "" {
						contents, err := testFiles.ReadFile(bt.fileName)
						require.NoError(b, err)

						_, rev, err := validationfile.PopulateFromFilesContents(b.Context(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, map[string][]byte{
							"testfile": contents,
						})
						require.NoError(b, err)
						revision = rev
					} else {
						benchmark, ok := bm.Get(bt.benchmarkName)
						require.True(b, ok, "benchmark %q not found in registry", bt.benchmarkName)

						_, err := benchmark.Setup(b.Context(), ds)
						require.NoError(b, err)

						rev, err := ds.HeadRevision(b.Context())
						require.NoError(b, err)
						revision = rev
					}

					conn, cleanup := testserver.TestClusterWithDispatch(b, 1, ds)
					b.Cleanup(cleanup)

					tester := consistencytestutil.NewServiceTester(conn[0])

					b.StartTimer()

					b.Run(tester.Name(), func(b *testing.B) {
						for n := 0; n < b.N; n++ {
							require.NoError(b, bt.runner(b, tester, revision))
						}
					})
				})
			}
		})
	}
}

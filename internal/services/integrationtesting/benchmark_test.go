//go:build integration

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
	"github.com/authzed/spicedb/pkg/cmd/server"
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

// benchmarkRunner executes a single benchmark operation against a service tester.
type benchmarkRunner func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error

// benchmarkScenario defines a test data set and the operations to benchmark against it.
type benchmarkScenario struct {
	name string

	// Either fileName (for YAML-based) or benchmarkName (for registry-based) is set.
	fileName      string
	benchmarkName string

	check           benchmarkRunner
	lookupResources benchmarkRunner
	lookupSubjects  benchmarkRunner
}

// allScenarios consolidates every scenario previously spread across BenchmarkCheck,
// BenchmarkLookupResources, and BenchmarkLookupSubjects. Each scenario carries
// whichever operations it supports.
var allScenarios = []benchmarkScenario{
	{
		name:     "basicrbac",
		fileName: "testconfigs/basicrbac.yaml",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		lookupResources: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:     "quay",
		fileName: "testconfigs/quay.yaml",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:     "simplerecursive",
		fileName: "testconfigs/simplerecursive.yaml",
		lookupResources: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "CheckWideGroups",
		benchmarkName: "CheckWideGroups",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		lookupSubjects: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "CheckWideDirect",
		benchmarkName: "CheckWideDirect",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "DeepArrow",
		benchmarkName: "DeepArrow",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "WideArrow",
		benchmarkName: "WideArrow",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "DoubleWideArrow",
		benchmarkName: "DoubleWideArrow",
		check: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
			result, err := tester.Check(b.Context(), tuple.ObjectAndRelation{
				ObjectType: "file",
				ObjectID:   "file0",
				Relation:   "viewer",
			}, tuple.ObjectAndRelation{
				ObjectType: "user",
				ObjectID:   "user214",
				Relation:   tuple.Ellipsis,
			}, revision, nil)
			require.Equal(b, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, result)
			return err
		},
	},
	{
		name:          "WideGroups",
		benchmarkName: "WideGroups",
		lookupResources: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		lookupSubjects: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "LookupIntersection",
		benchmarkName: "LookupIntersection",
		lookupResources: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		lookupSubjects: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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
		name:          "LookupSameTypes",
		benchmarkName: "LookupSameTypes",
		lookupSubjects: func(b *testing.B, tester consistencytestutil.ServiceTester, revision datastore.Revision) error {
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

// BenchmarkServices benchmarks Check, LookupResources, and LookupSubjects
// through the full gRPC service stack for each datastore engine. Each
// operation is tested with both the current dispatch path and the
// experimental query planner, giving an apples-to-apples comparison.
//
// Hierarchy: engine → scenario (test data) → operation → {dispatch, queryplan}
func BenchmarkServices(b *testing.B) {
	for _, engineID := range enginesToBenchmark {
		if testing.Short() {
			if engineID != "memory" {
				continue
			}
		}
		b.Run(engineID, func(b *testing.B) {
			for _, scenario := range allScenarios {
				b.Run(scenario.name, func(b *testing.B) {
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
					if scenario.fileName != "" {
						contents, err := testFiles.ReadFile(scenario.fileName)
						require.NoError(b, err)

						_, rev, err := validationfile.PopulateFromFilesContents(b.Context(), datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, map[string][]byte{
							"testfile": contents,
						})
						require.NoError(b, err)
						revision = rev
					} else {
						benchmark, ok := bm.Get(scenario.benchmarkName)
						require.True(b, ok, "benchmark %q not found in registry", scenario.benchmarkName)

						_, err := benchmark.Setup(b.Context(), ds)
						require.NoError(b, err)

						rev, err := ds.HeadRevision(b.Context())
						require.NoError(b, err)
						revision = rev.Revision
					}

					// Set up dispatch server (default path).
					dispatchConn, dispatchCleanup := testserver.TestClusterWithDispatch(b, 1, ds)
					b.Cleanup(dispatchCleanup)
					dispatchTester := consistencytestutil.NewServiceTester(dispatchConn[0])

					// Set up query plan server.
					qpConn, qpCleanup := testserver.TestClusterWithDispatch(b, 1, ds,
						server.WithExperimentalQueryPlan("check"),
						server.WithExperimentalQueryPlan("lr"),
						server.WithExperimentalQueryPlan("ls"),
					)
					b.Cleanup(qpCleanup)
					qpTester := consistencytestutil.NewServiceTester(qpConn[0])

					// Warm up the query planner so the server-side CountAdvisor
					// has statistics to optimize subsequent iterations.
					if scenario.check != nil {
						_ = scenario.check(b, qpTester, revision)
					}
					if scenario.lookupResources != nil {
						_ = scenario.lookupResources(b, qpTester, revision)
					}
					if scenario.lookupSubjects != nil {
						_ = scenario.lookupSubjects(b, qpTester, revision)
					}

					b.StartTimer()

					if scenario.check != nil {
						b.Run("Check", func(b *testing.B) {
							b.Run("dispatch", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.check(b, dispatchTester, revision))
								}
							})
							b.Run("queryplan", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.check(b, qpTester, revision))
								}
							})
						})
					}

					if scenario.lookupResources != nil {
						b.Run("LookupResources", func(b *testing.B) {
							b.Run("dispatch", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.lookupResources(b, dispatchTester, revision))
								}
							})
							b.Run("queryplan", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.lookupResources(b, qpTester, revision))
								}
							})
						})
					}

					if scenario.lookupSubjects != nil {
						b.Run("LookupSubjects", func(b *testing.B) {
							b.Run("dispatch", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.lookupSubjects(b, dispatchTester, revision))
								}
							})
							b.Run("queryplan", func(b *testing.B) {
								for n := 0; n < b.N; n++ {
									require.NoError(b, scenario.lookupSubjects(b, qpTester, revision))
								}
							})
						})
					}
				})
			}
		})
	}
}

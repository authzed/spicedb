// Package test provides the system-wide consistency test suite: it reads the
// validation files in the testconfigs directory and executes the full set of APIs
// against the data within, ensuring that all results of the various APIs are
// consistent with one another.
//
// This package lives outside of pkg/datastore/test on purpose. The consistency
// harness needs to stand up a full SpiceDB cluster (via internal/testserver and
// pkg/cmd/server), both of which transitively import every datastore engine. The
// per-engine datastore tests live in internal test packages (package crdb, etc.)
// that import pkg/datastore/test, so placing this harness there would create an
// import cycle (engine [test] -> pkg/datastore/test -> server -> engine). No engine
// package imports pkg/consistency/test, so it is free to depend on the full stack.
package test

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy/indexcheck"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	"github.com/authzed/spicedb/pkg/caveats/types"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dstest "github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/development"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// veryLargeGCWindow is a very large time duration which, when passed to a datastore
// constructor, effectively disables garbage collection.
const (
	veryLargeGCWindow   = 90000 * time.Second
	veryLargeGCInterval = 90000 * time.Second
)

// AllConsistency runs the full system-wide consistency suite against the datastore
// produced by the given tester. It is the consistency analog of test.All and lets any
// DatastoreTester be exercised by the consistency suite.
func AllConsistency(t *testing.T, tester dstest.DatastoreTester) {
	ConsistencyForEngine(t, "", tester)
}

// ConsistencyForEngine runs the system-wide consistency suite, reading in the various
// validation files in the testconfigs directory and executing the full set of APIs
// against the data within, ensuring that all results of the various APIs are consistent
// with one another. It runs the suite against the local and caching dispatchers and
// multiple dispatch chunk sizes.
//
// The datastore under test is obtained in one of two ways:
//   - If tester is non-nil, it is used to create a fresh datastore for each test file.
//     This supports running the consistency suite against any DatastoreTester.
//   - Otherwise, a single Docker instance is spun up for engineID (one of the supported
//     engines: postgres, mysql, crdb, spanner) and reused across all test files.
//
// This acts as essentially a full integration test for the API, dispatching, caching,
// computation and datastore layers.
func ConsistencyForEngine(t *testing.T, engineID string, tester dstest.DatastoreTester) {
	consistencyTestFiles, err := consistencytestutil.ListTestConfigs()
	require.NoError(t, err)

	// newDatastore creates a fresh datastore for a single consistency test file.
	var newDatastore func(t *testing.T) datastore.Datastore
	if tester != nil {
		newDatastore = func(t *testing.T) datastore.Datastore {
			ds, err := tester.New(t, 10, veryLargeGCInterval, veryLargeGCWindow, 0)
			require.NoError(t, err)
			return ds
		}
	} else {
		// One Docker instance per engine, reused across all test files.
		rde := testdatastore.RunDatastoreEngine(t, engineID)
		newDatastore = func(t *testing.T) datastore.Datastore {
			return rde.NewDatastore(t, config.DatastoreConfigInitFunc(t,
				dsconfig.WithWatchBufferLength(0),
				dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
				dsconfig.WithRevisionQuantization(10),
				dsconfig.WithMaxRetries(50),
				dsconfig.WithWriteAcquisitionTimeout(5*time.Second)))
		}
	}

	for _, filePath := range consistencyTestFiles {
		t.Run(path.Base(filePath), func(t *testing.T) {
			baseds := newDatastore(t)
			ds := indexcheck.WrapWithIndexCheckingDatastoreProxyIfApplicable(baseds)

			// Write the schema and relationships.
			dl := datalayer.NewDataLayer(ds)
			populated, _, err := validationfile.PopulateFromFiles(t.Context(), dl, types.Default.TypeSet, []string{filePath})
			require.NoError(t, err)

			dsCtx := datalayer.ContextWithHandle(t.Context())
			require.NoError(t, datalayer.SetInContext(dsCtx, dl))

			headRevisionResult, _, err := dl.HeadRevision(t.Context())
			require.NoError(t, err)
			headRevision := headRevisionResult

			// Validate the type system for each namespace.
			ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(headRevision)))

			for _, nsDef := range populated.NamespaceDefinitions {
				_, err := ts.GetValidatedDefinition(t.Context(), nsDef.Name)
				require.NoError(t, err)

				_, err = development.AddDefinitionWarnings(t.Context(), nsDef, ts)
				// We would normally check to see if there were any warnings; however, some of the integration tests are
				// here to intentionally test things that are warnings-but-not-errors in a schema (like arrow-references-relation).
				// So instead, just validate that warning validation succeeds.
				require.NoError(t, err)
			}

			accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, dsCtx, populated, ds)

			for _, chunkSize := range []uint16{5, 10} {
				t.Run(fmt.Sprintf("chunk-size-%d", chunkSize), func(t *testing.T) {
					t.Parallel()

					options := []server.ConfigOption{
						server.WithDispatchChunkSize(chunkSize),
						server.WithEnableExperimentalLookupResources(true),
						server.WithExperimentalLookupResourcesVersion("lr3"),
					}

					connections := testserver.TestClusterWithDispatch(t, 1, ds, options...)

					cad := consistencytestutil.ConsistencyClusterAndData{
						Conn:      connections[0],
						DataStore: ds,
						Ctx:       dsCtx,
						Populated: populated,
					}

					serviceTester := consistencytestutil.NewServiceTester(cad.Conn)

					for _, dispatcherKind := range []string{"local", "caching"} {
						t.Run(dispatcherKind, func(t *testing.T) {
							t.Parallel()

							dispatcher := consistencytestutil.CreateDispatcherForTesting(t, dispatcherKind == "caching")

							vctx := consistencytestutil.ValidationContext{
								ClusterAndData:   cad,
								AccessibilitySet: accessibilitySet,
								ServiceTester:    serviceTester,
								Revision:         headRevision,
								Dispatcher:       dispatcher,
							}

							consistencytestutil.RunConsistencyTestSuiteForCluster(t, vctx)
						})
					}
				})
			}
		})
	}
}

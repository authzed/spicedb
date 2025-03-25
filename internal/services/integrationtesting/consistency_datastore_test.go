//go:build ci && docker && datastoreconsistency
// +build ci,docker,datastoreconsistency

package integrationtesting_test

import (
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestConsistencyPerDatastore(t *testing.T) {
	// TODO(jschorr): Re-enable for *all* files once we make this faster.
	_, filename, _, _ := runtime.Caller(0)
	consistencyTestFiles := []string{
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "document.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "basicrbac.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "public.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "nil.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "basiccaveat.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "relexpiration.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "expirationwithcaveat.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "simplewildcard.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "caveatarrow.yaml"),
		path.Join(path.Join(path.Dir(filename), "testconfigs"), "intersectionarrow.yaml"),
	}

	for _, engineID := range datastore.Engines {
		engineID := engineID

		t.Run(engineID, func(t *testing.T) {
			t.Parallel()
			for _, filePath := range consistencyTestFiles {
				filePath := filePath

				t.Run(path.Base(filePath), func(t *testing.T) {
					// FIXME errors arise if spanner is run in parallel
					if engineID != "spanner" {
						t.Parallel()
					}

					rde := testdatastore.RunDatastoreEngine(t, engineID)
					baseds := rde.NewDatastore(t, config.DatastoreConfigInitFunc(t,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10),
						dsconfig.WithMaxRetries(50),
						dsconfig.WithRequestHedgingEnabled(false)))
					ds := proxy.WrapWithIndexCheckingDatastoreProxyIfApplicable(baseds)

					cad := consistencytestutil.BuildDataAndCreateClusterForTesting(t, filePath, ds)
					dispatcher := graph.NewLocalOnlyDispatcher(10, 100)
					accessibilitySet := consistencytestutil.BuildAccessibilitySet(t, cad)

					headRevision, err := cad.DataStore.HeadRevision(cad.Ctx)
					require.NoError(t, err)

					// Run the assertions within each file.
					testers := consistencytestutil.ServiceTesters(cad.Conn)
					for _, tester := range testers {
						tester := tester

						vctx := validationContext{
							clusterAndData:   cad,
							accessibilitySet: accessibilitySet,
							serviceTester:    tester,
							revision:         headRevision,
							dispatcher:       dispatcher,
						}

						t.Run(tester.Name(), func(t *testing.T) {
							t.Parallel()
							runAssertions(t, vctx)
						})
					}
				})
			}
		})
	}
}

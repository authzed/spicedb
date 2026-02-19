//go:build steelthread && docker && image

package steelthreadtesting

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gosimple/slug"
	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/proxy/indexcheck"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func TestMemdbSteelThreads(t *testing.T) {
	for _, tc := range steelThreadTestCases {
		t.Run(tc.name, func(t *testing.T) {
			emptyDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 5*time.Second, 2*time.Hour)
			require.NoError(t, err)

			runSteelThreadTest(t, tc, emptyDS)
		})
	}
}

func TestNonMemdbSteelThreads(t *testing.T) {
	if os.Getenv("REGENERATE_STEEL_RESULTS") == "true" {
		t.Skip("Skipping non-memdb steelthread tests in regenerate mode")
	}

	for _, engineID := range datastore.SortedEngineIDs() {
		t.Run(engineID, func(t *testing.T) {
			rde := testdatastore.RunDatastoreEngine(t, engineID)

			for _, tc := range steelThreadTestCases {
				t.Run(tc.name, func(t *testing.T) {
					ds := rde.NewDatastore(t, config.DatastoreConfigInitFunc(t,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10),
						dsconfig.WithMaxRetries(50),
						dsconfig.WithExperimentalColumnOptimization(true),
						dsconfig.WithWriteAcquisitionTimeout(5*time.Second)))

					ds = indexcheck.WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
					runSteelThreadTest(t, tc, ds)
				})
			}
		})
	}
}

func runSteelThreadTest(t *testing.T, tc steelThreadTestCase, ds datastore.Datastore) {
	req := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clientConn, cleanup, _, _ := testserver.NewTestServerWithConfigAndDatastore(req, 0, 0, false,
		testserver.DefaultTestServerConfig,
		ds,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			// Load in the data.
			_, rev, err := validationfile.PopulateFromFiles(ctx, datalayer.NewDataLayer(ds), caveattypes.Default.TypeSet, []string{"testdata/" + tc.datafile})
			require.NoError(err)

			return ds, rev
		})

	t.Cleanup(cleanup)

	clients := stClients{
		PermissionsClient: v1.NewPermissionsServiceClient(clientConn),
		SchemaClient:      v1.NewSchemaServiceClient(clientConn),
	}
	for _, operationInfo := range tc.operations {
		t.Run(operationInfo.name, func(t *testing.T) {
			handler, ok := operations[operationInfo.operationName]
			require.True(t, ok, "operation not found: %s", operationInfo.name)

			result, err := handler(operationInfo.arguments, clients)
			require.NoError(t, err)

			// Generate the actual results file.
			actual, err := yamlv3.Marshal(result)
			require.NoError(t, err)

			// Read in the expected results file.
			resultsFileName := fmt.Sprintf("steelresults/%s-%s-results.yaml", slug.Make(tc.name), slug.Make(operationInfo.name))
			if operationInfo.resultsFileName != "" {
				resultsFileName = "steelresults/" + operationInfo.resultsFileName
			}

			if os.Getenv("REGENERATE_STEEL_RESULTS") == "true" {
				err := os.WriteFile(resultsFileName, []byte("---\n"+string(actual)), 0o600)
				require.NoError(t, err)
				return
			}

			expected, err := os.ReadFile(resultsFileName)
			require.NoError(t, err)

			// Compare the actual and expected results.
			require.Equal(t, string(expected), "---\n"+string(actual))
		})
	}
}

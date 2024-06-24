//go:build steelthread
// +build steelthread

package steelthreadtesting

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	yamlv3 "gopkg.in/yaml.v3"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/dustin/go-humanize"
	"github.com/gosimple/slug"
	"github.com/stretchr/testify/require"
)

const defaultConnBufferSize = humanize.MiByte

func TestSteelThreads(t *testing.T) {
	for _, tc := range steelThreadTestCases {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			clientConn, cleanup, _, _ := testserver.NewTestServer(req, 0, 0, false, func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
				// Load in the data.
				_, rev, err := validationfile.PopulateFromFiles(ctx, ds, []string{"testdata/" + tc.datafile})
				require.NoError(err)

				return ds, rev
			})

			t.Cleanup(cleanup)

			psClient := v1.NewPermissionsServiceClient(clientConn)
			for _, operationInfo := range tc.operations {
				t.Run(operationInfo.name, func(t *testing.T) {
					handler, ok := operations[operationInfo.operationName]
					require.True(t, ok, "operation not found: %s", operationInfo.name)

					result, err := handler(operationInfo.arguments, psClient)
					require.NoError(t, err)

					// Generate the actual results file.
					actual, err := yamlv3.Marshal(result)
					require.NoError(t, err)

					// Read in the expected results file.
					resultsFileName := fmt.Sprintf("steelresults/%s-%s-results.yaml", slug.Make(tc.name), slug.Make(operationInfo.name))

					if os.Getenv("REGENERATE_STEEL_RESULTS") == "true" {
						err := os.WriteFile(resultsFileName, []byte("---\n"+string(actual)), 0o644)
						require.NoError(t, err)
						return
					}

					expected, err := os.ReadFile(resultsFileName)
					require.NoError(t, err)

					// Compare the actual and expected results.
					require.Equal(t, string(expected), "---\n"+string(actual))
				})
			}
		})
	}
}

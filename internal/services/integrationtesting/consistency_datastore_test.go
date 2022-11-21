//go:build ci && docker
// +build ci,docker

package integrationtesting_test

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func TestConsistencyPerDatastore(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	consistencyTestFiles := []string{}
	err := filepath.Walk(path.Join(path.Dir(filename), "testconfigs"), func(path string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), ".yaml") {
			consistencyTestFiles = append(consistencyTestFiles, path)
		}

		return nil
	})

	rrequire := require.New(t)
	rrequire.NoError(err)

	for _, engineID := range datastore.Engines {
		t.Run(engineID, func(t *testing.T) {
			for _, filePath := range consistencyTestFiles {
				t.Run(path.Base(filePath), func(t *testing.T) {
					lrequire := require.New(t)

					rde := testdatastore.RunDatastoreEngine(t, engineID)
					ds := rde.NewDatastore(t, config.DatastoreConfigInitFunc(t,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10)))

					fullyResolved, revision, err := validationfile.PopulateFromFiles(context.Background(), ds, []string{filePath})
					require.NoError(t, err)

					conn, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
					t.Cleanup(cleanup)

					dsCtx := datastoremw.ContextWithHandle(context.Background())
					lrequire.NoError(datastoremw.SetInContext(dsCtx, ds))

					dispatcher := graph.NewLocalOnlyDispatcher(10)

					testers := []serviceTester{
						v1ServiceTester{v1.NewPermissionsServiceClient(conn[0])},
					}

					// Run the assertions within each file.
					for _, tester := range testers {
						t.Run(tester.Name(), func(t *testing.T) {
							runAssertions(t, tester, dispatcher, fullyResolved, revision)
						})
					}
				})
			}
		})
	}
}

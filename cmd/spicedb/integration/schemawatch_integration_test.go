//go:build image

package integration_test

import (
	"fmt"
	"io"
	"maps"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

// This is needed because the containers speak over their
// default ports, not over the host-mapped ports that the
// container exposes.
var engineDefaultPortMap = map[string]string{
	"cockroachdb": "26257",
	"postgres":    "5432",
	"mysql":       "3306",
	"spanner":     "9010",
}

func internalConnString(t testing.TB, dbConnString, driverName string) string {
	t.Helper()
	dbURL, err := url.Parse(dbConnString)
	require.NoError(t, err)
	defaultPort, ok := engineDefaultPortMap[driverName]
	require.True(t, ok, "missing default port for %s", driverName)
	// NOTE: we need to replace this because the migrate container
	// lives on the same network as the DB container - it uses
	// the internal hostname and port.
	// We ignore the case where the host is unset because that's spanner
	// and spanner is a special child.
	if dbURL.Host != "" {
		dbURL.Host = fmt.Sprintf("%s:%s", driverName, defaultPort)
	}
	return dbURL.String()
}

func TestSchemaWatch(t *testing.T) {
	engines := map[string]bool{
		"postgres":    false,
		"mysql":       false,
		"cockroachdb": true,
		"spanner":     false,
	}
	require.Len(t, datastore.Engines, len(engines))

	for driverName, shouldRun := range engines {
		if !shouldRun {
			continue
		}

		t.Run(driverName, func(t *testing.T) {
			ctx := t.Context()

			// Create an internal network
			net, err := network.New(ctx)
			testcontainers.CleanupNetwork(t, net)
			require.NoError(t, err)

			engine := testdatastore.RunDatastoreEngine(t,
				driverName,
				// Pass in a network so that the spicedb and migrate containers
				// can talk to the database container
				network.WithNetwork([]string{driverName}, net))

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			db := engine.NewDatabase(t)

			envVars["SPICEDB_DATASTORE_ENGINE"] = driverName
			envVars["SPICEDB_DATASTORE_CONN_URI"] = internalConnString(t, db, driverName)

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := testcontainers.Run(ctx, ciImage,
				network.WithNetwork([]string{"migrate"}, net),
				testcontainers.WithLogger(log.TestLogger(t)),
				testcontainers.WithCmd("migrate", "head"),
				testcontainers.WithEnv(envVars),
				testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
			)
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, migrateContainer)

			// Ensure the command completed successfully.
			containerState, err := migrateContainer.State(ctx)
			if containerState.ExitCode != 0 {
				logReader, err := migrateContainer.Logs(t.Context())
				require.NoError(t, err)
				out, err := io.ReadAll(logReader)
				require.NoError(t, err)
				t.Log("Container logs:")
				t.Log(string(out))
			}
			require.NoError(t, err)
			require.Equal(t, 0, containerState.ExitCode)
			t.Log("finished migrating")

			spicedbEnvVars := make(map[string]string)
			maps.Copy(spicedbEnvVars, envVars)

			spicedbEnvVars["SPICEDB_DATASTORE_GC_INTERVAL"] = "1s"
			spicedbEnvVars["SPICEDB_LOG_LEVEL"] = "trace"
			spicedbEnvVars["SPICEDB_ENABLE_EXPERIMENTAL_WATCHABLE_SCHEMA_CACHE"] = "true"

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			// Consume logs so we can ensure schema watch has started before graceful shutdown.
			ww := &logWaiter{c: make(chan bool, 1), expectedString: "starting watching cache"}
			serveContainer, err := sdbtestcontainer.Run(ctx, ciImage,
				network.WithNetwork([]string{"spicedb"}, net),
				testcontainers.WithLogConsumerConfig(&testcontainers.LogConsumerConfig{
					Consumers: []testcontainers.LogConsumer{ww},
				}),
				testcontainers.WithEnv(spicedbEnvVars),
			)
			testcontainers.CleanupContainer(t, serveContainer)
			require.NoError(t, err)

			select {
			case <-ww.c:
			case <-time.After(10 * time.Second):
				require.Fail(t, "timed out waiting for schema watch to run")
			}
		})
	}
}

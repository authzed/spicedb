//go:build image

package integration_test

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

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

			engine := testdatastore.RunDatastoreEngine(t, driverName)

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// TODO: this is wrong
			// The datastore listens on a host-mapped port, so the SpiceDB
			// container must reach it via host.docker.internal.
			db := engine.NewDatabase(t)

			envVars["SPICEDB_DATASTORE_ENGINE"] = driverName
			envVars["SPICEDB_DATASTORE_CONN_URI"] = db

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := sdbtestcontainer.Run(ctx, ciImage,
				network.WithNetwork([]string{"migrate"}, net),
				testcontainers.WithCmd("migrate", "head"),
				testcontainers.WithEnv(envVars),
				testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
			)
			require.NoError(t, err)
			testcontainers.CleanupContainer(t, migrateContainer)

			// Ensure the command completed successfully.
			exitCode, err := migrateContainer.State(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, exitCode.ExitCode)
			t.Log("finished migrating")

			spicedbEnvVars := make(map[string]string)
			maps.Copy(spicedbEnvVars, envVars)

			spicedbEnvVars["SPICEDB_DATASTORE_GC_INTERVAL"] = "1s"
			spicedbEnvVars["SPICEDB_LOG_LEVEL"] = "trace"
			spicedbEnvVars["SPICEDB_ENABLE_EXPERIMENTAL_WATCHABLE_SCHEMA_CACHE"] = "true"

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			// Consume logs so we can ensure schema watch has started before graceful shutdown.
			ww := &logWaiter{c: make(chan bool, 1), expectedString: "starting watching cache"}
			serveContainer, err := sdbtestcontainer.Run(ctx, sdbtestcontainer.DefaultImageReference,
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

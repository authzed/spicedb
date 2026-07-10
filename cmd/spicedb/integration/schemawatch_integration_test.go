//go:build image

package integration_test

import (
	"io"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/testutil"
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

			engine := testdatastore.RunDatastoreEngine(t,
				driverName,
				// Pass in a network so that the spicedb and migrate containers
				// can talk to the database container
				network.WithNetwork([]string{driverName}, net))

			db := engine.NewDatabase(t)

			connectionVars, err := testutil.InternalConnectionEnvVars(db, driverName)
			require.NoError(t, err)

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := testcontainers.Run(ctx, ciImage,
				network.WithNetwork([]string{"migrate"}, net),
				testcontainers.WithLogger(log.TestLogger(t)),
				testcontainers.WithCmd("migrate", "head"),
				testcontainers.WithEnv(connectionVars),
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
			maps.Copy(spicedbEnvVars, connectionVars)

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

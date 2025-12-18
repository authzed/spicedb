//go:build image

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
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
			ctx := context.Background()
			bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

			// Create a bridge network for testing.
			net, err := network.New(ctx, network.WithDriver("bridge"), network.WithLabels(map[string]string{"name": bridgeNetworkName}))
			testcontainers.CleanupNetwork(t, net)
			require.NoError(t, err)

			engine := testdatastore.RunDatastoreEngine(t, driverName)

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(hostInternalize(env), "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// The datastore listens on a host-mapped port, so the SpiceDB
			// container must reach it via host.docker.internal.
			db := hostInternalize(engine.NewDatabase(t))

			// Run the migrate command and wait for it to complete.
			migrateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:              "authzed/spicedb:ci",
					Cmd:                []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
					Networks:           []string{bridgeNetworkName},
					Env:                envVars,
					HostConfigModifier: withHostGateway,
					WaitingFor:         wait.ForExit().WithExitTimeout(time.Minute),
				},
				Started: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = migrateContainer.Terminate(ctx)
			})

			// Ensure the command completed successfully.
			exitCode, err := migrateContainer.State(ctx)
			require.NoError(t, err)
			require.Equal(t, 0, exitCode.ExitCode)
			t.Log("finished migrating")

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			// Consume logs so we can ensure schema watch has started before graceful shutdown.
			ww := &logWaiter{c: make(chan bool, 1), expectedString: "starting watching cache"}
			serveContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", "", "--log-level", "trace", "--enable-experimental-watchable-schema-cache"},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
					LogConsumerCfg: &testcontainers.LogConsumerConfig{
						Consumers: []testcontainers.LogConsumer{ww},
					},
					HostConfigModifier: withHostGateway,
				},
				Started: true,
			})
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

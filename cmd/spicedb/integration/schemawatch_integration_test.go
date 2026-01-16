//go:build docker && image

package integration_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

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
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = net.Remove(ctx)
			})

			engine := testdatastore.RunDatastoreEngineWithBridge(t, driverName, bridgeNetworkName)

			envVars := map[string]string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// Run the migrate command and wait for it to complete.
			db := engine.NewDatabase(t)
			migrateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
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

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			serveContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", "", "--log-level", "trace", "--enable-experimental-watchable-schema-cache"},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
				},
				Started: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = serveContainer.Terminate(ctx)
			})

			ww := &watchingWriter{make(chan bool, 1), "starting watching cache"}

			// Grab logs and ensure schema watch has started before graceful shutdown.
			go (func() {
				logReader, err := serveContainer.Logs(ctx)
				if err != nil {
					assert.NoError(t, err)
					return
				}
				defer logReader.Close()
				_, err = io.Copy(ww, logReader)
				assert.NoError(t, err)
			})()

			select {
			case <-ww.c:
				break

			case <-time.After(10 * time.Second):
				require.Fail(t, "timed out waiting for schema watch to run")
			}

			require.True(t, gracefulShutdown(ctx, serveContainer))
		})
	}
}

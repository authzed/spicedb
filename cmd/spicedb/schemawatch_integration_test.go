//go:build docker && image
// +build docker,image

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

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
	require.Equal(t, len(engines), len(datastore.Engines))

	for driverName, shouldRun := range engines {
		if !shouldRun {
			continue
		}

		t.Run(driverName, func(t *testing.T) {
			bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			// Create a bridge network for testing.
			network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
				Name: bridgeNetworkName,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				pool.Client.RemoveNetwork(network.ID)
			})

			engine := testdatastore.RunDatastoreEngineWithBridge(t, driverName, bridgeNetworkName)

			envVars := []string{}
			if wev, ok := engine.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				envVars = wev.ExternalEnvVars()
			}

			// Run the migrate command and wait for it to complete.
			db := engine.NewDatabase(t)
			migrateResource, err := pool.RunWithOptions(&dockertest.RunOptions{
				Repository: "authzed/spicedb",
				Tag:        "ci",
				Cmd:        []string{"migrate", "head", "--datastore-engine", driverName, "--datastore-conn-uri", db},
				NetworkID:  bridgeNetworkName,
				Env:        envVars,
			}, func(config *docker.HostConfig) {
				config.RestartPolicy = docker.RestartPolicy{
					Name: "no",
				}
			})
			require.NoError(t, err)

			waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Ensure the command completed successfully.
			status, err := pool.Client.WaitContainerWithContext(migrateResource.Container.ID, waitCtx)
			require.NoError(t, err)
			require.Equal(t, 0, status)

			// Run a serve and immediately close, ensuring it shuts down gracefully.
			serveResource, err := pool.RunWithOptions(&dockertest.RunOptions{
				Repository: "authzed/spicedb",
				Tag:        "ci",
				Cmd:        []string{"serve", "--grpc-preshared-key", "firstkey", "--datastore-engine", driverName, "--datastore-conn-uri", db, "--datastore-gc-interval", "1s", "--telemetry-endpoint", "", "--log-level", "trace", "--enable-experimental-watchable-schema-cache"},
				NetworkID:  bridgeNetworkName,
				Env:        envVars,
			}, func(config *docker.HostConfig) {
				config.RestartPolicy = docker.RestartPolicy{
					Name: "no",
				}
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = pool.Purge(serveResource)
			})

			ww := &watchingWriter{make(chan bool, 1), "starting watching cache"}

			// Grab logs and ensure GC has run before starting a graceful shutdown.
			opts := docker.LogsOptions{
				Context:      context.Background(),
				Stderr:       true,
				Stdout:       true,
				Follow:       true,
				Timestamps:   true,
				RawTerminal:  true,
				Container:    serveResource.Container.ID,
				OutputStream: ww,
			}

			go (func() {
				err = pool.Client.Logs(opts)
				require.NoError(t, err)
			})()

			select {
			case <-ww.c:
				break

			case <-time.After(10 * time.Second):
				require.Fail(t, "timed out waiting for schema watch to run")
			}

			require.True(t, gracefulShutdown(pool, serveResource))
		})
	}
}

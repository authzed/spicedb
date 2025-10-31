//go:build docker && image
// +build docker,image

package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

var toSkip = []string{"memory"}

func TestMigrate(t *testing.T) {
	bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Create a bridge network for testing.
	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{
		Name: bridgeNetworkName,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Client.RemoveNetwork(network.ID)
	})

	for _, engineKey := range datastore.Engines {
		if slices.Contains(toSkip, engineKey) {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			for retryCount := range 5 {
				engineKey := engineKey

				r := testdatastore.RunDatastoreEngineWithBridge(t, engineKey, bridgeNetworkName)
				db := r.NewDatabase(t)

				envVars := []string{}
				if wev, ok := r.(testdatastore.RunningEngineForTestWithEnvVars); ok {
					envVars = wev.ExternalEnvVars()
				}

				// Run the migrate command and wait for it to complete.
				resource, err := pool.RunWithOptions(&dockertest.RunOptions{
					Repository: "authzed/spicedb",
					Tag:        "ci",
					Cmd:        []string{"migrate", "head", "--datastore-engine", engineKey, "--datastore-conn-uri", db},
					NetworkID:  bridgeNetworkName,
					Env:        envVars,
				}, func(config *docker.HostConfig) {
					config.RestartPolicy = docker.RestartPolicy{
						Name: "no",
					}
				})
				if err != nil && retryCount < 4 {
					// On error, retry a few times before failing
					t.Logf("retrying migration for engine %s due to error: %v", engineKey, err)
					continue
				}

				require.NoError(t, err)

				waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				// Ensure the command completed successfully.
				status, err := pool.Client.WaitContainerWithContext(resource.Container.ID, waitCtx)
				if err != nil && retryCount < 4 {
					// On error, retry a few times before failing
					t.Logf("retrying migration for engine %s due to error: %v", engineKey, err)
					continue
				}

				require.NoError(t, err)

				if status != 0 {
					stream := new(bytes.Buffer)

					lerr := pool.Client.Logs(docker.LogsOptions{
						Context:      waitCtx,
						OutputStream: stream,
						ErrorStream:  stream,
						Stdout:       true,
						Stderr:       true,
						Container:    resource.Container.ID,
					})
					require.NoError(t, lerr)

					require.Fail(t, "Got non-zero exit code", stream.String())
				}

				t.Cleanup(func() {
					// When you're done, kill and remove the container
					_ = pool.Purge(resource)
				})
			}
		})
	}
}

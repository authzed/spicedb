//go:build docker && image

package integration_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

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
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = pool.Purge(resource)
			})

			// Ensure the command completed successfully.
			status, err := pool.Client.WaitContainerWithContext(resource.Container.ID, t.Context())
			require.NoError(t, err)

			if status != 0 {
				stream := new(bytes.Buffer)

				lerr := pool.Client.Logs(docker.LogsOptions{
					Context:      t.Context(),
					OutputStream: stream,
					ErrorStream:  stream,
					Stdout:       true,
					Stderr:       true,
					Container:    resource.Container.ID,
				})
				require.NoError(t, lerr)

				require.Fail(t, "Got non-zero exit code", stream.String())
			}
		})
	}
}

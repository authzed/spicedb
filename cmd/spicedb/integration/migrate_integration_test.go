//go:build docker && image

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

var toSkip = []string{"memory"}

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())

	// Create a bridge network for testing.
	net, err := network.New(ctx, network.WithDriver("bridge"), network.WithLabels(map[string]string{"name": bridgeNetworkName}))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = net.Remove(ctx)
	})

	for _, engineKey := range datastore.Engines {
		if slices.Contains(toSkip, engineKey) {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			engineKey := engineKey

			r := testdatastore.RunDatastoreEngineWithBridge(t, engineKey, bridgeNetworkName)
			db := r.NewDatabase(t)

			envVars := map[string]string{}
			if wev, ok := r.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, env := range wev.ExternalEnvVars() {
					parts := strings.SplitN(env, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// Run the migrate command and wait for it to complete.
			container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
				ContainerRequest: testcontainers.ContainerRequest{
					Image:    "authzed/spicedb:ci",
					Cmd:      []string{"migrate", "head", "--datastore-engine", engineKey, "--datastore-conn-uri", db},
					Networks: []string{bridgeNetworkName},
					Env:      envVars,
				},
				Started: true,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = container.Terminate(ctx)
			})

			// Ensure the command completed successfully.
			state, err := container.State(ctx)
			require.NoError(t, err)

			if state.ExitCode != 0 {
				stream := new(bytes.Buffer)

				logReader, lerr := container.Logs(ctx)
				require.NoError(t, lerr)
				defer logReader.Close()

				_, lerr = io.Copy(stream, logReader)
				require.NoError(t, lerr)

				require.Fail(t, "Got non-zero exit code", stream.String())
			}
		})
	}
}

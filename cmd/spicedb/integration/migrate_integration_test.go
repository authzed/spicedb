//go:build image

package integration_test

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

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
			r := testdatastore.RunDatastoreEngineWithBridge(t, engineKey, bridgeNetworkName)
			db := r.NewDatabase(t)

			envVars := []string{}
			if wev, ok := r.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				envVars = wev.ExternalEnvVars()
			}

			runMigrateHead(t, pool, "ci", bridgeNetworkName, engineKey, db, envVars)
		})
	}
}

// TestMigrateUpgrade verifies that migrations introduced on v1.53.0
// apply cleanly on top of a database that was populated by v1.52.0.
func TestMigrateUpgrade(t *testing.T) {
	bridgeNetworkName := fmt.Sprintf("bridge-%s", uuid.New().String())
	presharedKey := uuid.NewString()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

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
			r := testdatastore.RunDatastoreEngineWithBridge(t, engineKey, bridgeNetworkName)
			db := r.NewDatabase(t)

			envVars := []string{}
			if wev, ok := r.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				envVars = wev.ExternalEnvVars()
			}

			// Run "migrate head" against v1.52.0
			runMigrateHead(t, pool, "v1.52.0", bridgeNetworkName, engineKey, db, envVars)

			// Write a schema with definitions and caveats
			_, grpcPort := runServe(t, pool, "v1.52.0", bridgeNetworkName, engineKey, db, presharedKey, envVars)

			conn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%s", grpcPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(presharedKey),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.Close()
			})

			require.NoError(t, pool.Retry(func() error {
				_, err := v1.NewSchemaServiceClient(conn).WriteSchema(t.Context(), &v1.WriteSchemaRequest{
					Schema: `
						caveat is_public(public bool) {
							public
						}

						definition user {}
						definition document {
							relation viewer: user with is_public
							permission view = viewer
						}
					`,
				})
				return err
			}))

			// Now run "migrate head" again but using the latest code on "main"
			runMigrateHead(t, pool, "ci", bridgeNetworkName, engineKey, db, envVars)
		})
	}
}

// runMigrateHead launches a one-shot `migrate head` container at the given image
// tag against the supplied datastore, waits for completion, and fails the test if
// the exit code is non-zero (dumping container logs).
func runMigrateHead(t *testing.T, pool *dockertest.Pool, spiceDBImageTag, bridgeNetworkName, engineKey, db string, envVars []string) {
	t.Helper()

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "authzed/spicedb",
		Tag:        spiceDBImageTag,
		Cmd:        []string{"migrate", "head", "--datastore-engine", engineKey, "--datastore-conn-uri", db},
		NetworkID:  bridgeNetworkName,
		Env:        envVars,
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Purge(resource)
	})

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
		require.Failf(t, "migrate head exited with non-zero exit code", "image=%s engine=%s status=%d logs:\n%s", spiceDBImageTag, engineKey, status, stream.String())
	}
}

func runServe(t *testing.T, pool *dockertest.Pool, spiceDBImageTag, bridgeNetworkName, engineKey, dbConnection, presharedKey string, envVars []string) (*dockertest.Resource, string) {
	t.Helper()

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "authzed/spicedb",
		Tag:          spiceDBImageTag,
		Cmd:          []string{"serve", "--log-level", "debug", "--grpc-preshared-key", presharedKey, "--datastore-engine", engineKey, "--datastore-conn-uri", dbConnection, "--telemetry-endpoint", ""},
		NetworkID:    bridgeNetworkName,
		Env:          envVars,
		ExposedPorts: []string{"50051/tcp"},
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Purge(resource)
	})

	port := resource.GetPort("50051/tcp")
	require.NotEmpty(t, port, "serve container did not expose a host port for 50051/tcp")

	// pool.Retry below will handle full readiness; the deadline here just
	// caps how long we wait for the gRPC service to come up.
	pool.MaxWait = 60 * time.Second

	return resource, port
}

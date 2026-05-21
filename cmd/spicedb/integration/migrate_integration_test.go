//go:build image

package integration_test

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
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
	"github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

var toSkip = []string{"memory"}

func TestMigrateWithNoData(t *testing.T) {
	for _, engineKey := range datastore.Engines {
		if slices.Contains(toSkip, engineKey) {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := testdatastore.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)

			runMigrateHeadWithCode(t, engineKey, db)
		})
	}
}

// TestMigrateWithData verifies that migrations introduced on v1.53.0
// apply on top of a database that has data written by v1.52.0.
func TestMigrateWithData(t *testing.T) {
	presharedKey := uuid.NewString()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	for _, engineKey := range datastore.Engines {
		if slices.Contains(toSkip, engineKey) {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := testdatastore.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)
			containerDB := hostInternalize(db)

			envVars := []string{}
			if wev, ok := r.(testdatastore.RunningEngineForTestWithEnvVars); ok {
				for _, ev := range wev.ExternalEnvVars() {
					envVars = append(envVars, hostInternalize(ev))
				}
			}

			// 1. Migrate using SpiceDB v1.52.0.
			runMigrateHeadWithContainer(t, pool, "v1.52.0", "", engineKey, containerDB, envVars)

			// 2. Run v1.52.0 serve and write a schema.
			_, grpcPort := runServe(t, pool, "v1.52.0", "", engineKey, containerDB, presharedKey, envVars)

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

			// 3. Migrate using the current branch's code, in-process,
			// so the migration code is included in the coverage profile.
			runMigrateHeadWithCode(t, engineKey, db)
		})
	}
}

// runMigrateHeadWithCode runs `migrate head` in-process against the supplied
// datastore by calling cmd.ExecuteMigrate directly.
func runMigrateHeadWithCode(t *testing.T, engineKey, dbConnection string) {
	t.Helper()

	cfg := &cmd.MigrateConfig{
		DatastoreEngine: engineKey,
		DatastoreURI:    dbConnection,
		Timeout:         5 * time.Minute,
		BatchSize:       1000,
	}
	require.NoError(t, cmd.ExecuteMigrate(t.Context(), cfg, migrate.Head))
}

// runMigrateHeadWithContainer launches a docker container that runs `spicedb migrate head`
// Use this when you need to exercise a released SpiceDB binary.
func runMigrateHeadWithContainer(t *testing.T, pool *dockertest.Pool, spiceDBImageTag, bridgeNetworkName, engineKey, db string, envVars []string) {
	t.Helper()

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "authzed/spicedb",
		Tag:        spiceDBImageTag,
		Cmd:        []string{"migrate", "head", "--datastore-engine", engineKey, "--datastore-conn-uri", db},
		NetworkID:  bridgeNetworkName,
		Env:        envVars,
	}, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.ExtraHosts = append(config.ExtraHosts, "host.docker.internal:host-gateway")
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
		config.ExtraHosts = append(config.ExtraHosts, "host.docker.internal:host-gateway")
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pool.Purge(resource)
	})

	port := resource.GetPort("50051/tcp")
	require.NotEmpty(t, port, "serve container did not expose a host port for 50051/tcp")

	return resource, port
}

func hostInternalize(uri string) string {
	return strings.ReplaceAll(uri, "localhost", "host.docker.internal")
}

package cmd

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	datastoreTest "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func TestExecuteMigrateErrorsOut(t *testing.T) {
	tests := []struct {
		name          string
		cfgBuilder    func(t *testing.T) *MigrateConfig
		revision      string
		expectedError string
	}{
		{
			name: "missing revision returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine: "postgres",
					DatastoreURI:    "postgres://localhost:5432/test",
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      "",
			expectedError: "missing required revision",
		},
		{
			name: "unsupported engine returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine: "unsupported",
					DatastoreURI:    "some://uri",
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      "head",
			expectedError: "cannot migrate datastore engine type: unsupported",
		},
		{
			name: "cockroachdb driver creation failure returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine: "cockroachdb",
					DatastoreURI:    "invalid://not-a-postgres-uri",
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unable to create migration driver for cockroachdb",
		},
		{
			name: "postgres invalid credentials provider returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine:         "postgres",
					DatastoreURI:            "postgres://localhost:5432/test",
					CredentialsProviderName: "nonexistent-provider",
					Timeout:                 1 * time.Hour,
					BatchSize:               1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unknown credentials provider",
		},
		{
			name: "postgres driver creation failure returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine: "postgres",
					DatastoreURI:    "invalid://not-a-postgres-uri",
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unable to create migration driver for postgres",
		},
		{
			name: "spanner driver creation failure returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine:        "spanner",
					DatastoreURI:           "projects/test/instances/test/databases/test",
					SpannerCredentialsFile: "/nonexistent/credentials/file.json",
					SpannerEmulatorHost:    "",
					Timeout:                1 * time.Hour,
					BatchSize:              1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unable to create migration driver for spanner",
		},
		{
			name: "mysql invalid credentials provider returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine:         "mysql",
					DatastoreURI:            "user:password@tcp(localhost:3306)/db",
					CredentialsProviderName: "nonexistent-provider",
					Timeout:                 1 * time.Hour,
					BatchSize:               1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unknown credentials provider",
		},
		{
			name: "mysql driver creation failure returns error",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				return &MigrateConfig{
					DatastoreEngine: "mysql",
					DatastoreURI:    "invalid://not-a-mysql-dsn",
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "unable to create migration driver for mysql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfgBuilder(t)

			err := executeMigrate(t.Context(), cfg, tt.revision)
			if tt.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

func TestExecuteMigrateWithNoDataSucceeds(t *testing.T) {
	for _, engineKey := range datastore.Engines {
		if engineKey == "memory" {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := datastoreTest.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)

			cfg := &MigrateConfig{
				DatastoreEngine: engineKey,
				DatastoreURI:    db,
				Timeout:         1 * time.Hour,
				BatchSize:       1000,
			}
			require.NoError(t, executeMigrate(t.Context(), cfg, migrate.Head))
		})
	}
}

// TestExecuteMigrateWithDataSucceeds verifies that migrations introduced on v1.53.0
// apply on top of a database that has data written by v1.52.0.
func TestExecuteMigrateWithDataSucceeds(t *testing.T) {
	presharedKey := uuid.NewString()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	for _, engineKey := range datastore.Engines {
		if engineKey == "memory" {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := datastoreTest.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)
			containerDB := hostInternalize(db)

			envVars := []string{}
			if wev, ok := r.(datastoreTest.RunningEngineForTestWithEnvVars); ok {
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
			cfg := &MigrateConfig{
				DatastoreEngine: engineKey,
				DatastoreURI:    db,
				Timeout:         5 * time.Minute,
				BatchSize:       1000,
			}
			require.NoError(t, executeMigrate(t.Context(), cfg, migrate.Head))
		})
	}
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

func TestMigrateRun(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		flags         map[string]string
		expectedError string
	}{
		{
			name:          "missing revision argument returns error",
			args:          []string{},
			flags:         map[string]string{"datastore-engine": "postgres"},
			expectedError: "missing required argument: 'revision'",
		},
		{
			name:          "too many arguments returns error",
			args:          []string{"head", "extra"},
			flags:         map[string]string{"datastore-engine": "postgres"},
			expectedError: "missing required argument: 'revision'",
		},
		{
			name: "unsupported engine returns error via cobra path",
			args: []string{"head"},
			flags: map[string]string{
				"datastore-engine": "unsupported",
			},
			expectedError: "cannot migrate datastore engine type: unsupported",
		},
		{
			name: "cockroachdb driver failure via cobra path",
			args: []string{"head"},
			flags: map[string]string{
				"datastore-engine":   "cockroachdb",
				"datastore-conn-uri": "invalid://not-a-postgres-uri",
			},
			expectedError: "unable to create migration driver for cockroachdb",
		},
		{
			name: "postgres driver failure via cobra path",
			args: []string{"head"},
			flags: map[string]string{
				"datastore-engine":   "postgres",
				"datastore-conn-uri": "invalid://not-a-postgres-uri",
			},
			expectedError: "unable to create migration driver for postgres",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{
				Use:  "migrate [revision]",
				RunE: migrateRun,
			}
			RegisterMigrateFlags(cmd)

			for k, v := range tt.flags {
				require.NoError(t, cmd.Flags().Set(k, v))
			}

			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

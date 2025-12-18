package cmd

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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

	for _, engineKey := range datastore.Engines {
		if engineKey == "memory" {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := datastoreTest.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)
			containerDB := hostInternalize(db)

			envVars := map[string]string{}
			if wev, ok := r.(datastoreTest.RunningEngineForTestWithEnvVars); ok {
				for _, ev := range wev.ExternalEnvVars() {
					parts := strings.SplitN(hostInternalize(ev), "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// 1. Migrate using SpiceDB v1.52.0.
			runMigrateHeadWithContainer(t, "v1.52.0", engineKey, containerDB, envVars)

			// 2. Run v1.52.0 serve and write a schema.
			grpcPort := runServe(t, "v1.52.0", engineKey, containerDB, presharedKey, envVars)

			conn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%s", grpcPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(presharedKey),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.Close()
			})

			require.Eventually(t, func() bool {
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
				return err == nil
			}, 30*time.Second, 1*time.Second)

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

// withHostGateway lets the container reach datastores listening on host-mapped
// ports via host.docker.internal (paired with hostInternalize). Docker Desktop
// auto-resolves host.docker.internal, but Linux (e.g. CI runners) does not
// without this extra host mapping.
func withHostGateway(hc *container.HostConfig) {
	hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
}

// runMigrateHeadWithContainer launches a docker container that runs `spicedb migrate head`
// Use this when you need to exercise a released SpiceDB binary.
func runMigrateHeadWithContainer(t *testing.T, spiceDBImageTag, engineKey, db string, envVars map[string]string) {
	t.Helper()

	migrateContainer, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:              "authzed/spicedb:" + spiceDBImageTag,
			Cmd:                []string{"migrate", "head", "--datastore-engine", engineKey, "--datastore-conn-uri", db},
			Env:                envVars,
			HostConfigModifier: withHostGateway,
			WaitingFor:         wait.ForExit().WithExitTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = migrateContainer.Terminate(t.Context())
	})

	state, err := migrateContainer.State(t.Context())
	require.NoError(t, err)

	if state.ExitCode != 0 {
		// TODO use a log consumer
		stream := new(bytes.Buffer)
		logReader, lerr := migrateContainer.Logs(t.Context())
		require.NoError(t, lerr)
		defer func() { _ = logReader.Close() }()
		_, _ = io.Copy(stream, logReader)
		require.Failf(t, "migrate head exited with non-zero exit code", "image=%s engine=%s status=%d logs:\n%s", spiceDBImageTag, engineKey, state.ExitCode, stream.String())
	}
}

func runServe(t *testing.T, spiceDBImageTag, engineKey, dbConnection, presharedKey string, envVars map[string]string) string {
	t.Helper()

	serveContainer, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:              "authzed/spicedb:" + spiceDBImageTag,
			Cmd:                []string{"serve", "--log-level", "debug", "--grpc-preshared-key", presharedKey, "--datastore-engine", engineKey, "--datastore-conn-uri", dbConnection, "--telemetry-endpoint", ""},
			Env:                envVars,
			ExposedPorts:       []string{"50051/tcp"},
			HostConfigModifier: withHostGateway,
			WaitingFor:         wait.ForListeningPort("50051/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = serveContainer.Terminate(t.Context())
	})

	mappedPort, err := serveContainer.MappedPort(t.Context(), "50051/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()
	require.NotEmpty(t, port, "serve container did not expose a host port for 50051/tcp")

	return port
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

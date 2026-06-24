package cmd

import (
	"maps"
	"strings"
	"testing"
	"time"

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
	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
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
	for _, engineKey := range datastore.Engines {
		if engineKey == "memory" {
			continue
		}

		t.Run(engineKey, func(t *testing.T) {
			r := datastoreTest.RunDatastoreEngine(t, engineKey)
			db := r.NewDatabase(t)

			envVars := map[string]string{}
			if wev, ok := r.(datastoreTest.RunningEngineForTestWithEnvVars); ok {
				for _, ev := range wev.ExternalEnvVars() {
					parts := strings.SplitN(ev, "=", 2)
					if len(parts) == 2 {
						envVars[parts[0]] = parts[1]
					}
				}
			}

			// 1. Migrate using SpiceDB v1.52.0.
			runMigrateHeadWithContainer(t, "v1.52.0", engineKey, db, envVars)

			// 2. Run v1.52.0 serve and write a schema.
			serveContainer := runServe(t, "v1.52.0", engineKey, db, envVars)

			conn, err := grpc.NewClient(
				serveContainer.GRPCEndpoint(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(serveContainer.PresharedKey()),
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

// runMigrateHeadWithContainer launches a docker container that runs `spicedb migrate head`
// Use this when you need to exercise a released SpiceDB binary.
func runMigrateHeadWithContainer(t *testing.T, spiceDBImageTag, engineKey, db string, envVars map[string]string) {
	t.Helper()

	container, err := sdbtestcontainer.Run(t.Context(),
		"authzed/spicedb:" + spiceDBImageTag,
		testcontainers.WithEnv(map[string]string{
			"SPICEDB_DATASTORE_ENGINE": engineKey,
			"SPICEDB_DATASTORE_CONN_URI": db,
		}),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	state, err := container.State(t.Context())
	require.NoError(t, err)
	require.Equal(t, 0, state.ExitCode)
}

func runServe(t *testing.T, spiceDBImageTag, engineKey, dbConnection string, envVars map[string]string) *sdbtestcontainer.Container {
	t.Helper()

	containerVars := map[string]string{
			"SPICEDB_DATASTORE_ENGINE": engineKey,
			"SPICEDB_DATASTORE_CONN_URI": dbConnection,
	}

	maps.Copy(containerVars, envVars)

	container, err := sdbtestcontainer.Run(t.Context(),
		"authzed/spicedb:" + spiceDBImageTag,
		testcontainers.WithEnv(containerVars),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	return container
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

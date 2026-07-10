package cmd

import (
	"io"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

	datastoreTest "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
	"github.com/authzed/spicedb/pkg/testutil"
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

		ctx := t.Context()

		// Create an internal network
		net, err := network.New(ctx)
		testcontainers.CleanupNetwork(t, net)
		require.NoError(t, err)

		t.Run(engineKey, func(t *testing.T) {
			r := datastoreTest.RunDatastoreEngine(t, engineKey, network.WithNetwork([]string{engineKey}, net))
			db := r.NewDatabase(t)

			// 1. Migrate using SpiceDB v1.52.0.
			runMigrateHeadWithContainer(t, "v1.52.0", engineKey, db, net)

			// 2. Run v1.52.0 serve and write a schema.
			serveContainer := runServe(t, "v1.52.0", engineKey, db, net)

			conn, err := grpc.NewClient(
				serveContainer.GRPCEndpoint(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpcutil.WithInsecureBearerToken(serveContainer.PresharedKey()),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.Close()
			})

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
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
				assert.NoError(collect, err)
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
func runMigrateHeadWithContainer(t *testing.T, spiceDBImageTag, engineKey, db string, net *testcontainers.DockerNetwork) {
	t.Helper()

	ctx := t.Context()

	connectionVars, err := testutil.InternalConnectionEnvVars(db, engineKey)
	require.NoError(t, err)

	migrateContainer, err := testcontainers.Run(ctx,
		"authzed/spicedb:"+spiceDBImageTag,
		network.WithNetwork([]string{"migrate"}, net),
		testcontainers.WithLogger(log.TestLogger(t)),
		testcontainers.WithCmd("migrate", "head"),
		testcontainers.WithEnv(connectionVars),
		testcontainers.WithWaitStrategy(wait.ForExit().WithExitTimeout(time.Minute)),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, migrateContainer)

	// Ensure the command completed successfully.
	containerState, err := migrateContainer.State(ctx)
	if containerState.ExitCode != 0 {
		logReader, err := migrateContainer.Logs(t.Context())
		require.NoError(t, err)
		out, err := io.ReadAll(logReader)
		require.NoError(t, err)
		t.Log("Container logs:")
		t.Log(string(out))
	}
	require.NoError(t, err)
	require.Equal(t, 0, containerState.ExitCode)
}

func runServe(t *testing.T, spiceDBImageTag, engineKey, dbConnection string, net *testcontainers.DockerNetwork) *sdbtestcontainer.Container {
	t.Helper()

	connectionVars, err := testutil.InternalConnectionEnvVars(dbConnection, engineKey)
	require.NoError(t, err)

	container, err := sdbtestcontainer.Run(
		t.Context(),
		"authzed/spicedb:"+spiceDBImageTag,
		network.WithNetwork([]string{"spicedb"}, net),
		testcontainers.WithEnv(connectionVars),
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

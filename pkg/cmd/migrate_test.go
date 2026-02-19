package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	datastoreTest "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func TestExecuteMigrate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfgBuilder    func(t *testing.T) *MigrateConfig
		ctxBuilder    func(t *testing.T) context.Context
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
			ctxBuilder: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
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
		{
			name: "cockroachdb migration runs successfully",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				runningDatastore := datastoreTest.RunDatastoreEngine(t, "cockroachdb")
				db := runningDatastore.NewDatabase(t)
				return &MigrateConfig{
					DatastoreEngine: "cockroachdb",
					DatastoreURI:    db,
					Timeout:         1 * time.Hour,
					BatchSize:       1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "",
		},
		{
			name: "postgres migration runs successfully",
			cfgBuilder: func(t *testing.T) *MigrateConfig {
				runningDatastore := datastoreTest.RunDatastoreEngine(t, "postgres")
				db := runningDatastore.NewDatabase(t)
				return &MigrateConfig{
					DatastoreEngine:         "postgres",
					DatastoreURI:            db,
					CredentialsProviderName: "",
					Timeout:                 1 * time.Hour,
					BatchSize:               1000,
				}
			},
			revision:      migrate.Head,
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfgBuilder(t)
			ctx := t.Context()
			if tt.ctxBuilder != nil {
				ctx = tt.ctxBuilder(t)
			}
			err := executeMigrate(ctx, cfg, tt.revision)
			if tt.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

func TestMigrateRun(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

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

func TestHeadRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engine        string
		expectError   bool
		expectedError string
	}{
		{
			name:        "cockroachdb returns head revision",
			engine:      "cockroachdb",
			expectError: false,
		},
		{
			name:        "postgres returns head revision",
			engine:      "postgres",
			expectError: false,
		},
		{
			name:        "mysql returns head revision",
			engine:      "mysql",
			expectError: false,
		},
		{
			name:        "spanner returns head revision",
			engine:      "spanner",
			expectError: false,
		},
		{
			name:          "unsupported engine returns error",
			engine:        "unsupported",
			expectError:   true,
			expectedError: "cannot migrate datastore engine type: unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			revision, err := HeadRevision(tt.engine)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, revision)
		})
	}
}

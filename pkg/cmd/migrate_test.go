package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	datastoreTest "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/migrate"
)

func TestExecuteMigrate(t *testing.T) {
	t.Parallel()

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
			err := executeMigrate(t.Context(), cfg, tt.revision)
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

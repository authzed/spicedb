package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"

	datastoreTest "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/cmd/datastore"
)

func TestExecuteGC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfgBuilder    func(t *testing.T) *datastore.Config
		expectedError string
	}{
		{
			name: "cockroachdb does not support garbage collection",
			cfgBuilder: func(t *testing.T) *datastore.Config {
				cfg := datastore.DefaultDatastoreConfig()
				cfg.Engine = "cockroachdb"
				runningDatastore := datastoreTest.RunDatastoreEngine(t, cfg.Engine)
				db := runningDatastore.NewDatabase(t)
				cfg.URI = db
				return cfg
			},
			expectedError: "datastore of type 'cockroachdb' does not support garbage collection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfgBuilder(t)
			err := executeGC(cfg)
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

func TestExecuteRepair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		cfgBuilder    func(t *testing.T) *datastore.Config
		expectedError string
	}{
		{
			name: "cockroachdb does not support repair",
			cfgBuilder: func(t *testing.T) *datastore.Config {
				cfg := datastore.DefaultDatastoreConfig()
				cfg.Engine = "cockroachdb"
				runningDatastore := datastoreTest.RunDatastoreEngine(t, cfg.Engine)
				db := runningDatastore.NewDatabase(t)
				cfg.URI = db
				return cfg
			},
			expectedError: "datastore of type 'cockroachdb' does not support the repair operation",
		},
		{
			name: "postgres supports repair",
			cfgBuilder: func(t *testing.T) *datastore.Config {
				cfg := datastore.DefaultDatastoreConfig()
				cfg.Engine = "postgres"
				runningDatastore := datastoreTest.RunDatastoreEngine(t, cfg.Engine)
				db := runningDatastore.NewDatabase(t)
				cfg.URI = db
				return cfg
			},
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.cfgBuilder(t)
			err := executeRepair(cfg, []string{})
			if tt.expectedError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

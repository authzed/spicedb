package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
)

// TestPrimaryDatastoreOptionsReadOnly checks that a read-only datastore does
// not run the background jobs that write to the database: the revision
// heartbeat (which INSERTs into relation_tuple_transaction) and garbage
// collection.
func TestPrimaryDatastoreOptionsReadOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		readOnly      bool
		heartbeatFlag bool
		wantHeartbeat bool
		wantGCEnabled bool
	}{
		{name: "read-write, heartbeat on", readOnly: false, heartbeatFlag: true, wantHeartbeat: true, wantGCEnabled: true},
		{name: "read-write, heartbeat off", readOnly: false, heartbeatFlag: false, wantHeartbeat: false, wantGCEnabled: true},
		{name: "read-only, heartbeat on", readOnly: true, heartbeatFlag: true, wantHeartbeat: false, wantGCEnabled: false},
		{name: "read-only, heartbeat off", readOnly: true, heartbeatFlag: false, wantHeartbeat: false, wantGCEnabled: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := datastorecfg.DefaultDatastoreConfig()
			cfg.Engine = Engine
			cfg.ReadOnly = tc.readOnly
			cfg.EnableRevisionHeartbeat = tc.heartbeatFlag

			opts, err := primaryDatastoreOptionsFromConfig(*cfg)
			require.NoError(t, err)

			pgCfg, err := generateConfig(opts)
			require.NoError(t, err)

			require.Equal(t, tc.wantHeartbeat, pgCfg.revisionHeartbeatEnabled, "revision heartbeat enabled")
			require.Equal(t, tc.wantGCEnabled, pgCfg.gcEnabled, "garbage collection enabled")
		})
	}
}

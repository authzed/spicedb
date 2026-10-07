package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/pkg/cmd/util"
)

func TestParseObjectAffinityRoutingMode(t *testing.T) {
	tcs := map[string]struct {
		input    string
		expected ObjectAffinityRoutingMode
		wantErr  bool
	}{
		"empty is disabled": {input: "", expected: ObjectAffinityRoutingModeDisabled},
		"disabled":          {input: "disabled", expected: ObjectAffinityRoutingModeDisabled},
		"enabled":           {input: "enabled", expected: ObjectAffinityRoutingModeEnabled},
		"unknown":           {input: "sometimes", wantErr: true},
		"wrong case":        {input: "Enabled", wantErr: true},
	}
	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			mode, err := ParseObjectAffinityRoutingMode(tc.input)
			if tc.wantErr {
				require.ErrorContains(t, err, "invalid object affinity routing mode")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, mode)
		})
	}
}

func TestCompleteRejectsInvalidObjectAffinityRoutingMode(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, 0)
	require.NoError(t, err)

	c := ConfigWithOptions(&Config{
		GRPCServer: util.GRPCServerConfig{Network: util.BufferedNetwork},
	}, WithPresharedSecureKey("psk"), WithDatastore(ds), WithEnableMemoryProtectionMiddleware(false),
		WithExperimentalObjectAffinityRouting("sometimes"))
	_, err = c.Complete(t.Context())
	require.ErrorContains(t, err, "invalid object affinity routing mode")
}

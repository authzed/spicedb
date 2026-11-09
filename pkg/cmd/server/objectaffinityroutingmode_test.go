package server

import (
	"math"
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

func TestValidateObjectSpread(t *testing.T) {
	const (
		disabledWarning = "experimental-dispatch-object-spread-share has no effect because experimental-object-affinity-routing is disabled"
		spreadWarning   = "experimental-dispatch-object-spread-share has no effect because experimental-dispatch-object-spread is less than 2"
	)
	for _, tc := range []struct {
		name         string
		mode         ObjectAffinityRoutingMode
		share        float64
		spread       uint8
		factor       float64
		wantErr      string
		wantWarnings []string
	}{
		{name: "defaults", mode: ObjectAffinityRoutingModeDisabled, spread: 2, factor: 1.5},
		{name: "spread on", mode: ObjectAffinityRoutingModeEnabled, share: 0.5, spread: 2, factor: 1.5},
		{name: "factor 0", mode: ObjectAffinityRoutingModeEnabled, share: 0.5, spread: 2},
		{name: "spread 1 without share", mode: ObjectAffinityRoutingModeEnabled, spread: 1, factor: 1.5},
		{name: "negative share", mode: ObjectAffinityRoutingModeEnabled, share: -0.1, spread: 2, factor: 1.5, wantErr: "experimental-dispatch-object-spread-share must not be negative"},
		{name: "NaN share", mode: ObjectAffinityRoutingModeEnabled, share: math.NaN(), spread: 2, factor: 1.5, wantErr: "experimental-dispatch-object-spread-share must not be negative"},
		{name: "negative factor", mode: ObjectAffinityRoutingModeEnabled, share: 0.5, spread: 2, factor: -1, wantErr: "experimental-dispatch-object-spread-latency-factor must not be negative"},
		{name: "negative factor without share", mode: ObjectAffinityRoutingModeDisabled, spread: 2, factor: -1, wantErr: "experimental-dispatch-object-spread-latency-factor must not be negative"},
		{name: "share with routing disabled", mode: ObjectAffinityRoutingModeDisabled, share: 0.5, spread: 2, factor: 1.5, wantWarnings: []string{disabledWarning}},
		{name: "share with spread 1", mode: ObjectAffinityRoutingModeEnabled, share: 0.5, spread: 1, factor: 1.5, wantWarnings: []string{spreadWarning}},
		{name: "share with spread 0 and routing disabled", mode: ObjectAffinityRoutingModeDisabled, share: 0.5, factor: 1.5, wantWarnings: []string{disabledWarning, spreadWarning}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings, err := validateObjectSpread(tc.mode, tc.share, tc.spread, tc.factor)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantWarnings, warnings)
		})
	}
}

func TestCompleteRejectsNegativeObjectSpread(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opt     ConfigOption
		wantErr string
	}{
		{"share", WithDispatchObjectSpreadShare(-1), "experimental-dispatch-object-spread-share must not be negative"},
		{"latency factor", WithDispatchObjectSpreadLatencyFactor(-1), "experimental-dispatch-object-spread-latency-factor must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, 0)
			require.NoError(t, err)

			c := ConfigWithOptions(&Config{
				GRPCServer: util.GRPCServerConfig{Network: util.BufferedNetwork},
			}, WithPresharedSecureKey("psk"), WithDatastore(ds), WithEnableMemoryProtectionMiddleware(false), tc.opt)
			_, err = c.Complete(t.Context())
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

package crdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		options  []Option
		validate func(t *testing.T, config crdbOptions)
	}{
		{
			name:    "default jitter configuration",
			options: []Option{},
			validate: func(t *testing.T, config crdbOptions) {
				require.NotNil(t, config.readPoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 30*time.Minute, *config.readPoolOpts.ConnMaxLifetimeJitter)

				require.NotNil(t, config.writePoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 30*time.Minute, *config.writePoolOpts.ConnMaxLifetimeJitter)
			},
		},
		{
			name: "explicit jitter values preserved",
			options: []Option{
				ReadConnMaxLifetimeJitter(10 * time.Minute),
				WriteConnMaxLifetimeJitter(15 * time.Minute),
			},
			validate: func(t *testing.T, config crdbOptions) {
				// Should preserve explicitly set values
				require.NotNil(t, config.readPoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 10*time.Minute, *config.readPoolOpts.ConnMaxLifetimeJitter)

				require.NotNil(t, config.writePoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 15*time.Minute, *config.writePoolOpts.ConnMaxLifetimeJitter)
			},
		},
		{
			name: "zeros values applies defaults",
			options: []Option{
				// This simulates what happens when pkg/cmd/datastore passes zero values
				// from ConnPoolConfig.MaxLifetimeJitter (which defaults to 0)
				ReadConnMaxLifetimeJitter(time.Duration(0)),
				WriteConnMaxLifetimeJitter(time.Duration(0)),
			},
			validate: func(t *testing.T, config crdbOptions) {
				require.NotNil(t, config.readPoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 30*time.Minute, *config.readPoolOpts.ConnMaxLifetimeJitter)

				require.NotNil(t, config.writePoolOpts.ConnMaxLifetimeJitter)
				require.Equal(t, 30*time.Minute, *config.writePoolOpts.ConnMaxLifetimeJitter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := generateConfig(tt.options)
			require.NoError(t, err)
			tt.validate(t, config)
		})
	}
}

func TestDefaultAcquireTimeoutIsZero(t *testing.T) {
	config, err := generateConfig(nil)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), config.acquireTimeout,
		"default acquireTimeout should be 0 (disabled) since cancel handler keeps pool healthy")
}

func TestAcquireTimeoutWithCancelDraining(t *testing.T) {
	tests := []struct {
		name            string
		options         []Option
		expectedTimeout time.Duration
	}{
		{
			name:            "cancel draining enabled (default): timeout stays 0",
			options:         []Option{},
			expectedTimeout: 0,
		},
		{
			name:            "cancel draining disabled: timeout restored to 30ms",
			options:         []Option{WithExperimentalCancelDraining(false)},
			expectedTimeout: 30 * time.Millisecond,
		},
		{
			name:            "cancel draining disabled with explicit timeout: explicit value wins",
			options:         []Option{WithExperimentalCancelDraining(false), WithAcquireTimeout(100 * time.Millisecond)},
			expectedTimeout: 100 * time.Millisecond,
		},
		{
			name: "cancel draining disabled with explicit zero: treated same as unset, gets 30ms",
			// There is no sentinel to distinguish "explicitly set to 0" from "default 0",
			// so disabling cancel draining always restores 30ms when timeout is 0.
			options:         []Option{WithExperimentalCancelDraining(false), WithAcquireTimeout(0)},
			expectedTimeout: 30 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := generateConfig(tt.options)
			require.NoError(t, err)
			require.Equal(t, tt.expectedTimeout, config.acquireTimeout)
		})
	}
}

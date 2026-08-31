package spanner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGCWindowOption(t *testing.T) {
	tcs := []struct {
		name             string
		options          []Option
		expectedGCWindow time.Duration
		expectedError    string
	}{
		{
			name:             "defaults to the change stream retention",
			expectedGCWindow: defaultChangeStreamRetention,
		},
		{
			name:             "a window within the change stream retention is honored",
			options:          []Option{GCWindow(1 * time.Hour)},
			expectedGCWindow: 1 * time.Hour,
		},
		{
			name:             "a window beyond the change stream retention is capped",
			options:          []Option{GCWindow(48 * time.Hour)},
			expectedGCWindow: defaultChangeStreamRetention,
		},
		{
			name:          "quantization at least as large as the window is rejected",
			options:       []Option{GCWindow(time.Second), RevisionQuantization(time.Second)},
			expectedError: "revision quantization (1s) must be less than (1s)",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			config, err := generateConfig(tc.options)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedGCWindow, config.gcWindow)
		})
	}
}

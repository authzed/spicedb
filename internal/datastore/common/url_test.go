package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsIDFromURL(t *testing.T) {
	tcs := []struct {
		dsURL         string
		expected      string
		expectedError string
	}{
		{
			"",
			"",
			"datastore URL is empty",
		},
		{
			"postgres://user:password@localhost:5432/dbname",
			"localhost:5432/dbname",
			"",
		},
		{
			"mysql://user:password@localhost:3306/dbname",
			"localhost:3306/dbname",
			"",
		},
		{
			"mysql://user:password@localhost:3306/dbname?sensitive=secret",
			"localhost:3306/dbname",
			"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.dsURL, func(t *testing.T) {
			result, err := MetricsIDFromURL(tc.dsURL)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

package releases

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type testCase struct {
	name           string
	version        string
	releaseVersion string
	expectedState  SoftwareUpdateState
}

func TestCheckIsLatestVersion(t *testing.T) {
	testCases := []testCase{
		{"up to date", "v1.5.6", "v1.5.6", UpToDate},
		{"ahead of version", "v1.7.0", "v1.5.6", UpToDate},
		{"new version", "v1.5.6", "v1.5.7", UpdateAvailable},
		{"new minor version", "v1.5.6", "v1.6.0", UpdateAvailable},
		{"new major version", "v1.5.6", "v2.0.0", UpdateAvailable},
		{"invalid version", "abcdef", "v1.6.0", UnreleasedVersion},
		{"empty version", "", "v1.6.0", UnreleasedVersion},
		{"invalid release version", "v1.5.6", "abderf", Unknown},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			state, _, _, _ := CheckIsLatestVersion(context.Background(), func() (string, error) {
				return tc.version, nil
			}, func(ctx context.Context) (*Release, error) {
				return &Release{
					Version: tc.releaseVersion,
				}, nil
			})
			require.Equal(t, tc.expectedState, state)
		})
	}
}

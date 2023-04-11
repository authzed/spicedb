package crdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseVersionString(t *testing.T) {
	testCases := []struct {
		versionStr  string
		expected    crdbVersion
		expectError bool
	}{
		{
			"CockroachDB CCL v22.2.0-rc.3 (aarch64-unknown-linux-gnu, built 2022/11/21 17:44:44, go1.19.1)",
			crdbVersion{0, 22, 2, 0},
			false,
		},
		{
			"CockroachDB CCL v22.2.0 (aarch64-unknown-linux-gnu, built 2022/12/05 17:11:12, go1.19.1)",
			crdbVersion{0, 22, 2, 0},
			false,
		},
		{
			"CockroachDB CCL v22.1.4 (x86_64-pc-linux-gnu, built 2022/07/19 17:09:48, go1.17.11)",
			crdbVersion{0, 22, 1, 4},
			false,
		},
		{
			"CockroachDB CCL",
			crdbVersion{},
			true,
		},
		{
			"CockroachDB CCL va.2.0-rc.3 (aarch64-unknown-linux-gnu, built 2022/11/21 17:44:44, go1.19.1)",
			crdbVersion{},
			true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.versionStr, func(t *testing.T) {
			var version crdbVersion
			err := parseVersionStringInto(tc.versionStr, &version)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, version)
			}
		})
	}
}

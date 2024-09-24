package compiler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefixedPath(t *testing.T) {
	fooPrefix := "foo"
	barPrefix := "bar"
	noPrefix := ""

	testCases := []struct {
		input         string
		prefix        *string
		expectedError bool
		expected      string
	}{
		{"bar", &fooPrefix, false, "foo/bar"},
		{"bar", &barPrefix, false, "bar/bar"},
		{"bar", &noPrefix, false, "bar"},
		{"foo/bar", &fooPrefix, false, "foo/bar"},
		{"foo/bar", &barPrefix, false, "foo/bar"},
		{"foo/bar", &noPrefix, false, "foo/bar"},
		{"bar", nil, true, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require := require.New(t)

			tctx := translationContext{
				objectTypePrefix: tc.prefix,
			}
			output, err := tctx.prefixedPath(tc.input)
			if tc.expectedError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tc.expected, output)
			}
		})
	}
}

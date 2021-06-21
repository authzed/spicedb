package validationfile

import (
	"testing"

	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestValidationString(t *testing.T) {
	type testCase struct {
		name            string
		input           string
		expectedSubject string
		expectedONRs    []string
	}

	tests := []testCase{
		{
			"empty",
			"",
			"",
			[]string{},
		},
		{
			"basic",
			"[tenant/user:someuser#...] is <tenant/document:example#viewer>",
			"tenant/user:someuser#...",
			[]string{"tenant/document:example#viewer"},
		},
		{
			"missing onrs",
			"[tenant/user:someuser#...]",
			"tenant/user:someuser#...",
			[]string{},
		},
		{
			"missing subject",
			"is <tenant/document:example#viewer>",
			"",
			[]string{"tenant/document:example#viewer"},
		},
		{
			"multiple onrs",
			"[tenant/user:someuser#...] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser#...",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
		{
			"bad subject",
			"[tenant/user:someuser#... is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
		{
			"bad parse",
			"[tenant/user:someuser...] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			vs := ValidationString(tc.input)

			subject, _ := vs.Subject()
			if tc.expectedSubject == "" {
				require.Nil(subject)
			} else {
				require.Equal(tc.expectedSubject, tuple.StringONR(subject))
			}

			foundONRStrings := []string{}
			onrs, _ := vs.ONRS()
			for _, onr := range onrs {
				foundONRStrings = append(foundONRStrings, tuple.StringONR(onr))
			}

			require.Equal(tc.expectedONRs, foundONRStrings)
		})
	}
}

func TestAssertions(t *testing.T) {
	type testCase struct {
		name     string
		input    string
		expected bool
	}

	tests := []testCase{
		{
			"empty",
			"",
			false,
		},
		{
			"empty",
			"foo:bar#baz@groo:grar#graz",
			true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			a := Assertions{AssertTrue: []string{tc.input}}

			foundRelationships := []string{}
			tpls, _ := a.AssertTrueRelationships()
			for _, tpl := range tpls {
				foundRelationships = append(foundRelationships, tuple.String(tpl))
			}

			if tc.expected {
				require.Equal([]string{tc.input}, foundRelationships)
			} else {
				require.Equal(0, len(foundRelationships))
			}
		})
	}
}

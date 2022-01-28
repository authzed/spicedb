package validationfile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
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
			"tenant/user:someuser",
			[]string{"tenant/document:example#viewer"},
		},
		{
			"missing onrs",
			"[tenant/user:someuser#...]",
			"tenant/user:someuser",
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
			"tenant/user:someuser",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
		{
			"ellided ellipsis",
			"[tenant/user:someuser] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
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
			"[tenant/user:someuser:asdsad] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
		{
			"subject with exclusions",
			"[tenant/user:someuser#... - {test/user:1,test/user:2}] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
		{
			"subject with bad exclusions",
			"[tenant/user:someuser#... - {te1,test/user:2}] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			vs := ValidationString(tc.input)

			subject, err := vs.Subject()

			if tc.expectedSubject == "" {
				require.Nil(subject)
			} else {
				require.Nil(err)
				require.Equal(tc.expectedSubject, tuple.StringONR(subject.Subject))
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
		name                 string
		input                string
		expectedRelationship string
	}

	tests := []testCase{
		{
			"empty",
			"",
			"",
		},
		{
			"empty",
			"foos:bar#bazzy@groo:grar#graz",
			"foos:bar#bazzy@groo:grar#graz",
		},
		{
			"empty",
			"foos:bar#bazzy@groo:grar",
			"foos:bar#bazzy@groo:grar",
		},
		{
			"empty",
			"foos:bar#bazzy@groo:grar#...",
			"foos:bar#bazzy@groo:grar",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			a := Assertions{AssertTrue: []Assertion{{tc.input, 0, 0}}}

			foundRelationships := []string{}
			assertions, _ := a.AssertTrueRelationships()
			for _, assertion := range assertions {
				foundRelationships = append(foundRelationships, tuple.String(assertion.Relationship))
			}

			if tc.expectedRelationship != "" {
				require.Equal([]string{tc.expectedRelationship}, foundRelationships)
			} else {
				require.Equal(0, len(foundRelationships))
			}
		})
	}
}

func TestParseAssertionsBlock(t *testing.T) {
	type testCase struct {
		name               string
		input              string
		expectedError      string
		expectedAssertions Assertions
	}

	tests := []testCase{
		{
			"empty",
			"",
			"",
			Assertions{},
		},
		{
			"with one assertion",
			`assertTrue:
- document:foo#view@user:someone`,
			"",
			Assertions{
				AssertTrue: []Assertion{
					{
						"document:foo#view@user:someone", 2, 3,
					},
				},
			},
		},
		{
			"with one assertion per section",
			`assertTrue:
- document:foo#view@user:someone
assertFalse:
- document:foo#write@user:someone`,
			"",
			Assertions{
				AssertTrue: []Assertion{
					{
						"document:foo#view@user:someone", 2, 3,
					},
				},
				AssertFalse: []Assertion{
					{
						"document:foo#write@user:someone", 4, 3,
					},
				},
			},
		},
		{
			"with invalid yaml",
			`assertTrue: somethinginvalid
- document:foo#view@user:someone`,
			"yaml: line 1: did not find expected key",
			Assertions{},
		},
		{
			"with garbage",
			`assertTrue:
  - document:firstdoc#view@user:tom
  - document:firstdoc#view@user:fred
  - document:seconddoc#view@user:tom
assertFalse: garbage
  - document:seconddoc#view@user:fred`,
			"unexpected key `garbage - document:seconddoc#view@user:fred` on line 5",
			Assertions{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			assertions, err := ParseAssertionsBlock([]byte(tc.input))
			if tc.expectedError != "" {
				require.Nil(assertions)
				require.Equal(err.Error(), tc.expectedError)
			} else {
				require.NoError(err)
				require.Equal(tc.expectedAssertions, *assertions)
			}
		})
	}
}

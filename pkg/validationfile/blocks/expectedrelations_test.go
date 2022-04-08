package blocks

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

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

func TestParseExpectedRelations(t *testing.T) {
	type testCase struct {
		name          string
		contents      string
		expectedError string
		expectedKeys  int
	}

	tests := []testCase{
		{
			"empty",
			"",
			"",
			0,
		},
		{
			"not a map",
			`- hi
- hello `,
			`cannot unmarshal !!seq into blocks.ValidationMap`,
			0,
		},
		{
			"valid",
			`document:firstdoc#view:
- "[user:tom] is <document:firstdoc#writer>"
- "[user:fred] is <document:firstdoc#reader>/<document:firstdoc#writer>"
document:seconddoc#view:
- "[user:tom] is <document:seconddoc#reader>"`,
			"",
			2,
		},
		{
			"invalid key",
			`document:firstdocview:
- "[user:tom] is <document:firstdoc#writer>"`,
			"could not parse document:firstdocview",
			0,
		},
		{
			"invalid subject",
			`document:firstdoc#view:
- "[usertom] is <document:firstdoc#writer>"`,
			"invalid subject: `usertom`",
			0,
		},
		{
			"invalid resource",
			`document:firstdoc#view:
- "[user:tom] is <document:firstdocwriter>"`,
			"invalid resource and relation: `document:firstdocwriter`",
			0,
		},
		{
			"invalid multi-resource",
			`document:firstdoc#view:
- "[user:tom] is <document:firstdoc#writer/document:firstdoc#reader>"`,
			"invalid resource and relation: `document:firstdoc#writer/document:firstdoc#reader`",
			0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			per := ParsedExpectedRelations{}
			err := yamlv3.Unmarshal([]byte(tc.contents), &per)
			if tc.expectedError != "" {
				require.Contains(err.Error(), tc.expectedError)
			} else {
				require.NoError(err)
				require.Equal(tc.expectedKeys, len(per.ValidationMap))
			}
		})
	}
}

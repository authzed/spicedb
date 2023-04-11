package blocks

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestValidationString(t *testing.T) {
	type testCase struct {
		name               string
		input              string
		expectedSubject    string
		isSubjectCaveated  bool
		expectedONRs       []string
		expectedExceptions []string
	}

	tests := []testCase{
		{
			"empty",
			"",
			"",
			false,
			[]string{},
			nil,
		},
		{
			"basic",
			"[tenant/user:someuser#...] is <tenant/document:example#viewer>",
			"tenant/user:someuser",
			false,
			[]string{"tenant/document:example#viewer"},
			nil,
		},
		{
			"missing onrs",
			"[tenant/user:someuser#...]",
			"tenant/user:someuser",
			false,
			[]string{},
			nil,
		},
		{
			"missing subject",
			"is <tenant/document:example#viewer>",
			"",
			false,
			[]string{"tenant/document:example#viewer"},
			nil,
		},
		{
			"multiple onrs",
			"[tenant/user:someuser#...] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			nil,
		},
		{
			"ellided ellipsis",
			"[tenant/user:someuser] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			nil,
		},
		{
			"bad subject",
			"[tenant/user:someuser#... is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			nil,
		},
		{
			"bad parse",
			"[tenant/user:someuser:asdsad] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			nil,
		},
		{
			"subject with exclusions",
			"[tenant/user:someuser#... - {test/user:1,test/user:2}] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			[]string{"test/user:1", "test/user:2"},
		},
		{
			"subject with bad exclusions",
			"[tenant/user:someuser#... - {te1,test/user:2}] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"",
			false,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			nil,
		},
		{
			"caveated subject with exclusions",
			"[tenant/user:someuser[...] - {test/user:1[...],test/user:2}] is <tenant/document:example#viewer>/<tenant/document:example#builder>",
			"tenant/user:someuser",
			true,
			[]string{"tenant/document:example#viewer", "tenant/document:example#builder"},
			[]string{"test/user:1[...]", "test/user:2"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			vs := ValidationString(tc.input)

			subject, err := vs.Subject()

			if tc.expectedSubject == "" {
				require.Nil(subject)
			} else {
				require.Nil(err)
				require.Equal(tc.expectedSubject, tuple.StringONR(subject.Subject.Subject))
				require.Equal(tc.isSubjectCaveated, subject.Subject.IsCaveated)
				require.Equal(len(tc.expectedExceptions), len(subject.Exceptions))

				if len(tc.expectedExceptions) > 0 {
					sort.Strings(tc.expectedExceptions)

					exceptionStrings := make([]string, 0, len(subject.Exceptions))
					for _, exception := range subject.Exceptions {
						exceptionString := tuple.StringONR(exception.Subject)
						if exception.IsCaveated {
							exceptionString += "[...]"
						}
						exceptionStrings = append(exceptionStrings, exceptionString)
					}

					require.Equal(tc.expectedExceptions, exceptionStrings)
				}
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
			"invalid caveated subject",
			`document:firstdoc#view:
- "[user:tom[df]] is <document:firstdoc#writer>"`,
			"invalid subject: `user:tom[df]`",
			0,
		},
		{
			"invalid exception subject",
			`document:firstdoc#view:
- "[user:*-{}] is <document:firstdoc#writer>"`,
			"invalid subject: `user:*-{}`",
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
		{
			"valid with caveats and exceptions",
			`document:firstdoc#view:
- "[user:tom[...]] is <document:firstdoc#writer>"
- "[user:fred - {user:a, user:b}] is <document:firstdoc#reader>/<document:firstdoc#writer>"
document:seconddoc#view:
- "[user:*[...] - {user:a, user:b}] is <document:seconddoc#reader>"`,
			"",
			2,
		},
		{
			"valid with caveats and caveated exceptions",
			`document:firstdoc#view:
- "[user:tom[...]] is <document:firstdoc#writer>"
- "[user:fred - {user:a[...], user:b}] is <document:firstdoc#reader>/<document:firstdoc#writer>"
document:seconddoc#view:
- "[user:*[...] - {user:a, user:b[...]}] is <document:seconddoc#reader>"`,
			"",
			2,
		},
	}

	for _, tc := range tests {
		tc := tc
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

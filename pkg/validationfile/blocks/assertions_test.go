package blocks

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestParseAssertions(t *testing.T) {
	type testCase struct {
		name               string
		contents           string
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
						"document:foo#view@user:someone",
						tuple.MustParse("document:foo#view@user:someone"),
						nil,
						spiceerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
				},
				SourcePosition: spiceerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
			},
		},
		{
			"with one assertion per section",
			`assertTrue:
- document:foo#view@user:someone
- document:bar#view@user:sometwo
assertFalse:
- document:foo#write@user:someone`,
			"",
			Assertions{
				AssertTrue: []Assertion{
					{
						"document:foo#view@user:someone",
						tuple.MustParse("document:foo#view@user:someone"),
						nil,
						spiceerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
					{
						"document:bar#view@user:sometwo",
						tuple.MustParse("document:bar#view@user:sometwo"),
						nil,
						spiceerrors.SourcePosition{LineNumber: 3, ColumnPosition: 3},
					},
				},
				AssertFalse: []Assertion{
					{
						"document:foo#write@user:someone",
						tuple.MustParse("document:foo#write@user:someone"),
						nil,
						spiceerrors.SourcePosition{LineNumber: 5, ColumnPosition: 3},
					},
				},
				SourcePosition: spiceerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
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
			"unexpected value `garbage`",
			Assertions{},
		},
		{
			"with one assertion with context",
			`assertTrue:
- 'document:foo#view@user:someone with {"foo": "bar"}'`,
			"",
			Assertions{
				AssertTrue: []Assertion{
					{
						`document:foo#view@user:someone with {"foo": "bar"}`,
						tuple.MustParse("document:foo#view@user:someone"),
						map[string]any{
							"foo": "bar",
						},
						spiceerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
				},
				SourcePosition: spiceerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
			},
		},
		{
			"with one assertion with invalid context",
			`assertTrue:
- 'document:foo#view@user:someone with {"foo: "bar"}'`,
			"error parsing caveat context in assertion",
			Assertions{},
		},
		{
			"with one spaced assertion with context",
			`assertTrue:
- '   document:foo#view@user:someone   with   {"foo":     "bar"}   '`,
			"",
			Assertions{
				AssertTrue: []Assertion{
					{
						`   document:foo#view@user:someone   with   {"foo":     "bar"}   `,
						tuple.MustParse("document:foo#view@user:someone"),
						map[string]any{
							"foo": "bar",
						},
						spiceerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
				},
				SourcePosition: spiceerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			a := Assertions{}
			err := yamlv3.Unmarshal([]byte(tc.contents), &a)
			if tc.expectedError != "" {
				require.Contains(err.Error(), tc.expectedError)
			} else {
				require.NoError(err)
				require.Equal(tc.expectedAssertions, a)
			}
		})
	}
}

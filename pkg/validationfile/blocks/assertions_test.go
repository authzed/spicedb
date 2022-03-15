package blocks

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/commonerrors"
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
						tuple.MustToRelationship(tuple.MustParse("document:foo#view@user:someone")),
						commonerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
				},
				SourcePosition: commonerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
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
						tuple.MustToRelationship(tuple.MustParse("document:foo#view@user:someone")),
						commonerrors.SourcePosition{LineNumber: 2, ColumnPosition: 3},
					},
					{
						"document:bar#view@user:sometwo",
						tuple.MustToRelationship(tuple.MustParse("document:bar#view@user:sometwo")),
						commonerrors.SourcePosition{LineNumber: 3, ColumnPosition: 3},
					},
				},
				AssertFalse: []Assertion{
					{
						"document:foo#write@user:someone",
						tuple.MustToRelationship(tuple.MustParse("document:foo#write@user:someone")),
						commonerrors.SourcePosition{LineNumber: 5, ColumnPosition: 3},
					},
				},
				SourcePosition: commonerrors.SourcePosition{LineNumber: 1, ColumnPosition: 1},
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			a := Assertions{}
			err := yamlv3.Unmarshal([]byte(tc.contents), &a)
			if tc.expectedError != "" {
				require.Equal(tc.expectedError, err.Error())
			} else {
				require.NoError(err)
				require.Equal(tc.expectedAssertions, a)
			}
		})
	}
}

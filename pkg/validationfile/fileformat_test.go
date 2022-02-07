package validationfile

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeValidationFile(t *testing.T) {
	tests := []struct {
		name                     string
		contents                 string
		expectedError            string
		expectedRelCount         int
		expectedAssertTrueCount  int
		expectedAssertFalseCount int
		expectedValidationCount  int
	}{
		{
			name:                     "empty",
			contents:                 "",
			expectedError:            "",
			expectedRelCount:         0,
			expectedAssertTrueCount:  0,
			expectedAssertFalseCount: 0,
			expectedValidationCount:  0,
		},
		{
			name: "invalid schema",
			contents: `schema:
foo:
	- asdasd`,
			expectedError:            "yaml: line 3: found character that cannot start any token",
			expectedRelCount:         0,
			expectedAssertTrueCount:  0,
			expectedAssertFalseCount: 0,
			expectedValidationCount:  0,
		},
		{
			name: "valid",
			contents: `
schema: >-
  definition user {}
  definition document {
      relation writer: user
      relation reader: user
      permission edit = writer
      permission view = reader + edit
  }

relationships: >-
  document:firstdoc#writer@user:tom

  document:firstdoc#reader@user:fred

  document:seconddoc#reader@user:tom

assertions:
  assertTrue:
    - document:firstdoc#view@user:tom
    - document:firstdoc#view@user:fred
    - document:seconddoc#view@user:tom
  assertFalse:
    - document:seconddoc#view@user:fred

validation:
  document:firstdoc#view:
    - "[user:tom] is <document:firstdoc#writer>"
    - "[user:fred] is <document:firstdoc#reader>"
  document:seconddoc#view:
    - "[user:tom] is <document:seconddoc#reader>"
`,
			expectedError:            "",
			expectedRelCount:         3,
			expectedAssertTrueCount:  3,
			expectedAssertFalseCount: 1,
			expectedValidationCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := DecodeValidationFile([]byte(tt.contents))
			if tt.expectedError != "" {
				require.NotNil(t, err)
				require.Equal(t, err.Error(), tt.expectedError)
			} else {
				require.Nil(t, err)

				require.NotNil(t, decoded)
				require.Equal(t, len(decoded.Relationships), tt.expectedRelCount)

				at, err := decoded.Assertions.AssertTrueRelationships()
				require.Nil(t, err)

				af, err := decoded.Assertions.AssertFalseRelationships()
				require.Nil(t, err)

				require.Equal(t, len(at), tt.expectedAssertTrueCount)
				require.Equal(t, len(af), tt.expectedAssertFalseCount)
				require.Equal(t, len(decoded.ExpectedRelations), tt.expectedValidationCount)
			}
		})
	}
}

func TestDecodeRelationshipsErrorLineNumber(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`
schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom

  document:firstdoc#readeruser:fred
`))

	var errWithSource ErrorWithSource
	require.True(t, errors.As(err, &errWithSource))

	require.Equal(t, err.Error(), "error parsing relationship #1: document:firstdoc#readeruser:fred")
	require.Equal(t, uint32(8), errWithSource.LineNumber)
}

func TestDecodeAssertionsErrorLineNumber(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`
schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom

assertions:
  assertTrues:
    - document:firstdoc#view@user:tom
    - document:firstdoc#view@user:fred
    - document:seconddoc#view@user:tom
  assertFalse:
    - document:seconddoc#view@user:fred
`))

	var errWithSource ErrorWithSource
	require.True(t, errors.As(err, &errWithSource))

	require.Equal(t, err.Error(), "unexpected key `assertTrues` on line 9")
	require.Equal(t, uint32(9), errWithSource.LineNumber)
}

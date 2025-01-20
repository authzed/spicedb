package validationfile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/spiceerrors"
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := DecodeValidationFile([]byte(tt.contents))
			if tt.expectedError != "" {
				require.NotNil(t, err)
				require.Equal(t, err.Error(), tt.expectedError)
			} else {
				require.Nil(t, err)

				require.NotNil(t, decoded)
				require.Equal(t, len(decoded.Relationships.Relationships), tt.expectedRelCount)

				require.Equal(t, len(decoded.Assertions.AssertTrue), tt.expectedAssertTrueCount)
				require.Equal(t, len(decoded.Assertions.AssertFalse), tt.expectedAssertFalseCount)
				require.Equal(t, len(decoded.ExpectedRelations.ValidationMap), tt.expectedValidationCount)
			}
		})
	}
}

func TestDecodeValidationFileWithoutSchema(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`schemaFile: >-
  someschemafilehere.zed

relationships: >-
  document:firstdoc#writer@user:tom
`))
	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.False(t, ok)
	require.Nil(t, errWithSource)
}

func TestDecodeRelationshipsErrorLineNumber(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`schema: >-
  definition user {}

relationships: >-
  document:firstdocwriter@user:tom

  document:firstdoc#reader#user:fred
`))

	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.True(t, ok)

	require.Equal(t, err.Error(), "error parsing relationship `document:firstdocwriter@user:tom`: invalid relationship string")
	require.Equal(t, uint64(5), errWithSource.LineNumber)
}

func TestDecodeRelationshipsErrorLineNumberLater(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom

  document:firstdoc#readeruser:fred
`))

	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.True(t, ok)

	require.Equal(t, err.Error(), "error parsing relationship `document:firstdoc#readeruser:fred`: invalid relationship string")
	require.Equal(t, uint64(7), errWithSource.LineNumber)
}

func TestDecodeRelationshipsErrorLineNumberEventLater(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom1

  document:firstdoc#writer@user:tom2

  document:firstdoc#writer@user:tom3

  document:firstdoc#writer@user:tom4

  document:firstdoc#readeruser:fred
`))

	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.True(t, ok)

	require.Equal(t, err.Error(), "error parsing relationship `document:firstdoc#readeruser:fred`: invalid relationship string")
	require.Equal(t, uint64(13), errWithSource.LineNumber)
}

func TestDecodeAssertionsErrorLineNumber(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`
schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom

assertions:
  assertTrue: asdkjhasd
    - document:firstdoc#view@user:tom
    - document:firstdoc#view@user:fred
    - document:seconddoc#view@user:tom
  assertFalse:
    - document:seconddoc#view@user:fred
`))

	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.True(t, ok)

	require.Equal(t, err.Error(), "unexpected value `asdkjha`")
	require.Equal(t, uint64(9), errWithSource.LineNumber)
}

func TestDecodeAssertionsErrorLineNumberSmallerToken(t *testing.T) {
	_, err := DecodeValidationFile([]byte(`
schema: >-
  definition user {}

relationships: >-
  document:firstdoc#writer@user:tom

assertions:
  assertTrue: asdk
    - document:firstdoc#view@user:tom
    - document:firstdoc#view@user:fred
    - document:seconddoc#view@user:tom
  assertFalse:
    - document:seconddoc#view@user:fred
`))

	errWithSource, ok := spiceerrors.AsWithSourceError(err)
	require.True(t, ok)

	require.Equal(t, err.Error(), "unexpected value `asdk`")
	require.Equal(t, uint64(9), errWithSource.LineNumber)
}

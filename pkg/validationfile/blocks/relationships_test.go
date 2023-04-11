package blocks

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"
)

func TestParseRelationships(t *testing.T) {
	tests := []struct {
		name             string
		contents         string
		expectedError    string
		expectedRelCount int
	}{
		{
			name:             "empty",
			contents:         "",
			expectedError:    "",
			expectedRelCount: 0,
		},
		{
			name:             "invalid yaml",
			contents:         `asdkjsad:`,
			expectedError:    "cannot unmarshal !!map into string",
			expectedRelCount: 0,
		},
		{
			name:             "invalid relationship",
			contents:         `document:firstviewer@user:1`,
			expectedError:    "error parsing relationship `document:firstviewer@user:1`",
			expectedRelCount: 0,
		},
		{
			name: "valid",
			contents: `document:first#viewer@user:1

// this is a comment

document:second#viewer@user:1`,
			expectedError:    "",
			expectedRelCount: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			pr := ParsedRelationships{}
			err := yamlv3.Unmarshal([]byte(tt.contents), &pr)
			if tt.expectedError != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.expectedRelCount, len(pr.Relationships))
			}
		})
	}
}

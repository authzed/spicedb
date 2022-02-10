package blocks

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"
)

func TestParseSchema(t *testing.T) {
	tests := []struct {
		name             string
		contents         string
		expectedError    string
		expectedDefCount int
	}{
		{
			name:             "empty",
			contents:         "",
			expectedError:    "",
			expectedDefCount: 0,
		},
		{
			name:             "invalid schema",
			contents:         "asdasd",
			expectedError:    "error when parsing schema: Unexpected token at root level: TokenTypeIdentifier",
			expectedDefCount: 0,
		},
		{
			name:             "valid schema",
			contents:         "definition foo/user {}",
			expectedError:    "",
			expectedDefCount: 1,
		},
		{
			name:             "valid schema no prefix",
			contents:         "definition user {}",
			expectedError:    "",
			expectedDefCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := ParsedSchema{}
			err := yamlv3.Unmarshal([]byte(tt.contents), &ps)
			if tt.expectedError != "" {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.Nil(t, err)
				require.Equal(t, tt.expectedDefCount, len(ps.Definitions))
				require.Equal(t, tt.contents, ps.Schema)
			}
		})
	}
}

package validationfile

import (
	"testing"

	"github.com/stretchr/testify/require"
	yamlv3 "gopkg.in/yaml.v3"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			schemaWithPosition := blocks.SchemaWithPosition{}
			err := yamlv3.Unmarshal([]byte(tt.contents), &schemaWithPosition)
			require.NoError(t, err)

			compiled, err := CompileSchema(schemaWithPosition, caveattypes.Default.TypeSet)
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				if tt.expectedDefCount > 0 {
					require.NotNil(t, compiled)
					require.Len(t, compiled.OrderedDefinitions, tt.expectedDefCount)
					require.Equal(t, tt.contents, schemaWithPosition.Schema)
				}
			}
		})
	}
}

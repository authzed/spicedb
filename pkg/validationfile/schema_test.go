package validationfile

import (
	"testing"

	"github.com/stretchr/testify/require"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
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
			inputSchema := compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tt.contents,
			}
			compiled, err := CompileSchema(inputSchema, caveattypes.Default.TypeSet)
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				if tt.expectedDefCount > 0 {
					require.NotNil(t, compiled)
					require.Len(t, compiled.OrderedDefinitions, tt.expectedDefCount)
				}
			}
		})
	}
}

package compiler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/composableschemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
)

func TestPositionToAstNode(t *testing.T) {
	tcs := []struct {
		name          string
		schema        string
		line          int
		column        int
		expected      []dslshape.NodeType
		expectedError string
	}{
		{
			name:          "empty schema",
			schema:        "",
			line:          1,
			column:        1,
			expected:      nil,
			expectedError: "unknown line 1 in source file",
		},
		{
			name:     "single def",
			schema:   `definition user {}`,
			line:     0,
			column:   0,
			expected: []dslshape.NodeType{dslshape.NodeTypeDefinition, dslshape.NodeTypeFile},
		},
		{
			name:     "single def, later on line",
			schema:   `definition user {}`,
			line:     0,
			column:   10,
			expected: []dslshape.NodeType{dslshape.NodeTypeDefinition, dslshape.NodeTypeFile},
		},
		{
			name: "relation",
			schema: `definition resource {
				relation viewer: user
			}`,
			line:     1,
			column:   10,
			expected: []dslshape.NodeType{dslshape.NodeTypeRelation, dslshape.NodeTypeDefinition, dslshape.NodeTypeFile},
		},
		{
			name: "relation subject type",
			schema: `definition resource {
				relation viewer: user
			}`,
			line:   1,
			column: 22,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeSpecificTypeReference,
				dslshape.NodeTypeTypeReference,
				dslshape.NodeTypeRelation,
				dslshape.NodeTypeDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference in permission",
			schema: `definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			line:   2,
			column: 26,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeIdentifier,
				dslshape.NodeTypePermission,
				dslshape.NodeTypeDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference in union permission",
			schema: `definition resource {
				relation viewer: user
				relation editor: user
				permission view = viewer + editor
			}`,
			line:   3,
			column: 34,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeIdentifier,
				dslshape.NodeTypeUnionExpression,
				dslshape.NodeTypePermission,
				dslshape.NodeTypeDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference in caveat expression",
			schema: `
			caveat some_caveat(someparam int) {
				someparam > 42
			}

			definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			line:   2,
			column: 6,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeCaveatExpression,
				dslshape.NodeTypeCaveatDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference in caveat parameter",
			schema: `
			caveat some_caveat(someparam int) {
				someparam > 42
			}

			definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			line:   1,
			column: 26,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeCaveatParameter,
				dslshape.NodeTypeCaveatDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference in caveat parameter type",
			schema: `
			caveat some_caveat(someparam int) {
				someparam > 42
			}

			definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			line:   1,
			column: 33,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeCaveatTypeReference,
				dslshape.NodeTypeCaveatParameter,
				dslshape.NodeTypeCaveatDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference to caveat",
			schema: `
			caveat some_caveat(someparam int) {
				someparam > 42
			}

			definition resource {
				relation viewer: user with some_caveat
				permission view = viewer
			}`,
			line:   6,
			column: 40,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeCaveatReference,
				dslshape.NodeTypeSpecificTypeReference,
				dslshape.NodeTypeTypeReference,
				dslshape.NodeTypeRelation,
				dslshape.NodeTypeDefinition,
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "multiline doc comment",
			schema: `
				definition user {}

				/**
				 * This is a doc comment
				 */
				definition resource {
					relation viewer: user
				}
			`,
			line:   4,
			column: 2,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeFile,
			},
		},
		{
			name: "reference to commented",
			schema: `
				definition user {}

				definition resource {
					// viewer is some sort of relation
					relation viewer: user
				}
			`,
			line:   5,
			column: 10,
			expected: []dslshape.NodeType{
				dslshape.NodeTypeRelation,
				dslshape.NodeTypeDefinition,
				dslshape.NodeTypeFile,
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := Compile(InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schema,
			}, AllowUnprefixedObjectType())
			require.NoError(t, err)

			nodeChain, err := PositionToAstNodeChain(compiled, input.Source("test"), input.Position{
				LineNumber:     tc.line,
				ColumnPosition: tc.column,
			})
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)

			if tc.expected == nil {
				require.Nil(t, nodeChain)
			} else {
				require.NotNil(t, nodeChain)
				nodes := nodeChain.nodes

				nodeTypes := make([]dslshape.NodeType, len(nodes))
				for idx, node := range nodes {
					nodeTypes[idx] = node.GetType()
				}

				require.Equal(t, tc.expected, nodeTypes)
			}
		})
	}
}

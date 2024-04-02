package development

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestResolver(t *testing.T) {
	testSource := input.Source("test")

	tcs := []struct {
		name              string
		schema            string
		line              int
		column            int
		expectedReference *SchemaReference
	}{
		{
			name: "basic relation",
			schema: `definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer
			}
			`,
			line:   4,
			column: 24,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 4, ColumnPosition: 24},
				Text:              "viewer",
				ReferenceType:     ReferenceTypeRelation,
				ReferenceMarkdown: "relation viewer",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 3, ColumnPosition: 4},
			},
		},
		{
			name: "basic permission",
			schema: `definition user {}

			definition resource {
				relation viewer: user
				relation editor: user
				permission edit = editor
				permission view = viewer + edit
			}
			`,
			line:   6,
			column: 33,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 6, ColumnPosition: 33},
				Text:              "edit",
				ReferenceType:     ReferenceTypePermission,
				ReferenceMarkdown: "permission edit",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 5, ColumnPosition: 4},
			},
		},
		{
			name: "basic type",
			schema: `definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer
			}
			`,
			line:   3,
			column: 24,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 3, ColumnPosition: 24},
				Text:              "user",
				ReferenceType:     ReferenceTypeDefinition,
				ReferenceMarkdown: "definition user",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 0, ColumnPosition: 0},
			},
		},
		{
			name: "caveat reference",
			schema: `definition user {}

			caveat somecaveat(someparam int) {
				someparam < 42
			}

			definition resource {
				relation viewer: user with somecaveat
				permission view = viewer
			}
			`,
			line:   7,
			column: 35,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 7, ColumnPosition: 35},
				Text:              "somecaveat",
				ReferenceType:     ReferenceTypeCaveat,
				ReferenceMarkdown: "caveat somecaveat",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 2, ColumnPosition: 3},
			},
		},
		{
			name: "arrow relation reference",
			schema: `definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer->foo
			}
			`,
			line:   4,
			column: 23,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 4, ColumnPosition: 23},
				Text:              "viewer",
				ReferenceType:     ReferenceTypeRelation,
				ReferenceMarkdown: "relation viewer",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 3, ColumnPosition: 4},
			},
		},
		{
			name: "arrow permission reference",
			schema: `definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer->viewer
			}
			`,
			line:              4,
			column:            31,
			expectedReference: nil,
		},
		{
			name: "caveat parameter reference",
			schema: `definition user {}

			caveat somecaveat(someparam int) {
				someparam < 42
			}

			definition resource {
				relation viewer: user with somecaveat
				permission view = viewer
			}
			`,
			line:   3,
			column: 6,
			expectedReference: &SchemaReference{
				Source:            input.Source("test"),
				Position:          input.Position{LineNumber: 3, ColumnPosition: 6},
				Text:              "someparam",
				ReferenceType:     ReferenceTypeCaveatParameter,
				ReferenceMarkdown: "someparam int",
				TargetSource:      &testSource,
				TargetPosition:    &input.Position{LineNumber: 2, ColumnPosition: 3},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			resolver, err := NewResolver(compiled)
			require.NoError(t, err)

			ref, err := resolver.ReferenceAtPosition(input.Source("test"), input.Position{
				LineNumber:     tc.line,
				ColumnPosition: tc.column,
			})

			require.NoError(t, err)
			require.Equal(t, tc.expectedReference, ref)
		})
	}
}

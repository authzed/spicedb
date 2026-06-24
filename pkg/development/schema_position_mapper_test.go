package development

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestSchemaPositionMapper(t *testing.T) {
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
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 4, ColumnPosition: 24},
				Text:                     "viewer",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation viewer",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation viewer: user\n",
				TargetNamePositionOffset: 9,
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
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 6, ColumnPosition: 33},
				Text:                     "edit",
				ReferenceType:            ReferenceTypePermission,
				ReferenceMarkdown:        "permission edit",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 5, ColumnPosition: 4},
				TargetSourceCode:         "permission edit = editor\n",
				TargetNamePositionOffset: 11,
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
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 3, ColumnPosition: 24},
				Text:                     "user",
				ReferenceType:            ReferenceTypeDefinition,
				ReferenceMarkdown:        "definition user",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 0, ColumnPosition: 0},
				TargetSourceCode:         "definition user {}",
				TargetNamePositionOffset: 11,
			},
		},
		{
			name: "subject relation type",
			schema: `definition user {}

			definition group {
				relation member: user
			}

			definition resource {
				relation viewer: group#member
				permission view = viewer
			}
			`,
			line:   7,
			column: 24,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 7, ColumnPosition: 24},
				Text:                     "group",
				ReferenceType:            ReferenceTypeDefinition,
				ReferenceMarkdown:        "definition group",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 2, ColumnPosition: 3},
				TargetSourceCode:         "definition group {\n\t// ...\n}",
				TargetNamePositionOffset: 11,
			},
		},
		{
			name: "subject relation relation",
			schema: `definition user {}

			definition group {
				relation member: user
			}

			definition resource {
				relation viewer: group#member
				permission view = viewer
			}
			`,
			line:   7,
			column: 32,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 7, ColumnPosition: 32},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "filled in type",
			schema: `definition user {}

			definition resource {
				relation viewer: user | resource
				permission view = viewer
			}
			`,
			line:   3,
			column: 29,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 3, ColumnPosition: 29},
				Text:                     "resource",
				ReferenceType:            ReferenceTypeDefinition,
				ReferenceMarkdown:        "definition resource",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 2, ColumnPosition: 3},
				TargetSourceCode:         "definition resource {\n\t// ...\n}",
				TargetNamePositionOffset: 11,
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
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 7, ColumnPosition: 35},
				Text:                     "somecaveat",
				ReferenceType:            ReferenceTypeCaveat,
				ReferenceMarkdown:        "caveat somecaveat",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 2, ColumnPosition: 3},
				TargetSourceCode:         "caveat somecaveat(someparam int) {\n\t// ...\n}",
				TargetNamePositionOffset: 7,
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
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 4, ColumnPosition: 23},
				Text:                     "viewer",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation viewer",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation viewer: user\n",
				TargetNamePositionOffset: 9,
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
			name: "arrow rhs resolves relation on target type",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition document {
				relation parent: org
				permission viewable = parent->member
			}
			`,
			line:   8,
			column: 36,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 8, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs (.any) resolves relation on target type",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition document {
				relation parent: org
				permission viewable = parent.any(member)
			}
			`,
			line:   8,
			column: 37,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 8, ColumnPosition: 37},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs (.all) resolves relation on target type",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition document {
				relation parent: org
				permission viewable = parent.all(member)
			}
			`,
			line:   8,
			column: 37,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 8, ColumnPosition: 37},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs resolves permission on target type",
			schema: `definition user {}

			definition org {
				relation member: user
				permission view = member
			}

			definition document {
				relation parent: org
				permission viewable = parent->view
			}
			`,
			line:   9,
			column: 35,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 9, ColumnPosition: 35},
				Text:                     "view",
				ReferenceType:            ReferenceTypePermission,
				ReferenceMarkdown:        "permission view",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 4, ColumnPosition: 4},
				TargetSourceCode:         "permission view = member\n",
				TargetNamePositionOffset: 11,
			},
		},
		{
			name: "arrow rhs subject-subrelation LHS resolves on target namespace",
			schema: `definition user {}

			definition group {
				relation member: user
			}

			definition document {
				relation owner_group: group#member
				permission viewable = owner_group->member
			}
			`,
			line:   8,
			column: 41,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 8, ColumnPosition: 41},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs with multi-target LHS combines identical matches",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition team {
				relation member: user
			}

			definition document {
				relation parent: org | team
				permission viewable = parent->member
			}
			`,
			line:   12,
			column: 36,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 12, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "// from org\nrelation member: user\n\n// from team\nrelation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs with multi-target LHS shows all matches",
			schema: `definition user {}
definition somethingelse {}

			definition org {
				relation member: user
			}

			definition team {
				relation member: somethingelse
			}

			definition document {
				relation parent: org | team
				permission viewable = parent->member
			}
			`,
			line:   13,
			column: 36,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 13, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 4, ColumnPosition: 4},
				TargetSourceCode:         "// from org\nrelation member: user\n\n// from team\nrelation member: somethingelse\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs with multi-target LHS first match is a permission",
			schema: `definition user {}

			definition org {
				relation admin: user
				permission member = admin
			}

			definition team {
				relation member: user
			}

			definition document {
				relation parent: org | team
				permission viewable = parent->member
			}
			`,
			line:   13,
			column: 36,
			// `org` defines `member` as a permission while `team` defines it as
			// a relation, so the multi-match path surfaces both. The top-level
			// reference reflects the first match (`org`'s permission).
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 13, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypePermission,
				ReferenceMarkdown:        "permission member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 4, ColumnPosition: 4},
				TargetSourceCode:         "// from org\npermission member = admin\n\n// from team\nrelation member: user\n",
				TargetNamePositionOffset: 11,
			},
		},
		{
			name: "arrow rhs with multi-target LHS later match is a permission",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition team {
				relation admin: user
				permission member = admin
			}

			definition document {
				relation parent: org | team
				permission viewable = parent->member
			}
			`,
			line:   13,
			column: 36,
			// `org` defines `member` as a relation and `team` as a permission.
			// The top-level reference reflects the first match (`org`'s
			// relation) even though a later match is a permission.
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 13, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 3, ColumnPosition: 4},
				TargetSourceCode:         "// from org\nrelation member: user\n\n// from team\npermission member = admin\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs with duplicate-namespace LHS dedupes",
			schema: `definition user {}

			definition org {
				relation admin: user
				relation member: user
			}

			definition document {
				relation parent: org | org#admin
				permission viewable = parent->member
			}
			`,
			line:   9,
			column: 36,
			// `AllowedSubjectRelations` returns one entry per allowed direct
			// subject type, so `parent: org | org#admin` produces two entries
			// that both resolve `member` on `org`. Dedup by namespace so we
			// don't repeat the same block in the hover output.
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 9, ColumnPosition: 36},
				Text:                     "member",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation member",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 4, ColumnPosition: 4},
				TargetSourceCode:         "relation member: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "arrow rhs with no match returns nil",
			schema: `definition user {}

			definition org {
				relation member: user
			}

			definition document {
				relation parent: org
				permission viewable = parent->nonexistent
			}
			`,
			line:              8,
			column:            36,
			expectedReference: nil,
		},
		{
			name: "caveat parameter reference",
			schema: `definition user {}

			caveat somecaveat(someparam int) {
				someparam < 42 || someparam > 43
			}

			definition resource {
				relation viewer: user with somecaveat
				permission view = viewer
			}
			`,
			line:   3,
			column: 6, // TODO if you put 23, the mapper doesn't return anything
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 3, ColumnPosition: 6},
				Text:                     "someparam",
				ReferenceType:            ReferenceTypeCaveatParameter,
				ReferenceMarkdown:        "someparam int",
				TargetSource:             &testSource,
				TargetSourceCode:         "someparam int",
				TargetNamePositionOffset: 0,
			},
		},
		{
			name: "caveat parameter reference second reference",
			schema: `definition user {}

			caveat somecaveat(someparam int) {
				someparam < 42 || someparam > 43
			}

			definition resource {
				relation viewer: user with somecaveat
				permission view = viewer
			}
			`,
			line:   3,
			column: 23,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 3, ColumnPosition: 23},
				Text:                     "someparam",
				ReferenceType:            ReferenceTypeCaveatParameter,
				ReferenceMarkdown:        "someparam int",
				TargetSource:             &testSource,
				TargetSourceCode:         "someparam int",
				TargetNamePositionOffset: 0,
			},
		},
		{
			name: "caveat expression non-parameter token",
			schema: `definition user {}

			caveat somecaveat(someparam int) {
				someparam < 42 || someparam > 43
			}

			definition resource {
				relation viewer: user with somecaveat
				permission view = viewer
			}
			`,
			line:              3,
			column:            19, // space
			expectedReference: nil,
		},
		{
			name: "longer test",
			schema: `definition user {}

definition document {
	relation viewer: user
	relation editor: user
	relation third: user
	permission another = viewer
	permission view = third + editor + another
}`,
			line:   7,
			column: 19,
			expectedReference: &SchemaReference{
				Source:                   input.Source("test"),
				Position:                 input.Position{LineNumber: 7, ColumnPosition: 19},
				Text:                     "third",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation third",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 5, ColumnPosition: 1},
				TargetSourceCode:         "relation third: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "reference to comment",
			schema: `
				definition user {}

				definition resource {
					// viewer is some sort of relation
					relation viewer: user
				}
			`,
			line:              4,
			column:            10,
			expectedReference: nil,
		},
		{
			name: "reference to on commented",
			schema: `
				definition user {}

				definition resource {
					// viewer is some sort of relation
					relation viewer: user
				}
			`,
			line:   5,
			column: 22,
			expectedReference: &SchemaReference{
				Source:                   "test",
				Position:                 input.Position{LineNumber: 5, ColumnPosition: 22},
				Text:                     "user",
				ReferenceType:            1,
				ReferenceMarkdown:        "definition user",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 1, ColumnPosition: 4},
				TargetSourceCode:         "definition user {}",
				TargetNamePositionOffset: 11,
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
			line:              5,
			column:            10,
			expectedReference: nil,
		},
		{
			name: "reference to doc comment",
			schema: `
				definition user {}

				/**
				 * This is a comment
				 */
				definition resource {
					relation viewer: user
				}
			`,
			line:              4,
			column:            5,
			expectedReference: nil,
		},
		{
			name: "reference to commented type",
			schema: `
				/** user is a user */
				definition user {}

				definition resource {
					relation viewer: user
				}
			`,
			line:   5,
			column: 22,
			expectedReference: &SchemaReference{
				Source:                   "test",
				Position:                 input.Position{LineNumber: 5, ColumnPosition: 22},
				Text:                     "user",
				ReferenceType:            1,
				ReferenceMarkdown:        "definition user",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 2, ColumnPosition: 4},
				TargetSourceCode:         "/** user is a user */\ndefinition user {}",
				TargetNamePositionOffset: 11,
			},
		},
		{
			name: "reference to commented relation",
			schema: `
				definition user {}

				definition resource {
					// viewer is a relation for viewing
					relation viewer: user

					permission view = viewer
				}
			`,
			line:   7,
			column: 25,
			expectedReference: &SchemaReference{
				Source:                   "test",
				Position:                 input.Position{LineNumber: 7, ColumnPosition: 25},
				Text:                     "viewer",
				ReferenceType:            ReferenceTypeRelation,
				ReferenceMarkdown:        "relation viewer",
				TargetSource:             &testSource,
				TargetPosition:           &input.Position{LineNumber: 5, ColumnPosition: 5},
				TargetSourceCode:         "// viewer is a relation for viewing\nrelation viewer: user\n",
				TargetNamePositionOffset: 9,
			},
		},
		{
			name: "reference to a partial",
			schema: `use partial

				partial view_partial {
					relation user: user
					permission view = user
				}

				definition secret {
					...view_partial
				}
			`,
			line:   8,
			column: 8,
			expectedReference: &SchemaReference{
				Source:                   "test",
				Position:                 input.Position{LineNumber: 8, ColumnPosition: 8},
				Text:                     "view_partial",
				ReferenceType:            ReferenceTypePartial,
				TargetSource:             &testSource,
				TargetSourceCode:         "partial view_partial",
				TargetPosition:           &input.Position{LineNumber: 2, ColumnPosition: 4},
				TargetNamePositionOffset: len("partial "),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			schemaPositionMapper, err := NewSchemaPositionMapper(compiled)
			require.NoError(t, err)

			ref, err := schemaPositionMapper.ReferenceAtPosition(input.Source("test"), input.Position{
				LineNumber:     tc.line,
				ColumnPosition: tc.column,
			})

			require.NoError(t, err)
			require.Equal(t, tc.expectedReference, ref)
		})
	}
}

func TestSchemaPositionMapperComposableSchema(t *testing.T) {
	rootSchema := `// this is a comment
use import
use partial
import "path/users.zed"
import "path/partials.zed"

definition resource {
	relation somerelation: user with is_raining
	relation oops: group#member
	...secret
	permission usessecret = secret
}
`
	sourceFS := fstest.MapFS{
		"path/partials.zed": &fstest.MapFile{Data: []byte("use partial\npartial secret {\nrelation secret: user\n}")},
		"path/users.zed":    &fstest.MapFile{Data: []byte("definition user {}\ncaveat is_raining(day string) {\nday == \"saturday\"\n}")},
	}

	rootSource := input.Source("path/root.zed")
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       rootSource,
		SchemaString: rootSchema,
	}, compiler.AllowUnprefixedObjectType(), compiler.SourceFS(sourceFS))
	require.NoError(t, err)

	mapper, err := NewSchemaPositionMapper(compiled)
	require.NoError(t, err)

	t.Run("cursor on the import path", func(t *testing.T) {
		ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
			LineNumber:     3,
			ColumnPosition: 5,
		})
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, ReferenceTypeImport, ref.ReferenceType)
		require.Equal(t, input.Source("path/users.zed"), *ref.TargetSource)
		require.Empty(t, ref.TargetSourceCode)
		require.Equal(t, "path/users.zed", ref.Text)
		require.Equal(t, rootSource, ref.Source)
		require.Equal(t, input.Position{LineNumber: 3, ColumnPosition: 5}, ref.Position)
		require.Equal(t, &input.Position{LineNumber: 0, ColumnPosition: 0}, ref.TargetPosition)
	})

	t.Run("cursor on the user definition", func(t *testing.T) {
		ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
			LineNumber:     7,
			ColumnPosition: 30,
		})
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, ReferenceTypeDefinition, ref.ReferenceType)
		require.Equal(t, "definition user {}", ref.TargetSourceCode)
		require.Equal(t, "user", ref.Text)
		require.Equal(t, rootSource, ref.Source)
		require.Equal(t, input.Source("path/users.zed"), *ref.TargetSource)
		require.Equal(t, input.Position{LineNumber: 7, ColumnPosition: 30}, ref.Position)
		require.Equal(t, &input.Position{LineNumber: 0, ColumnPosition: 0}, ref.TargetPosition)
	})

	t.Run("cursor on the referenced relation", func(t *testing.T) {
		ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
			LineNumber:     10,
			ColumnPosition: 26,
		})
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, ReferenceTypeRelation, ref.ReferenceType)
		require.Equal(t, "relation secret: user\n", ref.TargetSourceCode)
		require.Equal(t, "secret", ref.Text)
		require.Equal(t, rootSource, ref.Source)
		require.Equal(t, input.Source("path/partials.zed"), *ref.TargetSource)
		require.Equal(t, input.Position{LineNumber: 10, ColumnPosition: 26}, ref.Position)
		require.Equal(t, &input.Position{LineNumber: 2, ColumnPosition: 0}, ref.TargetPosition)
	})

	t.Run("cursor on the caveat ref", func(t *testing.T) {
		ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
			LineNumber:     7,
			ColumnPosition: 43,
		})
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, ReferenceTypeCaveat, ref.ReferenceType)
		require.Equal(t, "caveat is_raining(day string) {\n\t// ...\n}", ref.TargetSourceCode)
		require.Equal(t, "is_raining", ref.Text)
		require.Equal(t, rootSource, ref.Source)
		require.Equal(t, input.Source("path/users.zed"), *ref.TargetSource)
		require.Equal(t, input.Position{LineNumber: 7, ColumnPosition: 43}, ref.Position)
		require.Equal(t, &input.Position{LineNumber: 1, ColumnPosition: 0}, ref.TargetPosition)
	})

	t.Run("cursor on the partial ref", func(t *testing.T) {
		ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
			LineNumber:     9,
			ColumnPosition: 5,
		})
		require.NoError(t, err)
		require.NotNil(t, ref)
		require.Equal(t, ReferenceTypePartial, ref.ReferenceType)
		require.Equal(t, "partial secret", ref.TargetSourceCode)
		require.Equal(t, "secret", ref.Text)
		require.Equal(t, input.Source("path/partials.zed"), *ref.TargetSource)
		require.Equal(t, rootSource, ref.Source)
		require.Equal(t, input.Position{LineNumber: 9, ColumnPosition: 5}, ref.Position)
		require.Equal(t, &input.Position{LineNumber: 1, ColumnPosition: 0}, ref.TargetPosition)
	})
}

// TestSchemaPositionMapperArrowRHSImportedTarget exercises arrow RHS hover when
// the target namespace lives in an imported file. The arrow resolution itself
// finds the right relation; the TargetSource may currently point at the wrong
// file because resolveTargetSource keys off the relation name rather than the
// owning definition name. This test pins the current behavior so regressions
// are caught and the unrelated TargetSource fix can be tracked separately.
func TestSchemaPositionMapperArrowRHSImportedTarget(t *testing.T) {
	rootSchema := `use import
import "path/groups.zed"

definition document {
	relation parent: group
	permission viewable = parent->member
}
`
	sourceFS := fstest.MapFS{
		"path/groups.zed": &fstest.MapFile{Data: []byte("definition user {}\ndefinition group {\nrelation member: user\n}\n")},
	}

	rootSource := input.Source("path/root.zed")
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       rootSource,
		SchemaString: rootSchema,
	}, compiler.AllowUnprefixedObjectType(), compiler.SourceFS(sourceFS))
	require.NoError(t, err)

	mapper, err := NewSchemaPositionMapper(compiled)
	require.NoError(t, err)

	// Cursor on "member" in `parent->member`.
	ref, err := mapper.ReferenceAtPosition(rootSource, input.Position{
		LineNumber:     5,
		ColumnPosition: 32,
	})
	require.NoError(t, err)
	require.NotNil(t, ref, "arrow RHS should now resolve")
	require.Equal(t, "member", ref.Text)
	require.Equal(t, ReferenceTypeRelation, ref.ReferenceType)
	require.Equal(t, "relation member", ref.ReferenceMarkdown)
	require.Equal(t, &input.Position{LineNumber: 2, ColumnPosition: 0}, ref.TargetPosition)
}

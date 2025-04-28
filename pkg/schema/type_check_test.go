package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/stretchr/testify/require"
)

func TestTypechecking(t *testing.T) {
	t.Parallel()
	type testcase struct {
		name       string
		schemaText string
		expected   map[string][]string
	}
	tcs := []testcase{
		{
			name: "basic arrow",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org->member + viewer
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
				"resource#org":        {"organization"},
				"resource#view":       {"user"},
			},
		},
		{
			name: "multi-type arrow",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org + viewer
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
				"resource#org":        {"organization"},
				"resource#view":       {"organization", "user"},
			},
		},
		{
			name: "functional",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				permission view = org.all(member)
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
				"resource#org":        {"organization"},
				"resource#view":       {"user"},
			},
		},
		{
			name: "multi-type rel",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation viewer: user | organization
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user", "organization"},
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			schema, err := compiler.Compile(compiler.InputSchema{
				Source:       "",
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			res := ResolverForCompiledSchema(*schema)
			ts := NewTypeSystem(res)
			for _, resource := range schema.ObjectDefinitions {
				for _, relation := range resource.Relation {
					set, err := ts.GetTypesForRelation(context.Background(), resource.Name, relation.Name)
					require.NoError(t, err)
					types := set.AsSlice()

					rel := resource.Name + "#" + relation.Name
					expected, ok := tc.expected[rel]
					require.True(t, ok, fmt.Sprintf("expected %v to be in %v", rel, tc.expected))
					require.Len(t, types, len(expected), rel)

					for _, typ := range types {
						require.Contains(t, expected, typ, fmt.Sprintf("expected %v to be in %v", typ, expected))
					}
				}
			}
		})
	}
}

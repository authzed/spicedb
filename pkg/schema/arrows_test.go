package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

func TestLookupTuplesetArrows(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name       string
		schemaText string

		expected map[string][]string
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
				"organization#member": {},
				"resource#viewer":     {},
				"resource#org":        {"org->member"},
				"resource#view":       {},
			},
		},
		{
			name: "multiple arrows",
			schemaText: `
			definition user {}

			definition organization {
				relation admin: user
				relation member: user
				permission can_admin = admin
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org->member + viewer
				permission edit = org->can_admin
			}
		`,
			expected: map[string][]string{
				"organization#admin":     {},
				"organization#member":    {},
				"organization#can_admin": {},
				"resource#viewer":        {},
				"resource#org":           {"org->member", "org->can_admin"},
				"resource#view":          {},
				"resource#edit":          {},
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
			arrowSet, err := buildArrowSet(context.Background(), res)
			require.NoError(t, err)

			for _, resource := range schema.ObjectDefinitions {
				for _, relation := range resource.Relation {
					arrows := arrowSet.LookupTuplesetArrows(resource.Name, relation.Name)
					require.NotNil(t, arrows)

					rel := resource.Name + "#" + relation.Name
					expected, ok := tc.expected[rel]
					require.True(t, ok, fmt.Sprintf("expected %v to be in %v", rel, tc.expected))
					require.Len(t, arrows, len(expected), rel)

					for _, arrow := range arrows {
						key := arrow.Arrow.Tupleset.Relation + "->" + arrow.Arrow.ComputedUserset.Relation
						require.Contains(t, expected, key, fmt.Sprintf("expected %v to be in %v", key, expected))
					}
				}
			}
		})
	}
}

func TestAllReachableRelations(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name       string
		schemaText string

		expected []string
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
			expected: []string{
				"organization#member",
				"resource#org",
			},
		},
		{
			name: "multiple arrows",
			schemaText: `
			definition user {}

			definition organization {
				relation admin: user
				relation member: user
				permission can_admin = admin
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org->member + viewer
				permission edit = org->can_admin
			}
		`,
			expected: []string{
				"organization#member",
				"organization#can_admin",
				"resource#org",
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
			arrows, err := buildArrowSet(context.Background(), res)
			require.NoError(t, err)

			reachable := arrows.AllReachableRelations()
			require.NotNil(t, reachable)

			reachable.RemoveAll(mapz.NewSet(tc.expected...))
			require.True(t, reachable.IsEmpty())
		})
	}
}

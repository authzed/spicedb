package schema

import (
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
			name: "functioned arrow any",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org.any(member) + viewer
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
			name: "functioned arrow all",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org.all(member) + viewer
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
			arrowSet, err := buildArrowSet(t.Context(), res)
			require.NoError(t, err)

			for _, resource := range schema.ObjectDefinitions {
				for _, relation := range resource.Relation {
					arrows := arrowSet.LookupTuplesetArrows(resource.Name, relation.Name)
					require.NotNil(t, arrows)

					rel := resource.Name + "#" + relation.Name
					expected, ok := tc.expected[rel]
					require.True(t, ok, "expected %v to be in %v", rel, tc.expected)
					require.Len(t, arrows, len(expected), rel)

					for _, arrow := range arrows {
						key := arrow.Arrow.Tupleset.Relation + "->" + arrow.Arrow.ComputedUserset.Relation
						require.Contains(t, expected, key, "expected %v to be in %v", key, expected)
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
			name: "functioned arrow any",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org.any(member) + viewer
			}
		`,
			expected: []string{
				"organization#member",
				"resource#org",
			},
		},
		{
			name: "functioned arrow all",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org.all(member) + viewer
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
			arrows, err := buildArrowSet(t.Context(), res)
			require.NoError(t, err)

			reachable := arrows.AllReachableRelations()
			require.NotNil(t, reachable)

			reachable.RemoveAll(mapz.NewSet(tc.expected...))
			require.True(t, reachable.IsEmpty())
		})
	}
}

func TestLookupArrowsWithComputedUserset(t *testing.T) {
	t.Parallel()

	type expectedArrow struct {
		parentNamespace string
		parentRelation  string
		arrowExpression string
	}

	type testcase struct {
		name       string
		schemaText string

		targetNamespace string
		targetRelation  string
		expected        []expectedArrow
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
				permission view = org->member
			}
		`,
			targetNamespace: "organization",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "resource",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
			},
		},
		{
			name: "multiple arrows to same relation",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				permission view = org->member
				permission another_view = org->member
			}

			definition another_resource {
				relation org: organization
				permission view = org->member
			}
		`,
			targetNamespace: "organization",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "resource",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
				{
					parentNamespace: "resource",
					parentRelation:  "another_view",
					arrowExpression: "org->member",
				},
				{
					parentNamespace: "another_resource",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
			},
		},
		{
			name: "arrows across different namespaces",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition project {
				relation org: organization
				permission view = org->member
			}

			definition document {
				relation org: organization
				permission view = org->member
			}
		`,
			targetNamespace: "organization",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "project",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
				{
					parentNamespace: "document",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
			},
		},
		{
			name: "arrow in union and exclusion expression",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation reader: user
				relation writer: user
				relation banned: user
				relation org: organization
				permission view = reader + writer - banned + org->member
			}
		`,
			targetNamespace: "organization",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "resource",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
			},
		},
		{
			name: "chained arrow through intermediate type",
			schemaText: `
			definition user {}

			definition team {
				relation member: user
			}

			definition organization {
				relation team: team
				permission member = team->member
			}

			definition resource {
				relation org: organization
				permission view = org->member
			}
		`,
			targetNamespace: "team",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "organization",
					parentRelation:  "member",
					arrowExpression: "team->member",
				},
			},
		},
		{
			name: "multiple arrows in nested expression",
			schemaText: `
			definition user {}

			definition team {
				relation member: user
			}

			definition organization {
				relation admin: user
				relation team: team
				permission member = admin + team->member
			}

			definition resource {
				relation org: organization
				relation team: team
				permission view = org->member + team->member
			}
		`,
			targetNamespace: "team",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "organization",
					parentRelation:  "member",
					arrowExpression: "team->member",
				},
				{
					parentNamespace: "resource",
					parentRelation:  "view",
					arrowExpression: "team->member",
				},
			},
		},
		{
			name: "arrow with intersection and exclusion",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
				relation admin: user
			}

			definition resource {
				relation org: organization
				relation blocked: user
				permission view = org->member & org->admin - blocked
			}
		`,
			targetNamespace: "organization",
			targetRelation:  "member",
			expected: []expectedArrow{
				{
					parentNamespace: "resource",
					parentRelation:  "view",
					arrowExpression: "org->member",
				},
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
			arrowSet, err := buildArrowSet(t.Context(), res)
			require.NoError(t, err)

			arrows := arrowSet.LookupArrowsWithComputedUserset(tc.targetNamespace, tc.targetRelation)
			require.Len(t, arrows, len(tc.expected))

			for _, expected := range tc.expected {
				found := false
				for _, actual := range arrows {
					actualExpression := actual.Arrow.Tupleset.Relation + "->" + actual.Arrow.ComputedUserset.Relation
					if actual.ParentNamespace == expected.parentNamespace &&
						actual.ParentRelationName == expected.parentRelation &&
						actualExpression == expected.arrowExpression {
						found = true
						break
					}
				}
				require.True(t, found, "expected arrow %v not found in %v", expected, arrows)
			}
		})
	}
}

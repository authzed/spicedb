package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

func TestTypecheckingJustTypes(t *testing.T) {
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
		{
			name: "subrel",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation viewer: organization#member
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
			},
		},
		{
			name: "wildcard",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user:*
			}

			definition resource {
				relation viewer: organization#member
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
			},
		},
		{
			name: "banned",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation viewer: organization#member
				relation banned: user
				permission view = viewer - banned
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user"},
				"resource#banned":     {"user"},
				"resource#view":       {"user"},
			},
		},
		{
			name: "sub_rewrites",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org + (org->member & viewer)
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
			name: "change_name",
			schemaText: `
			definition user {}

			definition organization {
				relation change_name: user
			}

			definition team {
				relation parent: organization | team
				permission change_name = parent->change_name
			}
			`,
			expected: map[string][]string{
				"organization#change_name": {"user"},
				"team#parent":              {"organization", "team"},
				"team#change_name":         {"user"},
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
					types, err := ts.GetRecursiveTerminalTypesForRelation(t.Context(), resource.Name, relation.Name)
					require.NoError(t, err)

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

// TODO(jschorr): Add to the consistency tests as well, to validate the full set of types within the consistency tests.
func TestTypecheckingWithSubrelations(t *testing.T) {
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
		{
			name: "subrel",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation viewer: organization#member
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user", "organization#member"},
			},
		},
		{
			name: "wildcard",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user:*
			}

			definition resource {
				relation viewer: organization#member
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user", "organization#member"},
			},
		},
		{
			name: "banned",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation viewer: organization#member
				relation banned: user
				permission view = viewer - banned
			}
		`,
			expected: map[string][]string{
				"organization#member": {"user"},
				"resource#viewer":     {"user", "organization#member"},
				"resource#banned":     {"user"},
				"resource#view":       {"user", "organization#member"},
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
					types, err := ts.GetFullRecursiveSubjectTypesForRelation(t.Context(), resource.Name, relation.Name)
					require.NoError(t, err)

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

func TestIncompleteSchema(t *testing.T) {
	// This test is a little redundant, as doing this type checking requires one to have the full schema, but it _may_ be pulled dynamically and fail.
	// So until we operate in complete schema caching, there are fail points that can bubble up.
	t.Parallel()
	type testcase struct {
		name       string
		schemaText string
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

			// Inject the failure
			var missingDefs []*corev1.NamespaceDefinition
			for _, v := range schema.ObjectDefinitions {
				if v.GetName() == "resource" {
					missingDefs = append(missingDefs, v)
				}
			}
			schema.ObjectDefinitions = missingDefs

			res := ResolverForCompiledSchema(*schema)
			ts := NewTypeSystem(res)
			_, err = ts.GetRecursiveTerminalTypesForRelation(t.Context(), "resource", "view")
			require.Error(t, err)
			_, err = ts.GetFullRecursiveSubjectTypesForRelation(t.Context(), "resource", "view")
			require.Error(t, err)
		})
	}
}

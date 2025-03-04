package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

type expectedRelation struct {
	Namespace string
	Relation  string
	Type      ReferenceType
}

type testcase struct {
	name       string
	schemaText string

	expected map[string][]expectedRelation
}

func TestRelationsReferencing(t *testing.T) {
	t.Parallel()

	tcs := []testcase{
		{
			name: "basic",
			schemaText: `
			definition user {}

			definition group {
				relation direct_member: user
				permission member = direct_member
			}

			definition resource {
				relation viewer: user
				relation editor: user
				permission view = viewer + editor
			}
			`,
			expected: map[string][]expectedRelation{
				"group#direct_member": {
					{Namespace: "group", Relation: "member", Type: RelationInExpression},
				},
				"group#member": {},
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "basic intersection",
			schemaText: `
			definition user {}

			definition group {
				relation direct_member: user | group#member
				permission member = direct_member
			}

			definition resource {
				relation viewer: user | group#member
				relation editor: user | group#member
				permission view = viewer & editor
			}
		`,
			expected: map[string][]expectedRelation{
				"group#direct_member": {
					{Namespace: "group", Relation: "member", Type: RelationInExpression},
				},
				"group#member": {
					{Namespace: "group", Relation: "direct_member", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "viewer", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "editor", Type: RelationIsSubjectType},
				},
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "basic exclusion",
			schemaText: `
			definition user {}

			definition group {
				relation direct_member: user | group#member
				permission member = direct_member
			}

			definition resource {
				relation viewer: user | group#member
				relation editor: user | group#member
				permission view = viewer - editor
			}
		`,
			expected: map[string][]expectedRelation{
				"group#direct_member": {
					{Namespace: "group", Relation: "member", Type: RelationInExpression},
				},
				"group#member": {
					{Namespace: "group", Relation: "direct_member", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "viewer", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "editor", Type: RelationIsSubjectType},
				},
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "nested intersection exclusion",
			schemaText: `
			definition user {}

			definition resource {
				relation viewer: user
				relation editor: user
				relation banned: user
				permission view = (viewer & editor) - banned
			}
		`,
			expected: map[string][]expectedRelation{
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#banned": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "nested intersections",
			schemaText: `
			definition user {}

			definition resource {
				relation viewer: user
				relation editor: user
				relation third: user
				permission view = (viewer & editor) & third
			}
		`,
			expected: map[string][]expectedRelation{
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#third": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "nested intersections with groups",
			schemaText: `
			definition user {}

			definition group {
				relation direct_member: user | group#member
				permission member = direct_member
			}

			definition resource {
				relation viewer: user | group#member
				relation editor: user | group#member
				relation third: user  | group#member
				permission view = (viewer & editor) & third
			}
		`,
			expected: map[string][]expectedRelation{
				"group#direct_member": {
					{Namespace: "group", Relation: "member", Type: RelationInExpression},
				},
				"group#member": {
					{Namespace: "group", Relation: "direct_member", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "viewer", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "editor", Type: RelationIsSubjectType},
					{Namespace: "resource", Relation: "third", Type: RelationIsSubjectType},
				},
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#editor": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#third": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#view": {},
			},
		},
		{
			name: "basic arrow",
			schemaText: `
			definition user {}

			definition organization {
				relation direct_member: user
				permission member = direct_member
			}

			definition resource {
				relation viewer: user
				relation org: organization
				permission view = org->member + viewer
			}`,
			expected: map[string][]expectedRelation{
				"organization#direct_member": {
					{Namespace: "organization", Relation: "member", Type: RelationInExpression},
				},
				"organization#member": {
					{Namespace: "organization", Relation: "org", Type: RelationIsComputedUsersetForArrow},
				},
				"resource#viewer": {
					{Namespace: "resource", Relation: "view", Type: RelationInExpression},
				},
				"resource#org": {
					{Namespace: "resource", Relation: "view", Type: RelationIsTuplesetForArrow},
				},
				"resource#view": {},
			},
		},
		{
			name: "referencing permission",
			schemaText: `
			definition user {}

			definition organization {
				relation owner: user
				relation member: user

				permission create_repository = owner + member
			}

			definition repository {
				relation organization: organization

				relation reader: user
				relation admin: user

				permission read = reader + admin + organization->owner
			}`,
			expected: map[string][]expectedRelation{
				"organization#owner": {
					{Namespace: "organization", Relation: "create_repository", Type: RelationInExpression},
					{Namespace: "organization", Relation: "organization", Type: RelationIsComputedUsersetForArrow},
				},
				"organization#member": {
					{Namespace: "organization", Relation: "create_repository", Type: RelationInExpression},
				},
				"organization#create_repository": {},
				"repository#organization": {
					{Namespace: "repository", Relation: "read", Type: RelationIsTuplesetForArrow},
				},
				"repository#reader": {
					{Namespace: "repository", Relation: "read", Type: RelationInExpression},
				},
				"repository#admin": {
					{Namespace: "repository", Relation: "read", Type: RelationInExpression},
				},
				"repository#read": {},
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
			graph, err := BuildGraph(context.Background(), res)
			require.NoError(t, err)

			for _, resource := range schema.ObjectDefinitions {
				for _, relation := range resource.Relation {
					references := graph.RelationsReferencing(resource.Name, relation.Name)
					rel := resource.Name + "#" + relation.Name
					expectedRelations, ok := tc.expected[rel]
					require.True(t, ok, fmt.Sprintf("expected %v to be in %v", rel, tc.expected))
					require.Len(t, references, len(expectedRelations), "found rel: %s => expected: %v | found %v", rel, expectedRelations, references)
					for _, expected := range expectedRelations {
						require.True(t, containsRelation(references, expected), fmt.Sprintf("for %s, expected %v to contain %v", rel, references, expected))
					}
				}
			}
		})
	}
}

func containsRelation(references []RelationReferenceInfo, relation expectedRelation) bool {
	for _, reference := range references {
		if reference.Relation.Namespace == relation.Namespace &&
			reference.Relation.Relation == relation.Relation &&
			reference.Type == relation.Type {
			return true
		}
	}
	return false
}

func BenchmarkRelationsReferencing(b *testing.B) {
	b.ReportAllocs()
	schema, err := compiler.Compile(compiler.InputSchema{
		Source: "",
		SchemaString: `
			definition user {}

			definition organization {
				relation owner: user
				relation member: user

				permission create_repository = owner + member
			}

			definition repository {
				relation organization: organization

				relation reader: user
				relation admin: user

				permission read = reader + admin + organization->owner
			}`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(b, err)

	res := ResolverForCompiledSchema(*schema)
	graph, err := BuildGraph(context.Background(), res)
	require.NoError(b, err)

	var size int
	for i := 0; i < b.N; i++ {
		val := graph.RelationsReferencing("organization", "create_repository")
		size = len(val)
	}

	print(size)
}

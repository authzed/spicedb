package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

func diffable(t *testing.T, s *schema.Schema) diff.DiffableSchema {
	t.Helper()
	objectDefs, caveatDefs, err := s.ToDefinitions()
	require.NoError(t, err)
	return diff.DiffableSchema{ObjectDefinitions: objectDefs, CaveatDefinitions: caveatDefs}
}

func TestNewVariantNormalisesBase(t *testing.T) {
	base := mustSchema(t, `
		definition user {}

		/** doc is a document */
		definition doc {
			/** viewer can view */
			relation viewer: user

			/** view is the view permission */
			permission view = viewer
		}`)

	v, err := NewVariant(base, nil)
	require.NoError(t, err)

	// The schema as given carries comments that the variant does not.
	changes, err := diff.DiffSchemas(diffable(t, base), diffable(t, v.Schema), caveattypes.Default.TypeSet)
	require.NoError(t, err)
	require.NotEmpty(t, changes.ChangedNamespaces)

	changes, err = diff.DiffSchemas(diffable(t, v.Base), diffable(t, v.Schema), caveattypes.Default.TypeSet)
	require.NoError(t, err)
	require.Empty(t, changes.ChangedNamespaces)
	require.Empty(t, changes.AddedNamespaces)
	require.Empty(t, changes.RemovedNamespaces)
}

func TestInvalidated(t *testing.T) {
	removeViewer := []Mutation{
		{Kind: RemoveRelation, Definition: "doc", Name: "viewer"},
		{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("editor")},
	}
	removeGroupMemberFromViewer := []Mutation{{
		Kind: RemoveAllowedType, Definition: "doc", Name: "viewer",
		AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}},
	}}
	removeGroupMember := []Mutation{
		{Kind: RemoveRelation, Definition: "group", Name: "member"},
		{
			Kind: RemoveAllowedType, Definition: "doc", Name: "viewer",
			AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}},
		},
	}

	tcs := []struct {
		name         string
		base         string
		mutations    []Mutation
		relationship string
		expected     []string
	}{
		{"removed relation", mutationBaseSchema, removeViewer, "doc:a#viewer@user:b", []string{"remove-relation doc#viewer"}},
		{"other relation", mutationBaseSchema, removeViewer, "doc:a#editor@user:b", []string{}},
		{
			"removed allowed type", mutationBaseSchema, removeGroupMemberFromViewer, "doc:a#viewer@group:g#member",
			[]string{"remove-allowed-type doc#viewer: group#member"},
		},
		{"other allowed type", mutationBaseSchema, removeGroupMemberFromViewer, "doc:a#viewer@user:b", []string{}},
		{
			"removed subject relation", mutationBaseSchema, removeGroupMember, "doc:a#viewer@group:g#member",
			[]string{"remove-relation group#member", "remove-allowed-type doc#viewer: group#member"},
		},
		{
			"removed subject relation as resource", mutationBaseSchema, removeGroupMember, "group:g#member@user:b",
			[]string{"remove-relation group#member"},
		},
		{
			"removed definition", mutationBaseSchemaWithFolder,
			[]Mutation{{Kind: RemoveDefinition, Definition: "folder"}},
			"folder:f#owner@user:b",
			[]string{"remove-definition folder"},
		},
		{
			"added relation", mutationBaseSchema,
			[]Mutation{{Kind: AddRelation, Definition: "doc", Name: "auditor", AllowedTypes: []AllowedType{direct("user")}}},
			"doc:a#viewer@user:b",
			[]string{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			v, err := NewVariant(mustSchema(t, tc.base), tc.mutations)
			require.NoError(t, err)

			found := []string{}
			for _, mutation := range v.Invalidated(tuple.MustParse(tc.relationship)) {
				found = append(found, mutation.String())
			}
			require.Equal(t, tc.expected, found)
		})
	}
}

func TestPartition(t *testing.T) {
	v, err := NewVariant(mustSchema(t, mutationBaseSchema), []Mutation{
		{Kind: RemoveRelation, Definition: "doc", Name: "viewer"},
		{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("editor")},
	})
	require.NoError(t, err)

	rels := []tuple.Relationship{
		tuple.MustParse("doc:a#editor@user:b"),
		tuple.MustParse("doc:a#viewer@user:b"),
		tuple.MustParse("group:g#member@user:b"),
		tuple.MustParse("doc:a#viewer@group:g#member"),
		tuple.MustParse("doc:c#editor@user:d"),
	}

	kept, removed := v.Partition(rels)
	require.Equal(t, []tuple.Relationship{rels[0], rels[2], rels[4]}, kept)
	require.Equal(t, []tuple.Relationship{rels[1], rels[3]}, removed)
}

func TestInverse(t *testing.T) {
	tcs := []struct {
		name      string
		base      string
		mutations []Mutation
		expected  []string
	}{
		{
			"removed relation",
			mutationBaseSchema,
			[]Mutation{
				{Kind: RemoveRelation, Definition: "doc", Name: "viewer"},
				{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("editor")},
			},
			[]string{"change-permission doc#view", "add-relation doc#viewer"},
		},
		{
			"added and removed definitions",
			mutationBaseSchemaWithFolder,
			[]Mutation{
				{Kind: RemoveDefinition, Definition: "folder"},
				{Kind: AddDefinition, Definition: "project", Members: []Mutation{
					{Kind: AddRelation, Definition: "project", Name: "owner", AllowedTypes: []AllowedType{direct("user")}},
				}},
			},
			[]string{"remove-definition project", "add-definition folder"},
		},
		{
			"allowed types and permissions",
			mutationBaseSchema,
			[]Mutation{
				{Kind: AddAllowedType, Definition: "doc", Name: "editor", AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}}},
				{Kind: RemoveAllowedType, Definition: "doc", Name: "viewer", AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}}},
				{Kind: RemovePermission, Definition: "doc", Name: "edit"},
				{Kind: AddPermission, Definition: "doc", Name: "audit", Operation: schema.RelRef("viewer")},
			},
			[]string{
				"remove-permission doc#audit",
				"add-permission doc#edit",
				"add-allowed-type doc#viewer: group#member",
				"remove-allowed-type doc#editor: group#member",
			},
		},
		{
			"added union child",
			mutationBaseSchema,
			[]Mutation{{Kind: AddUnionChild, Definition: "doc", Name: "edit", Operation: schema.RelRef("viewer")}},
			[]string{"remove-union-child doc#edit"},
		},
		{
			"removed last union child",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveUnionChild, Definition: "doc", Name: "view", Operation: schema.RelRef("editor")}},
			[]string{"add-union-child doc#view"},
		},
		{
			"removed earlier union child",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveUnionChild, Definition: "doc", Name: "view", Operation: schema.RelRef("viewer")}},
			[]string{"change-permission doc#view"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			v, err := NewVariant(mustSchema(t, tc.base), tc.mutations)
			require.NoError(t, err)

			inverse := v.Inverse()
			require.Equal(t, schemaText(t, v.Schema), schemaText(t, inverse.Base))
			require.Equal(t, schemaText(t, v.Base), schemaText(t, inverse.Schema))

			found := []string{}
			for _, mutation := range inverse.Mutations {
				found = append(found, mutation.String())
			}
			require.Equal(t, tc.expected, found)

			// Applying the inverse mutations to the variant produces the base once more.
			reverted, err := NewVariant(v.Schema, inverse.Mutations)
			require.NoError(t, err)
			require.Equal(t, schemaText(t, v.Base), schemaText(t, reverted.Schema))
		})
	}
}

func TestInverseInvalidatesAddedRelationships(t *testing.T) {
	v, err := NewVariant(mustSchema(t, mutationBaseSchema), []Mutation{
		{Kind: AddRelation, Definition: "doc", Name: "auditor", AllowedTypes: []AllowedType{direct("user")}},
	})
	require.NoError(t, err)

	rel := tuple.MustParse("doc:a#auditor@user:b")
	require.Empty(t, v.Invalidated(rel))
	require.Len(t, v.Inverse().Invalidated(rel), 1)
}

func TestVariantWithoutRelationsGeneratesNoRelationships(t *testing.T) {
	v, err := NewVariant(mustSchema(t, `definition user {}`), nil)
	require.NoError(t, err)

	rapid.Check(t, func(t *rapid.T) {
		for rel := range v.Relationships.GenerateRelationships(t) {
			require.Failf(t, "found unexpected relationship", "%s", rel)
		}
	})
}

func TestNewVariantWithoutRelations(t *testing.T) {
	v, err := NewVariant(mustSchema(t, `
		definition user {}

		definition doc {
			relation viewer: user
		}`), []Mutation{{Kind: RemoveRelation, Definition: "doc", Name: "viewer"}})
	require.NoError(t, err)
	require.Empty(t, v.Schema.Definitions()["doc"].Relations())
}

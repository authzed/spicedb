package testing

import (
	"testing"

	"github.com/stretchr/testify/require"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/diff"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

const mutationBaseSchema = `
	definition user {}

	definition group {
		relation member: user
	}

	definition doc {
		relation viewer: user | group#member
		relation editor: user
		permission view = viewer + editor
		permission edit = editor
	}`

const mutationBaseSchemaWithFolder = mutationBaseSchema + `

	definition folder {
		relation owner: user
	}`

func direct(typeName string) AllowedType {
	return AllowedType{Type: typeName, Subrelation: tuple.Ellipsis}
}

func TestApplyMutationsEachKind(t *testing.T) {
	tcs := []struct {
		name      string
		base      string
		mutations []Mutation
		expected  string
	}{
		{
			"add definition",
			mutationBaseSchema,
			[]Mutation{{
				Kind:       AddDefinition,
				Definition: "folder",
				Members: []Mutation{
					{Kind: AddRelation, Definition: "folder", Name: "owner", AllowedTypes: []AllowedType{direct("user")}},
					{Kind: AddPermission, Definition: "folder", Name: "manage", Operation: schema.RelRef("owner")},
				},
			}},
			mutationBaseSchema + `

			definition folder {
				relation owner: user
				permission manage = owner
			}`,
		},
		{
			"remove definition",
			mutationBaseSchemaWithFolder,
			[]Mutation{{Kind: RemoveDefinition, Definition: "folder"}},
			mutationBaseSchema,
		},
		{
			"add relation",
			mutationBaseSchema,
			[]Mutation{{
				Kind: AddRelation, Definition: "doc", Name: "auditor",
				AllowedTypes: []AllowedType{direct("user"), {Type: "group", Subrelation: "member"}},
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				relation auditor: user | group#member
				permission view = viewer + editor
				permission edit = editor
			}`,
		},
		{
			"remove relation with cascaded permission change",
			mutationBaseSchema,
			[]Mutation{
				{Kind: RemoveRelation, Definition: "doc", Name: "viewer"},
				{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("editor")},
			},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation editor: user
				permission view = editor
				permission edit = editor
			}`,
		},
		{
			"add allowed type",
			mutationBaseSchema,
			[]Mutation{{
				Kind: AddAllowedType, Definition: "doc", Name: "editor",
				AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}},
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user | group#member
				permission view = viewer + editor
				permission edit = editor
			}`,
		},
		{
			"remove allowed type",
			mutationBaseSchema,
			[]Mutation{{
				Kind: RemoveAllowedType, Definition: "doc", Name: "viewer",
				AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}},
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user
				relation editor: user
				permission view = viewer + editor
				permission edit = editor
			}`,
		},
		{
			"add permission",
			mutationBaseSchema,
			[]Mutation{{
				Kind: AddPermission, Definition: "doc", Name: "audit",
				Operation: schema.Exclusion(schema.RelRef("viewer"), schema.RelRef("editor")),
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = viewer + editor
				permission edit = editor
				permission audit = viewer - editor
			}`,
		},
		{
			"remove permission",
			mutationBaseSchema,
			[]Mutation{{Kind: RemovePermission, Definition: "doc", Name: "edit"}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = viewer + editor
			}`,
		},
		{
			"change permission",
			mutationBaseSchema,
			[]Mutation{{
				Kind: ChangePermission, Definition: "doc", Name: "edit",
				Operation: schema.Intersection(schema.RelRef("viewer"), schema.RelRef("editor")),
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = viewer + editor
				permission edit = viewer & editor
			}`,
		},
		{
			"add union child to a union",
			mutationBaseSchema,
			[]Mutation{{
				Kind: AddUnionChild, Definition: "doc", Name: "view",
				Operation: schema.Intersection(schema.RelRef("viewer"), schema.RelRef("editor")),
			}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = viewer + editor + (viewer & editor)
				permission edit = editor
			}`,
		},
		{
			"add union child to a single relation",
			mutationBaseSchema,
			[]Mutation{{Kind: AddUnionChild, Definition: "doc", Name: "edit", Operation: schema.RelRef("viewer")}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = viewer + editor
				permission edit = editor + viewer
			}`,
		},
		{
			"remove union child",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveUnionChild, Definition: "doc", Name: "view", Operation: schema.RelRef("viewer")}},
			`
			definition user {}

			definition group {
				relation member: user
			}

			definition doc {
				relation viewer: user | group#member
				relation editor: user
				permission view = editor
				permission edit = editor
			}`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			applied, err := ApplyMutations(mustSchema(t, tc.base), tc.mutations)
			require.NoError(t, err)

			// NOTE: applying no mutations normalizes the expected schema in the same way.
			expected, err := ApplyMutations(mustSchema(t, tc.expected), nil)
			require.NoError(t, err)
			require.Equal(t, schemaText(t, expected), schemaText(t, applied))
		})
	}
}

func TestApplyMutationsSortsAllowedTypes(t *testing.T) {
	first, err := ApplyMutations(mustSchema(t, `
		definition user {}

		definition group {
			relation member: user
		}

		definition doc {
			relation viewer: user | group#member | group
		}`), nil)
	require.NoError(t, err)

	second, err := ApplyMutations(mustSchema(t, `
		definition user {}

		definition group {
			relation member: user
		}

		definition doc {
			relation viewer: group | group#member | user
		}`), nil)
	require.NoError(t, err)
	require.Equal(t, schemaText(t, first), schemaText(t, second))
}

func TestApplyMutationsZeroIsIdentity(t *testing.T) {
	base := mustSchema(t, mutationBaseSchema)
	applied, err := ApplyMutations(base, nil)
	require.NoError(t, err)

	// NOTE: the allowed types of each relation are sorted, so the text can differ.
	changes, err := diff.DiffSchemas(diffable(t, base), diffable(t, applied), caveattypes.Default.TypeSet)
	require.NoError(t, err)
	require.Empty(t, changes.ChangedNamespaces)
	require.Empty(t, changes.AddedNamespaces)
	require.Empty(t, changes.RemovedNamespaces)

	reapplied, err := ApplyMutations(applied, nil)
	require.NoError(t, err)
	require.Equal(t, schemaText(t, applied), schemaText(t, reapplied))
}

func TestApplyMutationsDeterministic(t *testing.T) {
	mutations := []Mutation{
		{Kind: AddRelation, Definition: "doc", Name: "auditor", AllowedTypes: []AllowedType{direct("user")}},
		{Kind: RemovePermission, Definition: "doc", Name: "edit"},
		{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("viewer")},
	}

	first, err := ApplyMutations(mustSchema(t, mutationBaseSchema), mutations)
	require.NoError(t, err)
	second, err := ApplyMutations(mustSchema(t, mutationBaseSchema), mutations)
	require.NoError(t, err)
	require.Equal(t, schemaText(t, first), schemaText(t, second))
}

func TestApplyMutationsDoesNotModifyBase(t *testing.T) {
	base := mustSchema(t, mutationBaseSchema)
	before := schemaText(t, base)

	_, err := ApplyMutations(base, []Mutation{
		{Kind: RemovePermission, Definition: "doc", Name: "edit"},
		{Kind: ChangePermission, Definition: "doc", Name: "view", Operation: schema.RelRef("viewer")},
	})
	require.NoError(t, err)
	require.Equal(t, before, schemaText(t, base))
}

func TestApplyMutationsErrors(t *testing.T) {
	tcs := []struct {
		name      string
		base      string
		mutations []Mutation
		expected  string
	}{
		{
			"remove missing relation",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveRelation, Definition: "doc", Name: "nope"}},
			"not found",
		},
		{
			"remove missing definition",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveDefinition, Definition: "nope"}},
			"not found",
		},
		{
			"add existing relation",
			mutationBaseSchema,
			[]Mutation{{Kind: AddRelation, Definition: "doc", Name: "viewer", AllowedTypes: []AllowedType{direct("user")}}},
			"already exists",
		},
		{
			"add permission with the name of a relation",
			mutationBaseSchema,
			[]Mutation{{Kind: AddPermission, Definition: "doc", Name: "viewer", Operation: schema.RelRef("editor")}},
			"already exists",
		},
		{
			"remove relation still referenced by a permission",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveRelation, Definition: "doc", Name: "viewer"}},
			"viewer",
		},
		{
			"remove definition still referenced as a subject type",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveDefinition, Definition: "group"}},
			"group",
		},
		{
			"remove last allowed type",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveAllowedType, Definition: "doc", Name: "editor", AllowedTypes: []AllowedType{direct("user")}}},
			"last allowed type",
		},
		{
			"remove missing allowed type",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveAllowedType, Definition: "doc", Name: "viewer", AllowedTypes: []AllowedType{direct("folder")}}},
			"not found",
		},
		{
			"overlapping targets",
			mutationBaseSchema,
			[]Mutation{
				{Kind: ChangePermission, Definition: "doc", Name: "edit", Operation: schema.RelRef("viewer")},
				{Kind: RemovePermission, Definition: "doc", Name: "edit"},
			},
			"overlapping",
		},
		{
			"remove union child from a permission that is not a union",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveUnionChild, Definition: "doc", Name: "edit", Operation: schema.RelRef("editor")}},
			"not a union",
		},
		{
			"remove missing union child",
			mutationBaseSchema,
			[]Mutation{{Kind: RemoveUnionChild, Definition: "doc", Name: "view", Operation: schema.RelRef("auditor")}},
			"union child not found",
		},
		{
			"add union child to missing permission",
			mutationBaseSchema,
			[]Mutation{{Kind: AddUnionChild, Definition: "doc", Name: "nope", Operation: schema.RelRef("editor")}},
			"not found",
		},
		{
			"unsupported base",
			`caveat somecaveat(x int) { x == 1 }

			definition user {}

			definition doc {
				relation viewer: user with somecaveat
			}`,
			nil,
			"unsupported schema feature",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ApplyMutations(mustSchema(t, tc.base), tc.mutations)
			require.ErrorContains(t, err, tc.expected)
		})
	}
}

func TestMutationStrings(t *testing.T) {
	require.Equal(t, "remove-relation doc#viewer", Mutation{Kind: RemoveRelation, Definition: "doc", Name: "viewer"}.String())
	require.Equal(t, "remove-definition doc", Mutation{Kind: RemoveDefinition, Definition: "doc"}.String())
	require.Equal(t, "remove-allowed-type doc#viewer: group#member", Mutation{
		Kind: RemoveAllowedType, Definition: "doc", Name: "viewer",
		AllowedTypes: []AllowedType{{Type: "group", Subrelation: "member"}},
	}.String())
	require.Equal(t, "add-allowed-type doc#viewer: user", Mutation{
		Kind: AddAllowedType, Definition: "doc", Name: "viewer",
		AllowedTypes: []AllowedType{direct("user")},
	}.String())

	require.True(t, Mutation{Kind: RemoveDefinition}.BreaksData())
	require.True(t, Mutation{Kind: RemoveRelation}.BreaksData())
	require.True(t, Mutation{Kind: RemoveAllowedType}.BreaksData())
	require.False(t, Mutation{Kind: AddRelation}.BreaksData())
	require.False(t, Mutation{Kind: ChangePermission}.BreaksData())
}

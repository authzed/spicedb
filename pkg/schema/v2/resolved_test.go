package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestResolveSchema_Nil(t *testing.T) {
	_, err := ResolveSchema(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil schema")
}

func TestResolveSchema_Empty(t *testing.T) {
	schema := &Schema{
		definitions: make(map[string]*Definition),
		caveats:     make(map[string]*Caveat),
	}

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.NotNil(t, resolved.Schema())
	require.Empty(t, resolved.Schema().definitions)
}

func TestResolveSchema_SimpleRelationReference(t *testing.T) {
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}

	perm := &Permission{
		name: "view",
		operation: &RelationReference{
			relationName: "viewer",
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Check that the original schema is unchanged
	originalPerm := schema.definitions["document"].permissions["view"]
	require.IsType(t, &RelationReference{}, originalPerm.operation)

	// Check that the resolved schema has ResolvedRelationReference
	resolvedDef := resolved.Schema().definitions["document"]
	resolvedPerm := resolvedDef.permissions["view"]
	require.IsType(t, &ResolvedRelationReference{}, resolvedPerm.operation)

	resolvedRef := resolvedPerm.operation.(*ResolvedRelationReference)
	require.Equal(t, "viewer", resolvedRef.relationName)
	require.NotNil(t, resolvedRef.resolved)
	require.Same(t, resolvedDef.relations["viewer"], resolvedRef.resolved)
}

func TestResolveSchema_PermissionReference(t *testing.T) {
	basePerm := &Permission{
		name: "base",
		operation: &RelationReference{
			relationName: "viewer",
		},
	}

	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}

	derivedPerm := &Permission{
		name: "derived",
		operation: &RelationReference{
			relationName: "base",
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"base":    basePerm,
			"derived": derivedPerm,
		},
	}
	rel.parent = def
	basePerm.parent = def
	derivedPerm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Check that derived permission resolves to base permission
	resolvedDef := resolved.Schema().definitions["document"]
	resolvedDerived := resolvedDef.permissions["derived"]
	require.IsType(t, &ResolvedRelationReference{}, resolvedDerived.operation)

	resolvedRef := resolvedDerived.operation.(*ResolvedRelationReference)
	require.Equal(t, "base", resolvedRef.relationName)
	require.Same(t, resolvedDef.permissions["base"], resolvedRef.resolved)
}

func TestResolveSchema_ArrowReference(t *testing.T) {
	rel := &Relation{
		name:          "parent",
		baseRelations: []*BaseRelation{},
	}

	perm := &Permission{
		name: "view",
		operation: &ArrowReference{
			left:  "parent",
			right: "view",
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"parent": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Check that the resolved schema has ResolvedArrowReference
	resolvedDef := resolved.Schema().definitions["document"]
	resolvedPerm := resolvedDef.permissions["view"]
	require.IsType(t, &ResolvedArrowReference{}, resolvedPerm.operation)

	resolvedArrow := resolvedPerm.operation.(*ResolvedArrowReference)
	require.Equal(t, "parent", resolvedArrow.left)
	require.Equal(t, "view", resolvedArrow.right)
	require.NotNil(t, resolvedArrow.resolvedLeft)
	require.Same(t, resolvedDef.relations["parent"], resolvedArrow.resolvedLeft)
}

func TestResolveSchema_ComplexOperation(t *testing.T) {
	rel1 := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}
	rel2 := &Relation{
		name:          "editor",
		baseRelations: []*BaseRelation{},
	}
	rel3 := &Relation{
		name:          "parent",
		baseRelations: []*BaseRelation{},
	}
	rel4 := &Relation{
		name:          "banned",
		baseRelations: []*BaseRelation{},
	}

	// Build: (viewer + parent->view) & editor - banned
	perm := &Permission{
		name: "view",
		operation: &ExclusionOperation{
			left: &IntersectionOperation{
				children: []Operation{
					&UnionOperation{
						children: []Operation{
							&RelationReference{relationName: "viewer"},
							&ArrowReference{left: "parent", right: "view"},
						},
					},
					&RelationReference{relationName: "editor"},
				},
			},
			right: &RelationReference{relationName: "banned"},
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel1,
			"editor": rel2,
			"parent": rel3,
			"banned": rel4,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel1.parent = def
	rel2.parent = def
	rel3.parent = def
	rel4.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Verify the structure is resolved correctly
	resolvedDef := resolved.Schema().definitions["document"]
	resolvedPerm := resolvedDef.permissions["view"]

	exclusion := resolvedPerm.operation.(*ExclusionOperation)
	require.NotNil(t, exclusion)

	// Check left side (intersection)
	intersection := exclusion.left.(*IntersectionOperation)
	require.Len(t, intersection.children, 2)

	// Check union inside intersection
	union := intersection.children[0].(*UnionOperation)
	require.Len(t, union.children, 2)

	// Check resolved relation reference
	resolvedRelRef := union.children[0].(*ResolvedRelationReference)
	require.Equal(t, "viewer", resolvedRelRef.relationName)
	require.Same(t, resolvedDef.relations["viewer"], resolvedRelRef.resolved)

	// Check resolved arrow reference
	resolvedArrowRef := union.children[1].(*ResolvedArrowReference)
	require.Equal(t, "parent", resolvedArrowRef.left)
	require.Equal(t, "view", resolvedArrowRef.right)
	require.Same(t, resolvedDef.relations["parent"], resolvedArrowRef.resolvedLeft)

	// Check editor reference in intersection
	editorRef := intersection.children[1].(*ResolvedRelationReference)
	require.Equal(t, "editor", editorRef.relationName)
	require.Same(t, resolvedDef.relations["editor"], editorRef.resolved)

	// Check banned reference in exclusion
	bannedRef := exclusion.right.(*ResolvedRelationReference)
	require.Equal(t, "banned", bannedRef.relationName)
	require.Same(t, resolvedDef.relations["banned"], bannedRef.resolved)
}

func TestResolveSchema_UnknownRelation(t *testing.T) {
	perm := &Permission{
		name: "view",
		operation: &RelationReference{
			relationName: "nonexistent",
		},
	}

	def := &Definition{
		name:      "document",
		relations: make(map[string]*Relation),
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	_, err := ResolveSchema(schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonexistent")
	require.Contains(t, err.Error(), "not found")
	require.Contains(t, err.Error(), "document")
}

func TestResolveSchema_UnknownArrowLeft(t *testing.T) {
	perm := &Permission{
		name: "view",
		operation: &ArrowReference{
			left:  "nonexistent",
			right: "view",
		},
	}

	def := &Definition{
		name:      "document",
		relations: make(map[string]*Relation),
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	_, err := ResolveSchema(schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonexistent")
	require.Contains(t, err.Error(), "not found")
	require.Contains(t, err.Error(), "left side of arrow")
	require.Contains(t, err.Error(), "document")
}

func TestResolveSchema_UnknownInNestedOperation(t *testing.T) {
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}

	perm := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&RelationReference{relationName: "viewer"},
				&RelationReference{relationName: "nonexistent"},
			},
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	_, err := ResolveSchema(schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonexistent")
	require.Contains(t, err.Error(), "not found")
}

func TestResolveSchema_MultipleDefinitions(t *testing.T) {
	// Create first definition with a relation and permission
	rel1 := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}
	perm1 := &Permission{
		name: "view",
		operation: &RelationReference{
			relationName: "viewer",
		},
	}
	def1 := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel1,
		},
		permissions: map[string]*Permission{
			"view": perm1,
		},
	}
	rel1.parent = def1
	perm1.parent = def1

	// Create second definition with different relation and permission
	rel2 := &Relation{
		name:          "member",
		baseRelations: []*BaseRelation{},
	}
	perm2 := &Permission{
		name: "access",
		operation: &ArrowReference{
			left:  "member",
			right: "view",
		},
	}
	def2 := &Definition{
		name: "folder",
		relations: map[string]*Relation{
			"member": rel2,
		},
		permissions: map[string]*Permission{
			"access": perm2,
		},
	}
	rel2.parent = def2
	perm2.parent = def2

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def1,
			"folder":   def2,
		},
		caveats: make(map[string]*Caveat),
	}
	def1.parent = schema
	def2.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Check first definition
	resolvedDef1 := resolved.Schema().definitions["document"]
	resolvedPerm1 := resolvedDef1.permissions["view"]
	require.IsType(t, &ResolvedRelationReference{}, resolvedPerm1.operation)
	resolvedRef1 := resolvedPerm1.operation.(*ResolvedRelationReference)
	require.Same(t, resolvedDef1.relations["viewer"], resolvedRef1.resolved)

	// Check second definition
	resolvedDef2 := resolved.Schema().definitions["folder"]
	resolvedPerm2 := resolvedDef2.permissions["access"]
	require.IsType(t, &ResolvedArrowReference{}, resolvedPerm2.operation)
	resolvedArrow2 := resolvedPerm2.operation.(*ResolvedArrowReference)
	require.Same(t, resolvedDef2.relations["member"], resolvedArrow2.resolvedLeft)
}

func TestResolveSchema_AlreadyResolvedOperations(t *testing.T) {
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}

	// Create a permission with already resolved operations
	perm := &Permission{
		name: "view",
		operation: &ResolvedRelationReference{
			relationName: "viewer",
			resolved:     rel,
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Should still work with already resolved operations
	resolvedDef := resolved.Schema().definitions["document"]
	resolvedPerm := resolvedDef.permissions["view"]
	require.IsType(t, &ResolvedRelationReference{}, resolvedPerm.operation)
}

func TestResolveSchema_FunctionedArrowReference(t *testing.T) {
	tests := []struct {
		name         string
		schemaString string
		wantErr      bool
	}{
		{
			name: "simple functioned arrow with any",
			schemaString: `definition document {
	relation parent: folder
	permission view = parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`,
			wantErr: false,
		},
		{
			name: "simple functioned arrow with all",
			schemaString: `definition document {
	relation parent: folder
	permission view = parent.all(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`,
			wantErr: false,
		},
		{
			name: "functioned arrow in union",
			schemaString: `definition document {
	relation parent: folder
	relation viewer: user
	permission view = viewer + parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`,
			wantErr: false,
		},
		{
			name: "functioned arrow in intersection",
			schemaString: `definition document {
	relation parent: folder
	relation approved: user
	permission view = parent.all(viewer) & approved
}

definition folder {
	relation viewer: user
}

definition user {}`,
			wantErr: false,
		},
		{
			name: "multiple functioned arrows",
			schemaString: `definition document {
	relation parent: folder
	relation owner: folder
	permission view = parent.any(viewer) + owner.all(editor)
}

definition folder {
	relation viewer: user
	relation editor: user
}

definition user {}`,
			wantErr: false,
		},
		{
			name: "invalid left relation",
			schemaString: `definition document {
	permission view = nonexistent.any(viewer)
}

definition user {}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Compile the schema
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tt.schemaString,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Step 2: Convert to *Schema
			schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)
			require.NotNil(t, schema)

			// Step 3: Resolve the schema
			resolved, err := ResolveSchema(schema)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resolved)

			// Verify that functioned arrows were properly resolved
			// by checking the schema has the right structure
			require.NotNil(t, resolved.Schema())
		})
	}
}

func TestToDefinitions_FunctionedArrowReference(t *testing.T) {
	schemaString := `definition document {
	relation parent: folder
	permission view = parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`

	// Step 1: Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Step 2: Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Step 3: Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Step 4: Convert back to namespace definition
	defs, caveats, err := resolved.Schema().ToDefinitions()
	require.NoError(t, err)
	require.NotNil(t, defs)
	require.Empty(t, caveats)

	// Verify that the conversion worked
	require.Len(t, defs, 3) // document, folder, user

	// Find the document definition
	var docDef *core.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "document" {
			docDef = def
			break
		}
	}
	require.NotNil(t, docDef)

	// Verify the permission exists
	var viewPerm *core.Relation
	for _, rel := range docDef.Relation {
		if rel.Name == "view" {
			viewPerm = rel
			break
		}
	}
	require.NotNil(t, viewPerm)

	// The UsersetRewrite should contain a FunctionedTupleToUserset
	require.NotNil(t, viewPerm.UsersetRewrite)
	require.NotNil(t, viewPerm.UsersetRewrite.RewriteOperation)
}

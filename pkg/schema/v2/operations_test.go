package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuilderSetsParentsCorrectly verifies that the fluent builder properly sets parent relationships
// for all elements created during construction. This ensures that the entire schema hierarchy is
// connected and can be traversed upward from any element.
func TestBuilderSetsParentsCorrectly(t *testing.T) {
	t.Parallel()

	// Build a comprehensive schema using the fluent builder
	schema := NewSchemaBuilder().
		AddDefinition("user").
		Done().
		AddDefinition("folder").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddRelation("editor").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			RelRef("owner"),
			ArrowRef("parent", "view"),
			RelRef("viewer"),
		).
		Done().
		AddPermission("edit").
		Intersection(
			RelRef("editor"),
			RelRef("owner"),
		).
		Done().
		AddPermission("admin").
		Exclusion(
			RelRef("owner"),
			RelRef("viewer"),
		).
		Done().
		Done().
		AddDefinition("document").
		AddRelation("folder").
		AllowedDirectRelation("folder").
		Done().
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Arrow("folder", "view").
		Done().
		Done().
		AddCaveat("has_access").
		Expression("resource == 42").
		Parameter("resource", CaveatTypeInt).
		Done().
		Build()

	// Verify Schema → Definition parent relationships
	folderDef, ok := schema.GetTypeDefinition("folder")
	require.True(t, ok, "folder definition should exist")
	require.NotNil(t, folderDef.Parent(), "folder definition should have a parent")
	require.Equal(t, schema, folderDef.Parent(), "folder definition's parent should be the schema")

	userDef, ok := schema.GetTypeDefinition("user")
	require.True(t, ok, "user definition should exist")
	require.NotNil(t, userDef.Parent(), "user definition should have a parent")
	require.Equal(t, schema, userDef.Parent(), "user definition's parent should be the schema")

	docDef, ok := schema.GetTypeDefinition("document")
	require.True(t, ok, "document definition should exist")
	require.NotNil(t, docDef.Parent(), "document definition should have a parent")
	require.Equal(t, schema, docDef.Parent(), "document definition's parent should be the schema")

	// Verify Definition → Relation parent relationships
	parentRel, ok := folderDef.GetRelation("parent")
	require.True(t, ok, "parent relation should exist")
	require.NotNil(t, parentRel.Parent(), "parent relation should have a parent")
	require.Equal(t, folderDef, parentRel.Parent(), "parent relation's parent should be the folder definition")

	ownerRel, ok := folderDef.GetRelation("owner")
	require.True(t, ok, "owner relation should exist")
	require.NotNil(t, ownerRel.Parent(), "owner relation should have a parent")
	require.Equal(t, folderDef, ownerRel.Parent(), "owner relation's parent should be the folder definition")

	// Verify Relation → BaseRelation parent relationships
	baseRelations := ownerRel.BaseRelations()
	require.NotEmpty(t, baseRelations, "owner relation should have base relations")
	for _, baseRel := range baseRelations {
		require.NotNil(t, baseRel.Parent(), "base relation should have a parent")
		require.Equal(t, ownerRel, baseRel.Parent(), "base relation's parent should be the owner relation")
	}

	// Verify Definition → Permission parent relationships
	viewPerm, ok := folderDef.GetPermission("view")
	require.True(t, ok, "view permission should exist")
	require.NotNil(t, viewPerm.Parent(), "view permission should have a parent")
	require.Equal(t, folderDef, viewPerm.Parent(), "view permission's parent should be the folder definition")

	editPerm, ok := folderDef.GetPermission("edit")
	require.True(t, ok, "edit permission should exist")
	require.NotNil(t, editPerm.Parent(), "edit permission should have a parent")
	require.Equal(t, folderDef, editPerm.Parent(), "edit permission's parent should be the folder definition")

	adminPerm, ok := folderDef.GetPermission("admin")
	require.True(t, ok, "admin permission should exist")
	require.NotNil(t, adminPerm.Parent(), "admin permission should have a parent")
	require.Equal(t, folderDef, adminPerm.Parent(), "admin permission's parent should be the folder definition")

	// Verify Permission → Operation (root) parent relationships
	viewOp := viewPerm.Operation()
	require.NotNil(t, viewOp, "view permission should have an operation")
	require.NotNil(t, viewOp.Parent(), "root operation should have a parent")
	require.Equal(t, viewPerm, viewOp.Parent(), "root operation's parent should be the permission")

	editOp := editPerm.Operation()
	require.NotNil(t, editOp, "edit permission should have an operation")
	require.NotNil(t, editOp.Parent(), "root operation should have a parent")
	require.Equal(t, editPerm, editOp.Parent(), "root operation's parent should be the permission")

	adminOp := adminPerm.Operation()
	require.NotNil(t, adminOp, "admin permission should have an operation")
	require.NotNil(t, adminOp.Parent(), "root operation should have a parent")
	require.Equal(t, adminPerm, adminOp.Parent(), "root operation's parent should be the permission")

	// Verify Operation → child operations for UnionOperation
	unionOp, ok := viewOp.(*UnionOperation)
	require.True(t, ok, "view operation should be a UnionOperation")
	unionChildren := unionOp.Children()
	require.Len(t, unionChildren, 3, "union should have 3 children")
	for i, child := range unionChildren {
		require.NotNil(t, child, "union child %d should not be nil", i)
		require.NotNil(t, child.Parent(), "union child %d should have a parent", i)
		require.Equal(t, unionOp, child.Parent(), "union child %d's parent should be the union operation", i)
	}

	// Verify Operation → child operations for IntersectionOperation
	intersectionOp, ok := editOp.(*IntersectionOperation)
	require.True(t, ok, "edit operation should be an IntersectionOperation")
	intersectionChildren := intersectionOp.Children()
	require.Len(t, intersectionChildren, 2, "intersection should have 2 children")
	for i, child := range intersectionChildren {
		require.NotNil(t, child, "intersection child %d should not be nil", i)
		require.NotNil(t, child.Parent(), "intersection child %d should have a parent", i)
		require.Equal(t, intersectionOp, child.Parent(), "intersection child %d's parent should be the intersection operation", i)
	}

	// Verify Operation → child operations for ExclusionOperation
	exclusionOp, ok := adminOp.(*ExclusionOperation)
	require.True(t, ok, "admin operation should be an ExclusionOperation")
	leftChild := exclusionOp.Left()
	require.NotNil(t, leftChild, "exclusion left child should not be nil")
	require.NotNil(t, leftChild.Parent(), "exclusion left child should have a parent")
	require.Equal(t, exclusionOp, leftChild.Parent(), "exclusion left child's parent should be the exclusion operation")

	rightChild := exclusionOp.Right()
	require.NotNil(t, rightChild, "exclusion right child should not be nil")
	require.NotNil(t, rightChild.Parent(), "exclusion right child should have a parent")
	require.Equal(t, exclusionOp, rightChild.Parent(), "exclusion right child's parent should be the exclusion operation")

	// Verify leaf operation types (RelationReference, ArrowReference)
	relRef, ok := unionChildren[0].(*RelationReference)
	require.True(t, ok, "first union child should be a RelationReference")
	require.Equal(t, "owner", relRef.RelationName(), "relation reference should point to owner")

	arrowRef, ok := unionChildren[1].(*ArrowReference)
	require.True(t, ok, "second union child should be an ArrowReference")
	require.Equal(t, "parent", arrowRef.Left(), "arrow reference left should be parent")
	require.Equal(t, "view", arrowRef.Right(), "arrow reference right should be view")

	// Test simple permission with direct operation (not composite)
	docViewPerm, ok := docDef.GetPermission("view")
	require.True(t, ok, "document view permission should exist")
	docViewOp := docViewPerm.Operation()
	require.NotNil(t, docViewOp, "document view permission should have an operation")
	require.NotNil(t, docViewOp.Parent(), "document view operation should have a parent")
	require.Equal(t, docViewPerm, docViewOp.Parent(), "document view operation's parent should be the permission")

	// Verify Schema → Caveat parent relationships
	caveat, ok := schema.Caveats()["has_access"]
	require.True(t, ok, "has_access caveat should exist")
	require.NotNil(t, caveat.Parent(), "caveat should have a parent")
	require.Equal(t, schema, caveat.Parent(), "caveat's parent should be the schema")

	// Verify we can traverse from a deeply nested operation all the way up to the schema
	deepChild := unionChildren[0] // RelationReference within Union within Permission within Definition within Schema
	current := deepChild.Parent() // Should be UnionOperation
	require.Equal(t, unionOp, current, "first parent should be the union operation")

	current = current.Parent() // Should be Permission
	require.Equal(t, viewPerm, current, "second parent should be the view permission")

	current = current.Parent() // Should be Definition
	require.Equal(t, folderDef, current, "third parent should be the folder definition")

	current = current.Parent() // Should be Schema
	require.Equal(t, schema, current, "fourth parent should be the schema")

	current = current.Parent() // Should be nil (top of hierarchy)
	require.Nil(t, current, "schema should have no parent (top of hierarchy)")

	// Test SelfReference and NilReference setParent functionality
	// These operation types are typically created when converting from proto definitions
	// but we can test them directly to ensure their setParent methods work correctly

	// Test SelfReference
	selfRef := &SelfReference{}
	require.Nil(t, selfRef.Parent(), "SelfReference should have no parent initially")

	// Create a permission to use as parent for testing
	testPerm := &Permission{
		parent:    folderDef,
		name:      "test_self",
		synthetic: false,
	}

	// Use setParent to set the parent
	selfRef.setParent(testPerm)
	require.NotNil(t, selfRef.Parent(), "SelfReference should have a parent after setParent")
	require.Equal(t, testPerm, selfRef.Parent(), "SelfReference parent should be the test permission")

	// Test that SelfReference can be part of a composite operation with proper parent
	testUnion := &UnionOperation{
		children: []Operation{selfRef, RelRef("viewer")},
	}
	setChildrenParent(testUnion.children, testUnion)
	require.Equal(t, testUnion, selfRef.Parent(), "SelfReference parent should be the union operation")

	// Test NilReference
	nilRef := &NilReference{}
	require.Nil(t, nilRef.Parent(), "NilReference should have no parent initially")

	// Create another permission to use as parent
	testPerm2 := &Permission{
		parent:    folderDef,
		name:      "test_nil",
		synthetic: false,
	}

	// Use setParent to set the parent
	nilRef.setParent(testPerm2)
	require.NotNil(t, nilRef.Parent(), "NilReference should have a parent after setParent")
	require.Equal(t, testPerm2, nilRef.Parent(), "NilReference parent should be the test permission")

	// Test that NilReference can be part of a composite operation with proper parent
	testIntersection := &IntersectionOperation{
		children: []Operation{nilRef, RelRef("editor")},
	}
	setChildrenParent(testIntersection.children, testIntersection)
	require.Equal(t, testIntersection, nilRef.Parent(), "NilReference parent should be the intersection operation")
}

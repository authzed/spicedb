package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// recursionTracker tracks visited permissions to detect potential infinite recursion
type recursionTracker struct {
	visitedPermissions []string
	visitedRelations   []string
	maxDepth           int
	currentDepth       int
}

func (rt *recursionTracker) VisitPermission(p *Permission, depth int) (int, bool, error) {
	rt.currentDepth = depth
	if depth > rt.maxDepth {
		rt.maxDepth = depth
	}

	// Track the permission name with its parent definition
	if p.parent != nil {
		permPath := p.parent.Name() + "#" + p.Name()
		rt.visitedPermissions = append(rt.visitedPermissions, permPath)
	} else {
		rt.visitedPermissions = append(rt.visitedPermissions, p.Name())
	}

	return depth + 1, true, nil
}

func (rt *recursionTracker) VisitRelation(r *Relation, depth int) (int, bool, error) {
	if depth > rt.maxDepth {
		rt.maxDepth = depth
	}

	// Track the relation name with its parent definition
	if r.parent != nil {
		relPath := r.parent.Name() + "#" + r.Name()
		rt.visitedRelations = append(rt.visitedRelations, relPath)
	} else {
		rt.visitedRelations = append(rt.visitedRelations, r.Name())
	}

	return depth + 1, true, nil
}

// TestWalkPostOrder_RecursiveGroupRelation tests that the walker can handle
// a recursive relation (group can have group members) without infinite recursion.
func TestWalkPostOrder_RecursiveGroupRelation(t *testing.T) {
	schemaString := `definition user {}

definition group {
	// Groups can contain other groups - this creates a potential for cycles
	relation member: user | group

	// Permission that checks membership
	permission is_member = member
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema (required for post-order traversal with arrow targets)
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with post-order traversal
	opts := NewWalkOptions().WithStrategy(WalkPostOrder).MustBuild()
	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have visited the schema without infinite recursion
	require.NotEmpty(t, tracker.visitedRelations, "Should have visited at least one relation")
	require.Contains(t, tracker.visitedRelations, "group#member", "Should have visited the member relation")

	// Check that we didn't get stuck in infinite recursion
	require.Equal(t, 2, tracker.maxDepth, "Depth should not indicate infinite recursion")
}

// TestWalkPostOrder_MutuallyRecursivePermissions tests that the walker can handle
// permissions that reference each other, creating a cycle.
func TestWalkPostOrder_MutuallyRecursivePermissions(t *testing.T) {
	schemaString := `definition user {}

definition document {
	relation owner: user

	// These permissions create a mutual recursion cycle
	permission view = edit
	permission edit = view
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with post-order traversal
	opts := NewWalkOptions().WithStrategy(WalkPostOrder).MustBuild()
	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have completed without infinite recursion
	require.NotEmpty(t, tracker.visitedPermissions, "Should have visited at least one permission")

	// Check that we visited the mutually recursive permissions
	require.True(t, containsPermission(tracker.visitedPermissions, "view"), "Should have visited view permission")
	require.True(t, containsPermission(tracker.visitedPermissions, "edit"), "Should have visited edit permission")

	// Check depth - this will reveal if there's cycle detection or if it walks the cycle multiple times
	require.Equal(t, 4, tracker.maxDepth, "Depth should not indicate infinite recursion")

	// Count how many times each permission was visited
	viewCount := countOccurrences(tracker.visitedPermissions, "view")
	editCount := countOccurrences(tracker.visitedPermissions, "edit")

	// If there's proper cycle detection, each should be visited once
	// If not, they might be visited multiple times (but should still terminate)
	// The key is that it should NOT infinite loop
	require.Positive(t, viewCount, "view should be visited at least once")
	require.Positive(t, editCount, "edit should be visited at least once")
}

// TestWalkPreOrder_RecursiveGroupRelation tests pre-order with recursive group relation
func TestWalkPreOrder_RecursiveGroupRelation(t *testing.T) {
	schemaString := `definition user {}

definition group {
	// Groups can contain other groups - this creates a potential for cycles
	relation member: user | group

	// Permission that checks membership
	permission is_member = member
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with PRE-ORDER traversal (default)
	opts := NewWalkOptions().WithStrategy(WalkPreOrder).MustBuild()
	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have visited the schema without infinite recursion
	require.NotEmpty(t, tracker.visitedRelations, "Should have visited at least one relation")
	require.Contains(t, tracker.visitedRelations, "group#member", "Should have visited the member relation")

	// Check that we didn't get stuck in infinite recursion
	require.Equal(t, 1, tracker.maxDepth, "Depth should not indicate infinite recursion")
}

// TestWalkPreOrder_MutuallyRecursivePermissions tests if pre-order also has infinite recursion
func TestWalkPreOrder_MutuallyRecursivePermissions(t *testing.T) {
	schemaString := `definition user {}

definition document {
	relation owner: user

	// These permissions create a mutual recursion cycle
	permission view = edit
	permission edit = view

	// This creates another cycle through owner
	permission delete = owner + edit
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with PRE-ORDER traversal (default)
	opts := NewWalkOptions().WithStrategy(WalkPreOrder).MustBuild()
	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have completed without infinite recursion
	require.NotEmpty(t, tracker.visitedPermissions, "Should have visited at least one permission")

	// Check that we visited the mutually recursive permissions
	require.True(t, containsPermission(tracker.visitedPermissions, "view"), "Should have visited view permission")
	require.True(t, containsPermission(tracker.visitedPermissions, "edit"), "Should have visited edit permission")

	// Check depth
	require.Less(t, tracker.maxDepth, 100, "Depth should not indicate infinite recursion")

	// Count how many times each permission was visited
	viewCount := countOccurrences(tracker.visitedPermissions, "view")
	editCount := countOccurrences(tracker.visitedPermissions, "edit")

	require.Positive(t, viewCount, "view should be visited at least once")
	require.Positive(t, editCount, "edit should be visited at least once")
}

// TestWalkPostOrder_ComplexRecursionWithArrows tests recursion through arrow references
// This tests the explicit cycle detection in ResolvedArrowReference
func TestWalkPostOrder_ComplexRecursionWithArrows(t *testing.T) {
	schemaString := `definition user {}

definition folder {
	relation parent: folder
	relation viewer: user

	// This creates potential for cycles through the folder hierarchy
	permission view = viewer + parent->view
}

definition document {
	relation parent: folder
	relation owner: user

	// This references the folder's view permission, which itself can recurse
	permission view = owner + parent->view
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema (required for arrow traversal)
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with post-order traversal AND arrow target traversal enabled
	// This should trigger the cycle detection in the arrow handling code
	opts := NewWalkOptions().
		WithStrategy(WalkPostOrder).
		WithTraverseArrowTargets(resolved.Schema()).MustBuild()

	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have completed without infinite recursion
	require.NotEmpty(t, tracker.visitedPermissions, "Should have visited at least one permission")

	// Check that we visited the view permissions in both definitions
	require.True(t, containsPermission(tracker.visitedPermissions, "folder#view"),
		"Should have visited folder#view permission")
	require.True(t, containsPermission(tracker.visitedPermissions, "document#view"),
		"Should have visited document#view permission")

	// Verify no infinite recursion
	require.Less(t, 2, tracker.maxDepth, "Depth should not indicate infinite recursion")

	// The arrow cycle detection should prevent folder#view from being visited multiple times
	// when following the parent->view arrow
	viewCount := countOccurrences(tracker.visitedPermissions, "folder#view")

	// With proper cycle detection, it should be visited a reasonable number of times
	require.Equal(t, 2, viewCount, "folder#view should not be visited excessive times due to cycle detection")
}

// TestWalkPostOrder_DeeplyNestedRecursion tests a more complex scenario with multiple levels
func TestWalkPostOrder_DeeplyNestedRecursion(t *testing.T) {
	schemaString := `definition user {}

definition resource {
	relation parent: resource
	relation owner: user
	relation viewer: user

	// Multiple recursive paths
	permission view = viewer + parent->view + parent->edit
	permission edit = owner + parent->view
	permission delete = edit + view
}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Track what gets visited
	tracker := &recursionTracker{}

	// Walk with post-order traversal with arrow traversal
	opts := NewWalkOptions().
		WithStrategy(WalkPostOrder).
		WithTraverseArrowTargets(resolved.Schema()).MustBuild()

	_, err = WalkSchemaWithOptions(resolved.Schema(), tracker, 0, opts)
	require.NoError(t, err)

	// The walker should have completed without infinite recursion
	require.NotEmpty(t, tracker.visitedPermissions, "Should have visited at least one permission")

	// Verify all permissions were visited
	require.True(t, containsPermission(tracker.visitedPermissions, "view"), "Should have visited view")
	require.True(t, containsPermission(tracker.visitedPermissions, "edit"), "Should have visited edit")
	require.True(t, containsPermission(tracker.visitedPermissions, "delete"), "Should have visited delete")

	// Check for reasonable depth
	require.Equal(t, 9, tracker.maxDepth, "Depth should not indicate infinite recursion")
}

// Helper functions

func containsPermission(permissions []string, name string) bool {
	for _, p := range permissions {
		if strings.Contains(p, name) {
			return true
		}
	}
	return false
}

func countOccurrences(slice []string, substr string) int {
	count := 0
	for _, s := range slice {
		if strings.Contains(s, substr) {
			count++
		}
	}
	return count
}

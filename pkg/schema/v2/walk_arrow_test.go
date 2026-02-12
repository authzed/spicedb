package schema

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// arrowTraversalVisitor tracks the order of visits for testing arrow traversal
type arrowTraversalVisitor struct {
	visitOrder []string
}

func (v *arrowTraversalVisitor) VisitPermission(p *Permission, value struct{}) (struct{}, bool, error) {
	defName := p.TypedParent().Name()
	v.visitOrder = append(v.visitOrder, defName+"#"+p.Name())
	return value, true, nil
}

func (v *arrowTraversalVisitor) VisitRelation(r *Relation, value struct{}) (struct{}, bool, error) {
	defName := r.TypedParent().Name()
	v.visitOrder = append(v.visitOrder, defName+"#"+r.Name())
	return value, true, nil
}

func (v *arrowTraversalVisitor) VisitResolvedArrowReference(rar *ResolvedArrowReference, value struct{}) (struct{}, error) {
	defName := FindParent[*Definition](rar).Name()
	v.visitOrder = append(v.visitOrder, defName+"#"+rar.Left()+"->"+rar.Right())
	return value, nil
}

func TestTraverseArrowTargets_Basic(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	// With arrow traversal enabled, folder#view should be visited BEFORE document's parent->view arrow
	// because the arrow targets folder#view
	t.Logf("Visit order: %v", visitor.visitOrder)

	// Find the FIRST occurrence of folder#view (via arrow traversal) and the arrow itself
	firstFolderViewIndex := slices.Index(visitor.visitOrder, "folder#view")
	arrowRefIndex := slices.Index(visitor.visitOrder, "document#parent->view")

	require.Contains(t, visitor.visitOrder, "folder#view", "folder#view should be visited")
	require.Contains(t, visitor.visitOrder, "document#parent->view", "document arrow should be visited")
	require.Less(t, firstFolderViewIndex, arrowRefIndex, "folder#view should be visited BEFORE document arrow (PostOrder with TraverseArrowTargets)")
}

func TestTraverseArrowTargets_RequiresPostOrder(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPreOrder). // PreOrder with TraverseArrowTargets should fail
			WithTraverseArrowTargets(schema),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TraverseArrowTargets requires PostOrder strategy")
}

// TestTraverseArrowTargets_WithoutSchema removed - schema is now required to be explicitly passed

func TestTraverseArrowTargets_MultipleTargets(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation viewer: user
			permission view = viewer
		}

		definition resource {
			relation parent: folder | document
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	// Both folder#view and document#view should be visited before resource's arrow
	// Note: Targets may be visited multiple times (once via arrow traversal, once in normal traversal)
	t.Logf("Visit order: %v", visitor.visitOrder)

	// Find the FIRST occurrence of each target and the arrow
	firstFolderViewIndex := slices.Index(visitor.visitOrder, "folder#view")
	firstDocumentViewIndex := slices.Index(visitor.visitOrder, "document#view")
	arrowRefIndex := slices.Index(visitor.visitOrder, "resource#parent->view")

	require.Contains(t, visitor.visitOrder, "folder#view", "folder#view should be visited")
	require.Contains(t, visitor.visitOrder, "document#view", "document#view should be visited")
	require.Contains(t, visitor.visitOrder, "resource#parent->view", "resource arrow should be visited")
	require.Less(t, firstFolderViewIndex, arrowRefIndex, "folder#view should be visited BEFORE resource arrow")
	require.Less(t, firstDocumentViewIndex, arrowRefIndex, "document#view should be visited BEFORE resource arrow")
}

func TestTraverseArrowTargets_CycleDetection(t *testing.T) {
	schemaText := `
		definition folder {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	// Cycle detection should prevent infinite recursion
	// folder#view may appear multiple times (normal traversal + arrow traversal)
	// but should not cause infinite recursion
	t.Logf("Visit order: %v", visitor.visitOrder)

	viewCount := 0
	for _, visit := range visitor.visitOrder {
		if visit == "folder#view" {
			viewCount++
		}
	}

	// Should be visited a reasonable number of times (not infinite)
	// Typically 2: once in normal traversal, once via arrow target traversal
	require.LessOrEqual(t, viewCount, 3, "folder#view should not be visited infinitely (cycle detection working)")
}

func TestTraverseArrowTargets_NestedArrows(t *testing.T) {
	schemaText := `
		definition org {
			relation member: user
			permission view = member
		}

		definition folder {
			relation parent: org
			permission view = parent->view
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	// org#view should be visited before folder arrow, and folder#view before document arrow
	t.Logf("Visit order: %v", visitor.visitOrder)

	firstOrgViewIndex := slices.Index(visitor.visitOrder, "org#view")
	firstFolderViewIndex := slices.Index(visitor.visitOrder, "folder#view")
	folderArrowIndex := slices.Index(visitor.visitOrder, "folder#parent->view")
	documentArrowIndex := slices.Index(visitor.visitOrder, "document#parent->view")

	require.Contains(t, visitor.visitOrder, "org#view", "org#view should be visited")
	require.Contains(t, visitor.visitOrder, "folder#view", "folder#view should be visited")
	require.Contains(t, visitor.visitOrder, "folder#parent->view", "folder arrow should be visited")
	require.Contains(t, visitor.visitOrder, "document#parent->view", "document arrow should be visited")

	require.Less(t, firstOrgViewIndex, folderArrowIndex, "org#view should be visited before folder arrow")
	require.Less(t, firstFolderViewIndex, documentArrowIndex, "folder#view should be visited before document arrow")
}

func TestTraverseArrowTargets_DisabledByDefault(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema to get ResolvedArrowReference nodes
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().WithStrategy(WalkPostOrder),
		// No WithTraverseArrowTargets - should not traverse arrow targets
	)
	require.NoError(t, err)

	// Without arrow traversal, the order depends on definition order in schema
	// folder#view should NOT necessarily be visited before document arrow
	// (they're in separate definitions)
	// Just verify both are visited
	foundFolderView := false
	foundDocumentArrow := false

	for _, visit := range visitor.visitOrder {
		if visit == "folder#view" {
			foundFolderView = true
		}
		if visit == "document#parent->view" {
			foundDocumentArrow = true
		}
	}

	require.True(t, foundFolderView, "folder#view should be visited")
	require.True(t, foundDocumentArrow, "document arrow should be visited")
}

func TestNewWalkOptions_Defaults(t *testing.T) {
	opts := NewWalkOptions()

	// Verify defaults
	require.Equal(t, WalkPreOrder, opts.strategy)
	require.False(t, opts.traverseArrowTargets)
	require.Nil(t, opts.schema)
	require.NotNil(t, t, opts.visitedArrowTargets)
}

func TestWalkOptions_FluentAPI(t *testing.T) {
	schemaText := `definition user {}`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Test fluent chaining
	opts := NewWalkOptions().
		WithStrategy(WalkPostOrder).
		WithTraverseArrowTargets(schema)

	require.Equal(t, WalkPostOrder, opts.strategy)
	require.True(t, opts.traverseArrowTargets)
	require.Equal(t, schema, opts.schema)
}

func TestTraverseArrowTargets_RequiresPostOrder_WalkPermission(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Get document definition and its view permission
	docDef, ok := resolved.Schema().GetTypeDefinition("document")
	require.True(t, ok)
	viewPerm, ok := docDef.GetPermission("view")
	require.True(t, ok)

	visitor := &arrowTraversalVisitor{}

	// Test that WalkPermissionWithOptions also validates (not just WalkSchemaWithOptions)
	_, err = WalkPermissionWithOptions(viewPerm, visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPreOrder). // PreOrder with TraverseArrowTargets should fail
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TraverseArrowTargets requires PostOrder strategy")
}

func TestTraverseArrowTargets_RequiresSchema(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Pass nil schema - should fail
	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(resolved.Schema(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(nil), // nil schema!
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TraverseArrowTargets requires a schema for target resolution")
	require.Contains(t, err.Error(), "parent->view")
}

func TestTraverseArrowTargets_RequiresResolvedSchema(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// DON'T resolve the schema - this should fail
	visitor := &arrowTraversalVisitor{}
	_, err = WalkSchemaWithOptions(schema, visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(schema), // Unresolved schema!
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "TraverseArrowTargets requires a resolved schema")
	require.Contains(t, err.Error(), "Call ResolveSchema()")
	require.Contains(t, err.Error(), "parent->view") // Should mention the problematic arrow
}

func TestTraverseArrowTargets_IndividualPermissionWalk(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Get document definition and its view permission
	docDef, ok := resolved.Schema().GetTypeDefinition("document")
	require.True(t, ok)
	viewPerm, ok := docDef.GetPermission("view")
	require.True(t, ok)

	visitor := &arrowTraversalVisitor{}

	// Walk ONLY the document#view permission (not the entire schema) with TraverseArrowTargets
	// This tests that visitedArrowTargets map is properly initialized in WalkPermissionWithOptions
	_, err = WalkPermissionWithOptions(viewPerm, visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	t.Logf("Visit order: %v", visitor.visitOrder)

	// folder#view should be visited before document's arrow (via arrow traversal)
	firstFolderViewIndex := -1
	arrowRefIndex := -1

	for i, visit := range visitor.visitOrder {
		if visit == "folder#view" && firstFolderViewIndex == -1 {
			firstFolderViewIndex = i
		}
		if visit == "document#parent->view" {
			arrowRefIndex = i
		}
	}

	require.NotEqual(t, -1, firstFolderViewIndex, "folder#view should be visited via arrow traversal")
	require.NotEqual(t, -1, arrowRefIndex, "document arrow should be visited")
	require.Less(t, firstFolderViewIndex, arrowRefIndex, "folder#view should be visited BEFORE document arrow")
}

func TestTraverseArrowTargets_IndividualRelationWalk(t *testing.T) {
	schemaText := `
		definition user {}

		definition folder {
			relation viewer: user
		}

		definition document {
			relation parent: folder
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Get document definition and its parent relation
	docDef, ok := resolved.Schema().GetTypeDefinition("document")
	require.True(t, ok)
	parentRel, ok := docDef.GetRelation("parent")
	require.True(t, ok)

	visitor := &arrowTraversalVisitor{}

	// Walk ONLY the document#parent relation with TraverseArrowTargets
	// This tests that visitedArrowTargets map is properly initialized in WalkRelationWithOptions
	_, err = WalkRelationWithOptions(parentRel, visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	// Should have visited document#parent
	require.Contains(t, visitor.visitOrder, "document#parent", "document#parent should be visited")
}

func TestTraverseArrowTargets_DirectOperationWalk(t *testing.T) {
	schemaText := `
		definition folder {
			relation viewer: user
			permission view = viewer
		}

		definition document {
			relation parent: folder
			permission view = parent->view
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Get document definition and its view permission's operation
	docDef, ok := resolved.Schema().GetTypeDefinition("document")
	require.True(t, ok)
	viewPerm, ok := docDef.GetPermission("view")
	require.True(t, ok)

	visitor := &arrowTraversalVisitor{}

	// Walk ONLY the operation directly (not the permission wrapper) with TraverseArrowTargets
	// This tests that visitedArrowTargets map is properly initialized in WalkOperationWithOptions
	_, err = WalkOperationWithOptions(viewPerm.Operation(), visitor, struct{}{},
		NewWalkOptions().
			WithStrategy(WalkPostOrder).
			WithTraverseArrowTargets(resolved.Schema()),
	)
	require.NoError(t, err)

	t.Logf("Visit order: %v", visitor.visitOrder)

	// folder#view should be visited via arrow traversal
	foundFolderView := false
	foundArrow := false

	for _, visit := range visitor.visitOrder {
		if visit == "folder#view" {
			foundFolderView = true
		}
		if visit == "document#parent->view" {
			foundArrow = true
		}
	}

	require.True(t, foundFolderView, "folder#view should be visited via arrow traversal")
	require.True(t, foundArrow, "document arrow should be visited")
}

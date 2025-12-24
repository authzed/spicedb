package schema_test

import (
	"fmt"

	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

// Example demonstrating how to traverse up the schema hierarchy using the Parented interface.
func Example_traversingParentHierarchy() {
	// Build a simple schema
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			schema.RelRef("owner"),
			schema.ArrowRef("parent", "view"),
		).
		Done().
		Done().
		Build()

	// Get the view permission
	def, _ := s.GetTypeDefinition("document")
	viewPerm, _ := def.GetPermission("view")
	viewOp := viewPerm.Operation()

	// Traverse up from an operation to the top-level schema
	fmt.Println("Traversing from operation to schema:")
	var current schema.Parented = viewOp
	for current != nil {
		switch node := current.(type) {
		case *schema.UnionOperation:
			fmt.Println("- UnionOperation")
		case *schema.Permission:
			fmt.Printf("- Permission: %s\n", node.Name())
		case *schema.Definition:
			fmt.Printf("- Definition: %s\n", node.Name())
		case *schema.Schema:
			fmt.Println("- Schema (top level)")
		}
		current = current.Parent()
	}

	// Output:
	// Traversing from operation to schema:
	// - UnionOperation
	// - Permission: view
	// - Definition: document
	// - Schema (top level)
}

// Example showing how to find the owning permission of an operation without type assertions.
func Example_findOwningPermission() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddPermission("view").
		Intersection(
			schema.RelRef("viewer"),
			schema.RelRef("owner"),
		).
		Done().
		Done().
		Build()

	def, _ := s.GetTypeDefinition("document")
	viewPerm, _ := def.GetPermission("view")
	intersectionOp := viewPerm.Operation()

	// Get the first child of the intersection
	if intersection, ok := intersectionOp.(*schema.IntersectionOperation); ok {
		firstChild := intersection.Children()[0]

		// Use FindParent to find the owning permission - much simpler!
		perm := schema.FindParent[*schema.Permission](firstChild)
		if perm != nil {
			fmt.Printf("Found owning permission: %s\n", perm.Name())
		}
	}

	// Output:
	// Found owning permission: view
}

// Example demonstrating FindParent with different types in the hierarchy.
func Example_findParentWithGenerics() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			schema.RelRef("owner"),
			schema.ArrowRef("parent", "view"),
		).
		Done().
		Done().
		Build()

	// Get an operation deep in the tree
	def, _ := s.GetTypeDefinition("document")
	viewPerm, _ := def.GetPermission("view")
	unionOp := viewPerm.Operation()
	firstChild := unionOp.(*schema.UnionOperation).Children()[0]

	// Find different parent types using generics
	perm := schema.FindParent[*schema.Permission](firstChild)
	fmt.Printf("Permission: %s\n", perm.Name())

	definition := schema.FindParent[*schema.Definition](firstChild)
	fmt.Printf("Definition: %s\n", definition.Name())

	topSchema := schema.FindParent[*schema.Schema](firstChild)
	if topSchema != nil {
		fmt.Println("Found schema (top level)")
	}

	// Output:
	// Permission: view
	// Definition: document
	// Found schema (top level)
}

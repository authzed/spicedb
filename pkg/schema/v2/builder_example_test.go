package schema_test

import (
	"fmt"
	"strings"

	schema "github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

func toSchemaDefinitions[T compiler.SchemaDefinition](items []T) []compiler.SchemaDefinition {
	result := make([]compiler.SchemaDefinition, len(items))
	for i, item := range items {
		result[i] = item
	}
	return result
}

// ExampleSchemaBuilder demonstrates basic schema creation using the builder pattern.
func ExampleSchemaBuilder() {
	s := schema.NewSchemaBuilder().
		AddDefinition("user").
		Done().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			schema.RelRef("owner"),
			schema.RelRef("viewer"),
		).
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_incrementalUnion demonstrates building a union permission incrementally.
func ExampleSchemaBuilder_incrementalUnion() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("editor").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("editor").
		AddRelationRef("viewer").
		Done().
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withArrow demonstrates using arrow references for hierarchical permissions.
func ExampleSchemaBuilder_withArrow() {
	s := schema.NewSchemaBuilder().
		AddDefinition("folder").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		RelationRef("owner").
		Done().
		Done().
		AddDefinition("document").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddArrow("parent", "view").
		Done().
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withCaveat demonstrates using caveats with relations.
func ExampleSchemaBuilder_withCaveat() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("conditional_viewer").
		AllowedDirectRelationWithCaveat("user", "valid_ip").
		Done().
		Done().
		AddCaveat("valid_ip").
		Expression("request.ip_address in allowed_ranges").
		Parameter("allowed_ranges", "list<string>").
		Parameter("request", "map<any>").
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withCaveatTypes demonstrates using type constants and ParameterMap for caveats.
func ExampleSchemaBuilder_withCaveatTypes() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelationWithCaveat("user", "ip_and_time_check").
		Done().
		Done().
		AddCaveat("ip_and_time_check").
		Expression("request.ip in allowed_ips && current_time < expiration").
		Parameter("allowed_ips", schema.CaveatTypeListString).
		Parameter("expiration", schema.CaveatTypeTimestamp).
		Parameter("current_time", schema.CaveatTypeTimestamp).
		Done().
		AddCaveat("role_check").
		Expression("user_roles.contains(required_role) && is_active").
		ParameterMap(map[string]string{
			"user_roles":    schema.CaveatTypeListString,
			"required_role": schema.CaveatTypeString,
			"is_active":     schema.CaveatTypeBool,
		}).
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withExclusion demonstrates using exclusion (subtraction) in permissions.
func ExampleSchemaBuilder_withExclusion() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddRelation("banned").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		ExclusionExpr().
		BaseRelationRef("viewer").
		ExcludeRelationRef("banned").
		Done().
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withIntersection demonstrates using intersection (AND) in permissions.
func ExampleSchemaBuilder_withIntersection() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("approved").
		AllowedDirectRelation("user").
		Done().
		AddPermission("admin_edit").
		IntersectionExpr().
		AddRelationRef("owner").
		AddRelationRef("approved").
		Done().
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withWildcard demonstrates using wildcard relations.
func ExampleSchemaBuilder_withWildcard() {
	s := schema.NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("public_viewer").
		AllowedWildcard("user").
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_complex demonstrates a complex schema with multiple features.
func ExampleSchemaBuilder_complex() {
	s := schema.NewSchemaBuilder().
		AddDefinition("user").
		Done().
		AddDefinition("group").
		AddRelation("member").
		AllowedDirectRelation("user").
		AllowedRelation("group", "member").
		Done().
		Done().
		AddDefinition("folder").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		AllowedRelation("group", "member").
		Done().
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		Done().
		Done().
		AddPermission("edit").
		RelationRef("owner").
		Done().
		Done().
		AddDefinition("document").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		AllowedRelation("group", "member").
		Done().
		AddRelation("banned").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		ExclusionExpr().
		Base(
			schema.Union(
				schema.RelRef("owner"),
				schema.RelRef("viewer"),
				schema.ArrowRef("parent", "view"),
			),
		).
		ExcludeRelationRef("banned").
		Done().
		Done().
		AddPermission("edit").
		IntersectionExpr().
		AddRelationRef("owner").
		AddArrow("parent", "edit").
		Done().
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorPattern demonstrates the new constructor-based builder pattern.
func ExampleSchemaBuilder_constructorPattern() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("user"),
		).
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Relation(schema.NewRelation("editor").AllowedDirectRelation("user")).
				Relation(schema.NewRelation("viewer").AllowedDirectRelation("user")).
				Permission(
					schema.NewPermission("view", schema.Union(
						schema.RelRef("owner"),
						schema.RelRef("editor"),
						schema.RelRef("viewer"),
					)),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorWithIntersection demonstrates using intersection with the constructor pattern.
func ExampleSchemaBuilder_constructorWithIntersection() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Relation(schema.NewRelation("approved").AllowedDirectRelation("user")).
				Permission(
					schema.NewPermission("admin_edit", schema.Intersection(
						schema.RelRef("owner"),
						schema.RelRef("approved"),
					)),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorWithExclusion demonstrates using exclusion (subtraction) with the constructor pattern.
func ExampleSchemaBuilder_constructorWithExclusion() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("viewer").AllowedDirectRelation("user")).
				Relation(schema.NewRelation("banned").AllowedDirectRelation("user")).
				Permission(
					schema.NewPermission("view", schema.Exclusion(
						schema.RelRef("viewer"),
						schema.RelRef("banned"),
					)),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorWithArrow demonstrates using arrow references for hierarchical permissions.
func ExampleSchemaBuilder_constructorWithArrow() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("folder").
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Permission(schema.NewPermission("view", schema.RelRef("owner"))),
		).
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("parent").AllowedDirectRelation("folder")).
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Permission(
					schema.NewPermission("view", schema.Union(
						schema.RelRef("owner"),
						schema.ArrowRef("parent", "view"),
					)),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorWithIntersectionArrow demonstrates using intersection arrow (all) operations.
func ExampleSchemaBuilder_constructorWithIntersectionArrow() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("folder").
				Relation(schema.NewRelation("viewer").AllowedDirectRelation("user")),
		).
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("parent").AllowedDirectRelation("folder")).
				Permission(
					schema.NewPermission("all_parents_can_view", schema.IntersectionArrowRef("parent", "viewer")),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorWithCaveat demonstrates using caveats with the constructor pattern.
func ExampleSchemaBuilder_constructorWithCaveat() {
	s := schema.NewSchemaBuilder().
		Definition(
			schema.NewDefinition("document").
				Relation(
					schema.NewRelation("conditional_viewer").
						AllowedDirectRelationWithCaveat("user", "valid_ip"),
				),
		).
		Caveat(
			schema.NewCaveat("valid_ip").
				Expression("request.ip_address in allowed_ranges").
				Parameter("allowed_ranges", schema.CaveatTypeListString).
				Parameter("request", schema.CaveatTypeMapAny),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_constructorComplexNested demonstrates complex nested operations with the constructor pattern.
func ExampleSchemaBuilder_constructorComplexNested() {
	s := schema.NewSchemaBuilder().
		Definition(schema.NewDefinition("user")).
		Definition(
			schema.NewDefinition("group").
				Relation(schema.NewRelation("member").AllowedDirectRelation("user")),
		).
		Definition(
			schema.NewDefinition("folder").
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Relation(
					schema.NewRelation("viewer").
						AllowedDirectRelation("user").
						AllowedRelation("group", "member"),
				).
				Permission(
					schema.NewPermission("view", schema.Union(
						schema.RelRef("owner"),
						schema.RelRef("viewer"),
					)),
				),
		).
		Definition(
			schema.NewDefinition("document").
				Relation(schema.NewRelation("parent").AllowedDirectRelation("folder")).
				Relation(schema.NewRelation("owner").AllowedDirectRelation("user")).
				Relation(schema.NewRelation("banned").AllowedDirectRelation("user")).
				Permission(
					schema.NewPermission("view", schema.Exclusion(
						schema.Union(
							schema.RelRef("owner"),
							schema.ArrowRef("parent", "view"),
						),
						schema.RelRef("banned"),
					)),
				).
				Permission(
					schema.NewPermission("edit", schema.Intersection(
						schema.Union(
							schema.RelRef("owner"),
							schema.ArrowRef("parent", "view"),
						),
						schema.Exclusion(
							schema.RelRef("owner"),
							schema.RelRef("banned"),
						),
					)),
				),
		).
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

// ExampleSchemaBuilder_withIntersectionArrow demonstrates using intersection arrow (all) operation.
func ExampleSchemaBuilder_withIntersectionArrow() {
	s := schema.NewSchemaBuilder().
		AddDefinition("folder").
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		Done().
		AddDefinition("document").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddPermission("all_parents_can_view").
		IntersectionArrow("parent", "viewer").
		Done().
		Done().
		Build()

	defs, caveats, _ := s.ToDefinitions()
	schemaText, _, _ := generator.GenerateSchema(append(toSchemaDefinitions(defs), toSchemaDefinitions(caveats)...))
	fmt.Println(strings.TrimSpace(schemaText))
}

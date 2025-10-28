package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSchemaBuilderBasic(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("user").
		Done().
		Build()

	require.NotNil(t, schema)
	require.Len(t, schema.definitions, 1)
	require.Contains(t, schema.definitions, "user")

	userDef := schema.definitions["user"]
	require.Equal(t, "user", userDef.name)
	require.Equal(t, schema, userDef.parent)
	require.Empty(t, userDef.relations)
	require.Empty(t, userDef.permissions)
}

func TestSchemaBuilderWithRelations(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		AllowedRelation("group", "member").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	// Check owner relation
	ownerRel := docDef.relations["owner"]
	require.NotNil(t, ownerRel)
	require.Equal(t, "owner", ownerRel.name)
	require.Equal(t, docDef, ownerRel.parent)
	require.Len(t, ownerRel.baseRelations, 1)
	require.Equal(t, "user", ownerRel.baseRelations[0].subjectType)
	require.Equal(t, tuple.Ellipsis, ownerRel.baseRelations[0].subrelation)

	// Check viewer relation
	viewerRel := docDef.relations["viewer"]
	require.NotNil(t, viewerRel)
	require.Len(t, viewerRel.baseRelations, 2)
	require.Equal(t, "user", viewerRel.baseRelations[0].subjectType)
	require.Equal(t, tuple.Ellipsis, viewerRel.baseRelations[0].subrelation)
	require.Equal(t, "group", viewerRel.baseRelations[1].subjectType)
	require.Equal(t, "member", viewerRel.baseRelations[1].subrelation)
}

func TestSchemaBuilderWithCaveats(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelationWithCaveat("user", "valid_ip").
		Done().
		Done().
		AddCaveat("valid_ip").
		Expression("request.ip_address in allowed_ips").
		Parameter("allowed_ips", "list<string>").
		Done().
		Build()

	require.NotNil(t, schema)

	// Check caveat
	caveat := schema.caveats["valid_ip"]
	require.NotNil(t, caveat)
	require.Equal(t, "valid_ip", caveat.name)
	require.Equal(t, "request.ip_address in allowed_ips", caveat.expression)
	require.Len(t, caveat.parameters, 1)
	require.Equal(t, "allowed_ips", caveat.parameters[0].name)
	require.Equal(t, "list<string>", caveat.parameters[0].typ)
	require.Equal(t, schema, caveat.parent)

	// Check relation with caveat
	docDef := schema.definitions["document"]
	viewerRel := docDef.relations["viewer"]
	require.NotNil(t, viewerRel)
	require.Len(t, viewerRel.baseRelations, 1)
	require.Equal(t, "user", viewerRel.baseRelations[0].subjectType)
	require.Equal(t, "valid_ip", viewerRel.baseRelations[0].caveat)
}

func TestSchemaBuilderWithExpiration(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("temporary_viewer").
		AllowedDirectRelationWithExpiration("user").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	tempViewerRel := docDef.relations["temporary_viewer"]
	require.NotNil(t, tempViewerRel)
	require.Len(t, tempViewerRel.baseRelations, 1)
	require.True(t, tempViewerRel.baseRelations[0].expiration)
}

func TestSchemaBuilderWithWildcard(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("public_viewer").
		AllowedWildcard("user").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	publicViewerRel := docDef.relations["public_viewer"]
	require.NotNil(t, publicViewerRel)
	require.Len(t, publicViewerRel.baseRelations, 1)
	require.True(t, publicViewerRel.baseRelations[0].wildcard)
	require.Equal(t, "user", publicViewerRel.baseRelations[0].subjectType)
}

func TestSchemaBuilderWithAlias(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("reader").
		Alias("viewer").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	readerRel := docDef.relations["reader"]
	require.NotNil(t, readerRel)
	require.Equal(t, "viewer", readerRel.aliasingRelation)
}

func TestSchemaBuilderWithPermissionRelationRef(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		RelationRef("viewer").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	require.Equal(t, "view", viewPerm.name)
	require.Equal(t, docDef, viewPerm.parent)

	relRef, ok := viewPerm.operation.(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRef.RelationName())
}

func TestSchemaBuilderWithPermissionArrow(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddPermission("view").
		Arrow("parent", "view").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	arrowRef, ok := viewPerm.operation.(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowRef.Left())
	require.Equal(t, "view", arrowRef.Right())
}

func TestSchemaBuilderWithPermissionUnion(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Union(
			RelRef("owner"),
			RelRef("viewer"),
		).
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	unionOp, ok := viewPerm.operation.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOp.Children(), 2)

	relRef1, ok := unionOp.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", relRef1.RelationName())

	relRef2, ok := unionOp.Children()[1].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRef2.RelationName())
}

func TestSchemaBuilderWithPermissionUnionExpr(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
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
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		AddRelationRef("editor").
		Done().
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	unionOp, ok := viewPerm.operation.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOp.Children(), 3)
}

func TestSchemaBuilderWithPermissionIntersection(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		AddRelation("approved").
		AllowedDirectRelation("user").
		Done().
		AddPermission("admin_edit").
		Intersection(
			RelRef("owner"),
			RelRef("approved"),
		).
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	adminEditPerm := docDef.permissions["admin_edit"]
	require.NotNil(t, adminEditPerm)

	intersectionOp, ok := adminEditPerm.operation.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)
}

func TestSchemaBuilderWithPermissionIntersectionExpr(t *testing.T) {
	schema := NewSchemaBuilder().
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

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	adminEditPerm := docDef.permissions["admin_edit"]
	require.NotNil(t, adminEditPerm)

	intersectionOp, ok := adminEditPerm.operation.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)
}

func TestSchemaBuilderWithPermissionExclusion(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		AddRelation("banned").
		AllowedDirectRelation("user").
		Done().
		AddPermission("view").
		Exclusion(
			RelRef("viewer"),
			RelRef("banned"),
		).
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	exclusionOp, ok := viewPerm.operation.(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOp.Left())
	require.NotNil(t, exclusionOp.Right())

	baseRef, ok := exclusionOp.Left().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", baseRef.RelationName())

	excludedRef, ok := exclusionOp.Right().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "banned", excludedRef.RelationName())
}

func TestSchemaBuilderWithPermissionExclusionExpr(t *testing.T) {
	schema := NewSchemaBuilder().
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

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	exclusionOp, ok := viewPerm.operation.(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOp.Left())
	require.NotNil(t, exclusionOp.Right())
}

func TestSchemaBuilderWithIntersectionArrow(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("parent").
		AllowedDirectRelation("folder").
		Done().
		AddPermission("view").
		IntersectionArrow("parent", "viewer").
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	funcOp, ok := viewPerm.operation.(*FunctionedArrowReference)
	require.True(t, ok)
	require.Equal(t, FunctionTypeAll, funcOp.Function())
	require.Equal(t, "parent", funcOp.Left())
	require.Equal(t, "viewer", funcOp.Right())
}

func TestSchemaBuilderComplexExample(t *testing.T) {
	schema := NewSchemaBuilder().
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
		AddPermission("view").
		UnionExpr().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		AddArrow("parent", "view").
		Done().
		Done().
		AddPermission("edit").
		RelationRef("owner").
		Done().
		Done().
		AddCaveat("valid_ip").
		Expression("request.ip_address in allowed_ranges").
		Parameter("allowed_ranges", "list<string>").
		Parameter("request", "map<any>").
		Done().
		Build()

	require.NotNil(t, schema)
	require.Len(t, schema.definitions, 4)
	require.Len(t, schema.caveats, 1)

	// Verify document definition
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)
	require.Len(t, docDef.relations, 3)
	require.Len(t, docDef.permissions, 2)

	// Verify view permission
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	unionOp, ok := viewPerm.operation.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOp.Children(), 3)

	// Verify arrow is part of union
	arrowRef, ok := unionOp.Children()[2].(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowRef.Left())
	require.Equal(t, "view", arrowRef.Right())
}

func TestSchemaBuilderModifyExisting(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add initial definition
	builder.AddDefinition("document").
		AddRelation("owner").
		AllowedDirectRelation("user").
		Done().
		Done()

	// Modify the same definition
	builder.AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelation("user").
		Done().
		Done()

	schema := builder.Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)
	require.Len(t, docDef.relations, 2)
	require.Contains(t, docDef.relations, "owner")
	require.Contains(t, docDef.relations, "viewer")
}

func TestSchemaBuilderWithAllFeatures(t *testing.T) {
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").
		AllowedDirectRelationWithFeatures("user", "valid_ip", true).
		Done().
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	viewerRel := docDef.relations["viewer"]
	require.NotNil(t, viewerRel)
	require.Len(t, viewerRel.baseRelations, 1)

	baseRel := viewerRel.baseRelations[0]
	require.Equal(t, "user", baseRel.subjectType)
	require.Equal(t, tuple.Ellipsis, baseRel.subrelation)
	require.Equal(t, "valid_ip", baseRel.caveat)
	require.True(t, baseRel.expiration)
}

func TestSchemaBuilderWithCaveatParameterMap(t *testing.T) {
	schema := NewSchemaBuilder().
		AddCaveat("complex_caveat").
		Expression("param1 == param2 && param3").
		ParameterMap(map[string]string{
			"param1": CaveatTypeString,
			"param2": CaveatTypeString,
			"param3": CaveatTypeBool,
		}).
		Done().
		Build()

	require.NotNil(t, schema)
	caveat := schema.caveats["complex_caveat"]
	require.NotNil(t, caveat)
	require.Len(t, caveat.parameters, 3)

	// Check that all parameters are present
	paramMap := make(map[string]string)
	for _, param := range caveat.parameters {
		paramMap[param.name] = param.typ
	}
	require.Equal(t, CaveatTypeString, paramMap["param1"])
	require.Equal(t, CaveatTypeString, paramMap["param2"])
	require.Equal(t, CaveatTypeBool, paramMap["param3"])
}

func TestSchemaBuilderWithMixedCaveatParameters(t *testing.T) {
	schema := NewSchemaBuilder().
		AddCaveat("mixed_caveat").
		Expression("count > threshold && allowed").
		Parameter("count", CaveatTypeInt).
		ParameterMap(map[string]string{
			"threshold": CaveatTypeInt,
			"allowed":   CaveatTypeBool,
		}).
		Done().
		Build()

	require.NotNil(t, schema)
	caveat := schema.caveats["mixed_caveat"]
	require.NotNil(t, caveat)
	require.Len(t, caveat.parameters, 3)

	// Check that all parameters are present in the right order
	require.Equal(t, "count", caveat.parameters[0].name)
	require.Equal(t, CaveatTypeInt, caveat.parameters[0].typ)

	// The map parameters might be in any order
	paramMap := make(map[string]string)
	for _, param := range caveat.parameters {
		paramMap[param.name] = param.typ
	}
	require.Equal(t, CaveatTypeInt, paramMap["count"])
	require.Equal(t, CaveatTypeInt, paramMap["threshold"])
	require.Equal(t, CaveatTypeBool, paramMap["allowed"])
}

func TestSchemaBuilderWithConstructorPattern(t *testing.T) {
	// Test the new constructor pattern with NewDefinition(), NewRelation(), NewPermission()
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("document").
				Relation(
					NewRelation("owner").AllowedDirectRelation("user"),
				).
				Relation(
					NewRelation("editor").AllowedDirectRelation("user"),
				).
				Relation(
					NewRelation("viewer").AllowedDirectRelation("user"),
				).
				Permission(
					NewPermission("view", Union(
						RelRef("owner"),
						RelRef("editor"),
						RelRef("viewer"),
					)),
				),
		).
		Definition(
			NewDefinition("user").
				Relation(
					NewRelation("friend").AllowedDirectRelation("user"),
				),
		).
		Build()

	require.NotNil(t, schema)
	require.Len(t, schema.definitions, 2)

	// Verify document definition
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)
	require.Equal(t, "document", docDef.name)
	require.Len(t, docDef.relations, 3)
	require.Len(t, docDef.permissions, 1)

	// Verify relations
	require.Contains(t, docDef.relations, "owner")
	require.Contains(t, docDef.relations, "editor")
	require.Contains(t, docDef.relations, "viewer")

	ownerRel := docDef.relations["owner"]
	require.Len(t, ownerRel.baseRelations, 1)
	require.Equal(t, "user", ownerRel.baseRelations[0].subjectType)

	// Verify permission
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	require.Equal(t, "view", viewPerm.name)

	unionOp, ok := viewPerm.operation.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOp.Children(), 3)

	// Verify user definition
	userDef := schema.definitions["user"]
	require.NotNil(t, userDef)
	require.Equal(t, "user", userDef.name)
	require.Len(t, userDef.relations, 1)

	friendRel := userDef.relations["friend"]
	require.NotNil(t, friendRel)
	require.Equal(t, "friend", friendRel.name)
	require.Len(t, friendRel.baseRelations, 1)
	require.Equal(t, "user", friendRel.baseRelations[0].subjectType)
}

func TestSchemaBuilderConstructorWithIntersection(t *testing.T) {
	// Test constructor pattern with intersection operations
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("document").
				Relation(NewRelation("owner").AllowedDirectRelation("user")).
				Relation(NewRelation("approved").AllowedDirectRelation("user")).
				Permission(
					NewPermission("admin_edit", Intersection(
						RelRef("owner"),
						RelRef("approved"),
					)),
				),
		).
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	// Verify permission with intersection
	adminEditPerm := docDef.permissions["admin_edit"]
	require.NotNil(t, adminEditPerm)
	intersectionOp, ok := adminEditPerm.operation.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)
}

func TestSchemaBuilderConstructorWithExclusion(t *testing.T) {
	// Test constructor pattern with exclusion operations
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("document").
				Relation(NewRelation("viewer").AllowedDirectRelation("user")).
				Relation(NewRelation("banned").AllowedDirectRelation("user")).
				Permission(
					NewPermission("view", Exclusion(
						RelRef("viewer"),
						RelRef("banned"),
					)),
				),
		).
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	// Verify permission with exclusion
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	exclusionOp, ok := viewPerm.operation.(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOp.Left())
	require.NotNil(t, exclusionOp.Right())

	baseRef, ok := exclusionOp.Left().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", baseRef.RelationName())

	excludedRef, ok := exclusionOp.Right().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "banned", excludedRef.RelationName())
}

func TestSchemaBuilderConstructorWithArrow(t *testing.T) {
	// Test constructor pattern with arrow references
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("folder").
				Relation(NewRelation("owner").AllowedDirectRelation("user")).
				Permission(NewPermission("view", RelRef("owner"))),
		).
		Definition(
			NewDefinition("document").
				Relation(NewRelation("parent").AllowedDirectRelation("folder")).
				Relation(NewRelation("owner").AllowedDirectRelation("user")).
				Permission(
					NewPermission("view", Union(
						RelRef("owner"),
						ArrowRef("parent", "view"),
					)),
				),
		).
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	// Verify permission with arrow
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	unionOp, ok := viewPerm.operation.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOp.Children(), 2)

	arrowRef, ok := unionOp.Children()[1].(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowRef.Left())
	require.Equal(t, "view", arrowRef.Right())
}

func TestSchemaBuilderConstructorWithIntersectionArrow(t *testing.T) {
	// Test constructor pattern with intersection arrow (all) operations
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("folder").
				Relation(NewRelation("viewer").AllowedDirectRelation("user")),
		).
		Definition(
			NewDefinition("document").
				Relation(NewRelation("parent").AllowedDirectRelation("folder")).
				Permission(
					NewPermission("all_parents_can_view", IntersectionArrowRef("parent", "viewer")),
				),
		).
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	// Verify permission with intersection arrow
	viewPerm := docDef.permissions["all_parents_can_view"]
	require.NotNil(t, viewPerm)
	funcOp, ok := viewPerm.operation.(*FunctionedArrowReference)
	require.True(t, ok)
	require.Equal(t, FunctionTypeAll, funcOp.Function())
	require.Equal(t, "parent", funcOp.Left())
	require.Equal(t, "viewer", funcOp.Right())
}

func TestSchemaBuilderConstructorWithCaveat(t *testing.T) {
	// Test constructor pattern with caveats
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("document").
				Relation(
					NewRelation("conditional_viewer").
						AllowedDirectRelationWithCaveat("user", "valid_ip"),
				),
		).
		Caveat(
			NewCaveat("valid_ip").
				Expression("request.ip_address in allowed_ranges").
				Parameter("allowed_ranges", CaveatTypeListString).
				Parameter("request", CaveatTypeMapAny),
		).
		Build()

	require.NotNil(t, schema)

	// Verify caveat
	caveat := schema.caveats["valid_ip"]
	require.NotNil(t, caveat)
	require.Equal(t, "valid_ip", caveat.name)
	require.Equal(t, "request.ip_address in allowed_ranges", caveat.expression)
	require.Len(t, caveat.parameters, 2)

	// Verify relation with caveat
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)
	conditionalViewerRel := docDef.relations["conditional_viewer"]
	require.NotNil(t, conditionalViewerRel)
	require.Len(t, conditionalViewerRel.baseRelations, 1)
	require.Equal(t, "user", conditionalViewerRel.baseRelations[0].subjectType)
	require.Equal(t, "valid_ip", conditionalViewerRel.baseRelations[0].caveat)
}

func TestSchemaBuilderConstructorComplexNested(t *testing.T) {
	// Test constructor pattern with complex nested operations
	schema := NewSchemaBuilder().
		Definition(
			NewDefinition("user"),
		).
		Definition(
			NewDefinition("group").
				Relation(NewRelation("member").AllowedDirectRelation("user")),
		).
		Definition(
			NewDefinition("folder").
				Relation(NewRelation("owner").AllowedDirectRelation("user")).
				Relation(
					NewRelation("editor").
						AllowedDirectRelation("user").
						AllowedRelation("group", "member"),
				).
				Relation(
					NewRelation("viewer").
						AllowedDirectRelation("user").
						AllowedRelation("group", "member"),
				).
				Permission(
					NewPermission("view", Union(
						RelRef("owner"),
						RelRef("editor"),
						RelRef("viewer"),
					)),
				).
				Permission(
					NewPermission("edit", Union(
						RelRef("owner"),
						RelRef("editor"),
					)),
				),
		).
		Definition(
			NewDefinition("document").
				Relation(NewRelation("parent").AllowedDirectRelation("folder")).
				Relation(NewRelation("owner").AllowedDirectRelation("user")).
				Relation(NewRelation("banned").AllowedDirectRelation("user")).
				Permission(
					NewPermission("view", Exclusion(
						Union(
							RelRef("owner"),
							ArrowRef("parent", "view"),
						),
						RelRef("banned"),
					)),
				).
				Permission(
					NewPermission("edit", Intersection(
						Union(
							RelRef("owner"),
							ArrowRef("parent", "edit"),
						),
						Exclusion(
							RelRef("owner"),
							RelRef("banned"),
						),
					)),
				),
		).
		Build()

	require.NotNil(t, schema)
	require.Len(t, schema.definitions, 4)

	// Verify document definition
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)
	require.Len(t, docDef.relations, 3)
	require.Len(t, docDef.permissions, 2)

	// Verify complex view permission (exclusion of union)
	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)
	exclusionOp, ok := viewPerm.operation.(*ExclusionOperation)
	require.True(t, ok)

	unionBase, ok := exclusionOp.Left().(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionBase.Children(), 2)

	// Verify complex edit permission (intersection of union and exclusion)
	editPerm := docDef.permissions["edit"]
	require.NotNil(t, editPerm)
	intersectionOp, ok := editPerm.operation.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)
}

func TestSchemaBuilderHelperFunctions(t *testing.T) {
	// Test RelRef
	relRef := RelRef("viewer")
	require.NotNil(t, relRef)
	relRefTyped, ok := relRef.(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRefTyped.RelationName())

	// Test ArrowRef
	arrowRef := ArrowRef("parent", "view")
	require.NotNil(t, arrowRef)
	arrowRefTyped, ok := arrowRef.(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowRefTyped.Left())
	require.Equal(t, "view", arrowRefTyped.Right())

	// Test Union
	unionOp := Union(RelRef("owner"), RelRef("viewer"))
	require.NotNil(t, unionOp)
	unionOpTyped, ok := unionOp.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOpTyped.Children(), 2)

	// Test Intersection
	intersectionOp := Intersection(RelRef("owner"), RelRef("approved"))
	require.NotNil(t, intersectionOp)
	intersectionOpTyped, ok := intersectionOp.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOpTyped.Children(), 2)

	// Test Exclusion
	exclusionOp := Exclusion(RelRef("viewer"), RelRef("banned"))
	require.NotNil(t, exclusionOp)
	exclusionOpTyped, ok := exclusionOp.(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOpTyped.Left())
	require.NotNil(t, exclusionOpTyped.Right())

	// Test IntersectionArrowRef
	intersectionArrowOp := IntersectionArrowRef("parent", "viewer")
	require.NotNil(t, intersectionArrowOp)
	funcOpTyped, ok := intersectionArrowOp.(*FunctionedArrowReference)
	require.True(t, ok)
	require.Equal(t, FunctionTypeAll, funcOpTyped.Function())
	require.Equal(t, "parent", funcOpTyped.Left())
	require.Equal(t, "viewer", funcOpTyped.Right())
}

func TestNewRelationRef(t *testing.T) {
	// Test creating a relation reference using NewRelationRef
	relRef := NewRelationRef("viewer")
	require.NotNil(t, relRef)

	relRefTyped, ok := relRef.(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRefTyped.RelationName())
}

func TestNewRelationRefWithPermission(t *testing.T) {
	// Test using NewRelationRef with NewPermission
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("viewer").AllowedDirectRelation("user").Done().
		Permission(NewPermission("view", NewRelationRef("viewer"))).
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	relRef, ok := viewPerm.operation.(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRef.RelationName())
}

func TestNewArrow(t *testing.T) {
	// Test creating an arrow reference using NewArrow
	arrow := NewArrow("parent", "view")
	require.NotNil(t, arrow)

	arrowTyped, ok := arrow.(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowTyped.Left())
	require.Equal(t, "view", arrowTyped.Right())
}

func TestNewArrowWithPermission(t *testing.T) {
	// Test using NewArrow with NewPermission
	schema := NewSchemaBuilder().
		AddDefinition("folder").
		AddRelation("viewer").AllowedDirectRelation("user").Done().
		Permission(NewPermission("view", NewRelationRef("viewer"))).
		Done().
		AddDefinition("document").
		AddRelation("parent").AllowedDirectRelation("folder").Done().
		Permission(NewPermission("view", NewArrow("parent", "view"))).
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	viewPerm := docDef.permissions["view"]
	require.NotNil(t, viewPerm)

	arrow, ok := viewPerm.operation.(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrow.Left())
	require.Equal(t, "view", arrow.Right())
}

func TestNewArrowWithBuilders(t *testing.T) {
	// Test using NewArrow with operation builders
	unionOp := NewUnion().
		Add(NewRelationRef("owner")).
		Add(NewArrow("parent", "view")).
		Build()

	require.NotNil(t, unionOp)
	unionOpTyped, ok := unionOp.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOpTyped.Children(), 2)

	// Check first child (relation reference)
	relRef, ok := unionOpTyped.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", relRef.RelationName())

	// Check second child (arrow reference)
	arrow, ok := unionOpTyped.Children()[1].(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrow.Left())
	require.Equal(t, "view", arrow.Right())
}

func TestNewUnionBuilder(t *testing.T) {
	// Test building a union operation standalone
	unionOp := NewUnion().
		AddRelationRef("owner").
		AddRelationRef("viewer").
		AddArrow("parent", "view").
		Build()

	require.NotNil(t, unionOp)
	unionOpTyped, ok := unionOp.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOpTyped.Children(), 3)

	// Check first child (relation reference)
	relRef1, ok := unionOpTyped.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", relRef1.RelationName())

	// Check second child (relation reference)
	relRef2, ok := unionOpTyped.Children()[1].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", relRef2.RelationName())

	// Check third child (arrow reference)
	arrowRef, ok := unionOpTyped.Children()[2].(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", arrowRef.Left())
	require.Equal(t, "view", arrowRef.Right())
}

func TestNewIntersectionBuilder(t *testing.T) {
	// Test building an intersection operation standalone
	intersectionOp := NewIntersection().
		AddRelationRef("owner").
		AddRelationRef("approved").
		AddArrow("org", "member").
		Build()

	require.NotNil(t, intersectionOp)
	intersectionOpTyped, ok := intersectionOp.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOpTyped.Children(), 3)

	// Check first child (relation reference)
	relRef1, ok := intersectionOpTyped.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", relRef1.RelationName())

	// Check second child (relation reference)
	relRef2, ok := intersectionOpTyped.Children()[1].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "approved", relRef2.RelationName())

	// Check third child (arrow reference)
	arrowRef, ok := intersectionOpTyped.Children()[2].(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "org", arrowRef.Left())
	require.Equal(t, "member", arrowRef.Right())
}

func TestNewExclusionBuilder(t *testing.T) {
	// Test building an exclusion operation standalone
	exclusionOp := NewExclusion().
		BaseRelationRef("viewer").
		ExcludeRelationRef("banned").
		Build()

	require.NotNil(t, exclusionOp)
	exclusionOpTyped, ok := exclusionOp.(*ExclusionOperation)
	require.True(t, ok)

	// Check base (left side)
	baseRef, ok := exclusionOpTyped.Left().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "viewer", baseRef.RelationName())

	// Check excluded (right side)
	excludedRef, ok := exclusionOpTyped.Right().(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "banned", excludedRef.RelationName())
}

func TestNewExclusionBuilderWithArrows(t *testing.T) {
	// Test building an exclusion operation with arrows
	exclusionOp := NewExclusion().
		BaseArrow("parent", "view").
		ExcludeArrow("parent", "blocked").
		Build()

	require.NotNil(t, exclusionOp)
	exclusionOpTyped, ok := exclusionOp.(*ExclusionOperation)
	require.True(t, ok)

	// Check base (left side)
	baseArrow, ok := exclusionOpTyped.Left().(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", baseArrow.Left())
	require.Equal(t, "view", baseArrow.Right())

	// Check excluded (right side)
	excludedArrow, ok := exclusionOpTyped.Right().(*ArrowReference)
	require.True(t, ok)
	require.Equal(t, "parent", excludedArrow.Left())
	require.Equal(t, "blocked", excludedArrow.Right())
}

func TestNewBuilderWithComplexNesting(t *testing.T) {
	// Test building complex nested operations using the new builders
	complexOp := NewUnion().
		Add(RelRef("owner")).
		Add(NewIntersection().
			AddRelationRef("editor").
			AddRelationRef("approved").
			Build()).
		Add(NewExclusion().
			BaseArrow("parent", "view").
			ExcludeRelationRef("blocked").
			Build()).
		Build()

	require.NotNil(t, complexOp)
	unionOpTyped, ok := complexOp.(*UnionOperation)
	require.True(t, ok)
	require.Len(t, unionOpTyped.Children(), 3)

	// Check first child (simple relation reference)
	relRef, ok := unionOpTyped.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", relRef.RelationName())

	// Check second child (intersection)
	intersectionOp, ok := unionOpTyped.Children()[1].(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)

	// Check third child (exclusion)
	exclusionOp, ok := unionOpTyped.Children()[2].(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOp.Left())
	require.NotNil(t, exclusionOp.Right())
}

func TestNewBuilderWithPermission(t *testing.T) {
	// Test using the new builders with NewPermission
	schema := NewSchemaBuilder().
		AddDefinition("document").
		AddRelation("owner").AllowedDirectRelation("user").Done().
		AddRelation("editor").AllowedDirectRelation("user").Done().
		AddRelation("banned").AllowedDirectRelation("user").Done().
		Permission(
			NewPermission("edit", NewIntersection().
				AddRelationRef("owner").
				Add(NewExclusion().
					BaseRelationRef("editor").
					ExcludeRelationRef("banned").
					Build()).
				Build()),
		).
		Done().
		Build()

	require.NotNil(t, schema)
	docDef := schema.definitions["document"]
	require.NotNil(t, docDef)

	editPerm := docDef.permissions["edit"]
	require.NotNil(t, editPerm)

	// Verify the intersection operation
	intersectionOp, ok := editPerm.operation.(*IntersectionOperation)
	require.True(t, ok)
	require.Len(t, intersectionOp.Children(), 2)

	// Verify first child is owner relation ref
	ownerRef, ok := intersectionOp.Children()[0].(*RelationReference)
	require.True(t, ok)
	require.Equal(t, "owner", ownerRef.RelationName())

	// Verify second child is exclusion
	exclusionOp, ok := intersectionOp.Children()[1].(*ExclusionOperation)
	require.True(t, ok)
	require.NotNil(t, exclusionOp.Left())
	require.NotNil(t, exclusionOp.Right())
}

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestCloneSchema_Nil(t *testing.T) {
	cloned := CloneSchema(nil)
	require.Nil(t, cloned)
}

func TestCloneSchema_Empty(t *testing.T) {
	original := &Schema{
		definitions: make(map[string]*Definition),
		caveats:     make(map[string]*Caveat),
	}

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	require.NotSame(t, original, cloned)
	require.Empty(t, cloned.definitions)
	require.Empty(t, cloned.caveats)
}

func TestCloneSchema_WithDefinitions(t *testing.T) {
	original := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
			"document": {
				name:        "document",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: make(map[string]*Caveat),
	}
	original.definitions["user"].parent = original
	original.definitions["document"].parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	require.NotSame(t, original, cloned)
	require.Len(t, cloned.definitions, 2)

	// Check that definitions are cloned
	require.Contains(t, cloned.definitions, "user")
	require.Contains(t, cloned.definitions, "document")
	require.NotSame(t, original.definitions["user"], cloned.definitions["user"])
	require.NotSame(t, original.definitions["document"], cloned.definitions["document"])

	// Check that parent references are updated
	require.Same(t, cloned, cloned.definitions["user"].parent)
	require.Same(t, cloned, cloned.definitions["document"].parent)

	// Check that values are preserved
	require.Equal(t, "user", cloned.definitions["user"].name)
	require.Equal(t, "document", cloned.definitions["document"].name)
}

func TestCloneSchema_WithCaveats(t *testing.T) {
	original := &Schema{
		definitions: make(map[string]*Definition),
		caveats: map[string]*Caveat{
			"is_admin": {
				name:       "is_admin",
				expression: "admin == true",
				parameters: []CaveatParameter{
					{name: "admin", typ: "bool"},
				},
			},
		},
	}
	original.caveats["is_admin"].parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	require.Len(t, cloned.caveats, 1)

	// Check that caveat is cloned
	require.Contains(t, cloned.caveats, "is_admin")
	require.NotSame(t, original.caveats["is_admin"], cloned.caveats["is_admin"])

	// Check that parent reference is updated
	require.Same(t, cloned, cloned.caveats["is_admin"].parent)

	// Check that values are preserved
	require.Equal(t, "is_admin", cloned.caveats["is_admin"].name)
	require.Equal(t, "admin == true", cloned.caveats["is_admin"].expression)
	require.Len(t, cloned.caveats["is_admin"].parameters, 1)
	require.Equal(t, "admin", cloned.caveats["is_admin"].parameters[0].name)
	require.Equal(t, "bool", cloned.caveats["is_admin"].parameters[0].typ)

	// Check that parameters slice is a new slice by verifying mutations don't affect original
	cloned.caveats["is_admin"].parameters[0] = CaveatParameter{name: "modified", typ: "string"}
	require.Equal(t, "admin", original.caveats["is_admin"].parameters[0].name)
	require.Equal(t, "bool", original.caveats["is_admin"].parameters[0].typ)
}

func TestCloneSchema_WithRelations(t *testing.T) {
	br1 := &BaseRelation{
		subjectType: "user",
		subrelation: "",
		caveat:      "is_member",
		expiration:  true,
		wildcard:    false,
	}
	br2 := &BaseRelation{
		subjectType: "user",
		subrelation: "member",
		caveat:      "",
		expiration:  false,
		wildcard:    false,
	}
	br3 := &BaseRelation{
		subjectType: "user",
		wildcard:    true,
	}

	rel := &Relation{
		name:             "viewer",
		baseRelations:    []*BaseRelation{br1, br2, br3},
		aliasingRelation: "",
	}
	br1.parent = rel
	br2.parent = rel
	br3.parent = rel

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: make(map[string]*Permission),
	}
	rel.parent = def

	original := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	clonedDef := cloned.definitions["document"]
	require.NotNil(t, clonedDef)
	require.NotSame(t, def, clonedDef)

	clonedRel := clonedDef.relations["viewer"]
	require.NotNil(t, clonedRel)
	require.NotSame(t, rel, clonedRel)

	// Check parent references
	require.Same(t, clonedDef, clonedRel.parent)

	// Check base relations
	require.Len(t, clonedRel.baseRelations, 3)
	for i, clonedBr := range clonedRel.baseRelations {
		require.NotSame(t, rel.baseRelations[i], clonedBr)
		require.Same(t, clonedRel, clonedBr.parent)
	}

	// Check values
	require.Equal(t, "user", clonedRel.baseRelations[0].subjectType)
	require.Equal(t, "", clonedRel.baseRelations[0].subrelation)
	require.Equal(t, "is_member", clonedRel.baseRelations[0].caveat)
	require.True(t, clonedRel.baseRelations[0].expiration)
	require.False(t, clonedRel.baseRelations[0].wildcard)

	require.Equal(t, "user", clonedRel.baseRelations[1].subjectType)
	require.Equal(t, "member", clonedRel.baseRelations[1].subrelation)
	require.Equal(t, "", clonedRel.baseRelations[1].caveat)
	require.False(t, clonedRel.baseRelations[1].expiration)
	require.False(t, clonedRel.baseRelations[1].wildcard)

	require.Equal(t, "user", clonedRel.baseRelations[2].subjectType)
	require.True(t, clonedRel.baseRelations[2].wildcard)
}

func TestCloneSchema_WithPermissions(t *testing.T) {
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "viewer"},
			&ArrowReference{left: "parent", right: "viewer"},
			&IntersectionOperation{
				children: []Operation{
					&RelationReference{relationName: "editor"},
					&RelationReference{relationName: "approved"},
				},
			},
			&ExclusionOperation{
				left:  &RelationReference{relationName: "member"},
				right: &RelationReference{relationName: "banned"},
			},
		},
	}

	perm := &Permission{
		name:      "view",
		operation: op,
	}

	def := &Definition{
		name:      "document",
		relations: make(map[string]*Relation),
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	perm.parent = def

	original := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	clonedDef := cloned.definitions["document"]
	require.NotNil(t, clonedDef)

	clonedPerm := clonedDef.permissions["view"]
	require.NotNil(t, clonedPerm)
	require.NotSame(t, perm, clonedPerm)

	// Check parent reference
	require.Same(t, clonedDef, clonedPerm.parent)

	// Check that operation tree is cloned
	require.NotSame(t, op, clonedPerm.operation)

	clonedUnion := clonedPerm.operation.(*UnionOperation)
	require.Len(t, clonedUnion.children, 4)

	// Check RelationReference
	clonedRelRef := clonedUnion.children[0].(*RelationReference)
	require.NotSame(t, op.children[0], clonedRelRef)
	require.Equal(t, "viewer", clonedRelRef.relationName)

	// Check ArrowReference
	clonedArrowRef := clonedUnion.children[1].(*ArrowReference)
	require.NotSame(t, op.children[1], clonedArrowRef)
	require.Equal(t, "parent", clonedArrowRef.left)
	require.Equal(t, "viewer", clonedArrowRef.right)

	// Check IntersectionOperation
	clonedIntersection := clonedUnion.children[2].(*IntersectionOperation)
	require.NotSame(t, op.children[2], clonedIntersection)
	require.Len(t, clonedIntersection.children, 2)

	// Check ExclusionOperation
	clonedExclusion := clonedUnion.children[3].(*ExclusionOperation)
	require.NotSame(t, op.children[3], clonedExclusion)
	require.IsType(t, &RelationReference{}, clonedExclusion.left)
	require.IsType(t, &RelationReference{}, clonedExclusion.right)
}

func TestCloneSchema_CompleteSchema(t *testing.T) {
	// Build a complete schema with all node types
	br1 := &BaseRelation{
		subjectType: "user",
	}
	rel1 := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{br1},
	}
	br1.parent = rel1

	perm1 := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&RelationReference{relationName: "viewer"},
				&ArrowReference{left: "parent", right: "view"},
			},
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

	caveat1 := &Caveat{
		name:       "is_admin",
		expression: "admin == true",
		parameters: []CaveatParameter{
			{name: "admin", typ: "bool"},
		},
	}

	original := &Schema{
		definitions: map[string]*Definition{
			"document": def1,
		},
		caveats: map[string]*Caveat{
			"is_admin": caveat1,
		},
	}
	def1.parent = original
	caveat1.parent = original

	cloned := CloneSchema(original)

	// Verify schema is cloned
	require.NotNil(t, cloned)
	require.NotSame(t, original, cloned)

	// Verify definition is cloned with correct parent
	clonedDef := cloned.definitions["document"]
	require.NotNil(t, clonedDef)
	require.NotSame(t, def1, clonedDef)
	require.Same(t, cloned, clonedDef.parent)

	// Verify relation is cloned with correct parent
	clonedRel := clonedDef.relations["viewer"]
	require.NotNil(t, clonedRel)
	require.NotSame(t, rel1, clonedRel)
	require.Same(t, clonedDef, clonedRel.parent)

	// Verify base relation is cloned with correct parent
	require.Len(t, clonedRel.baseRelations, 1)
	clonedBr := clonedRel.baseRelations[0]
	require.NotSame(t, br1, clonedBr)
	require.Same(t, clonedRel, clonedBr.parent)

	// Verify permission is cloned with correct parent
	clonedPerm := clonedDef.permissions["view"]
	require.NotNil(t, clonedPerm)
	require.NotSame(t, perm1, clonedPerm)
	require.Same(t, clonedDef, clonedPerm.parent)

	// Verify operation tree is cloned
	require.NotSame(t, perm1.operation, clonedPerm.operation)

	// Verify caveat is cloned with correct parent
	clonedCaveat := cloned.caveats["is_admin"]
	require.NotNil(t, clonedCaveat)
	require.NotSame(t, caveat1, clonedCaveat)
	require.Same(t, cloned, clonedCaveat.parent)
	require.Equal(t, "is_admin", clonedCaveat.name)
	require.Equal(t, "admin == true", clonedCaveat.expression)
}

func TestCloneSchema_Mutation(t *testing.T) {
	// Test that mutations to the cloned schema don't affect the original
	original := &Schema{
		definitions: map[string]*Definition{
			"user": {
				name:        "user",
				relations:   make(map[string]*Relation),
				permissions: make(map[string]*Permission),
			},
		},
		caveats: make(map[string]*Caveat),
	}
	original.definitions["user"].parent = original

	cloned := CloneSchema(original)

	// Mutate the cloned schema
	cloned.definitions["user"].name = "modified_user"
	newDef := &Definition{
		name:        "document",
		relations:   make(map[string]*Relation),
		permissions: make(map[string]*Permission),
	}
	newDef.parent = cloned
	cloned.definitions["document"] = newDef

	// Verify original is unchanged
	require.Equal(t, "user", original.definitions["user"].name)
	require.Len(t, original.definitions, 1)
	require.NotContains(t, original.definitions, "document")
}

func TestCloneSchema_WithFunctionedArrowReference(t *testing.T) {
	// Test cloning with FunctionedArrowReference
	anyOp := &FunctionedArrowReference{
		left:     "parent",
		function: FunctionTypeAny,
		right:    "viewer",
	}

	allOp := &FunctionedArrowReference{
		left:     "group",
		function: FunctionTypeAll,
		right:    "member",
	}

	// Test with both any() and all() functions in a union
	op := &UnionOperation{
		children: []Operation{
			anyOp,
			allOp,
			&RelationReference{relationName: "direct_viewer"},
		},
	}

	perm := &Permission{
		name:      "view",
		operation: op,
	}

	def := &Definition{
		name:      "document",
		relations: make(map[string]*Relation),
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	perm.parent = def

	original := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	clonedDef := cloned.definitions["document"]
	require.NotNil(t, clonedDef)

	clonedPerm := clonedDef.permissions["view"]
	require.NotNil(t, clonedPerm)
	require.NotSame(t, perm, clonedPerm)

	// Check parent reference
	require.Same(t, clonedDef, clonedPerm.parent)

	// Check that operation tree is cloned
	require.NotSame(t, op, clonedPerm.operation)

	clonedUnion := clonedPerm.operation.(*UnionOperation)
	require.Len(t, clonedUnion.children, 3)

	// Check FunctionedArrowReference with any()
	clonedAnyOp := clonedUnion.children[0].(*FunctionedArrowReference)
	require.NotSame(t, anyOp, clonedAnyOp)
	require.Equal(t, "parent", clonedAnyOp.left)
	require.Equal(t, FunctionTypeAny, clonedAnyOp.function)
	require.Equal(t, "viewer", clonedAnyOp.right)

	// Check FunctionedArrowReference with all()
	clonedAllOp := clonedUnion.children[1].(*FunctionedArrowReference)
	require.NotSame(t, allOp, clonedAllOp)
	require.Equal(t, "group", clonedAllOp.left)
	require.Equal(t, FunctionTypeAll, clonedAllOp.function)
	require.Equal(t, "member", clonedAllOp.right)

	// Check RelationReference
	clonedRelRef := clonedUnion.children[2].(*RelationReference)
	require.NotSame(t, op.children[2], clonedRelRef)
	require.Equal(t, "direct_viewer", clonedRelRef.relationName)
}

func TestCloneOperation_FunctionedArrowReference(t *testing.T) {
	// Test direct cloning of FunctionedArrowReference
	tests := []struct {
		name string
		op   *FunctionedArrowReference
	}{
		{
			name: "any function",
			op: &FunctionedArrowReference{
				left:     "parent",
				function: FunctionTypeAny,
				right:    "viewer",
			},
		},
		{
			name: "all function",
			op: &FunctionedArrowReference{
				left:     "group",
				function: FunctionTypeAll,
				right:    "admin",
			},
		},
		{
			name: "nil operation",
			op:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloned := tt.op.clone()

			if tt.op == nil {
				require.Nil(t, cloned)
				return
			}

			require.NotNil(t, cloned)
			require.NotSame(t, tt.op, cloned)

			clonedOp := cloned.(*FunctionedArrowReference)
			require.Equal(t, tt.op.left, clonedOp.left)
			require.Equal(t, tt.op.function, clonedOp.function)
			require.Equal(t, tt.op.right, clonedOp.right)

			// Test that mutation doesn't affect original
			clonedOp.left = "modified"
			require.NotEqual(t, tt.op.left, clonedOp.left)
		})
	}
}

func TestCloneSchema_ComplexOperationWithFunctionedTupleset(t *testing.T) {
	// Test a complex nested operation tree that includes FunctionedArrowReference
	op := &UnionOperation{
		children: []Operation{
			&RelationReference{relationName: "direct"},
			&IntersectionOperation{
				children: []Operation{
					&FunctionedArrowReference{
						left:     "parent",
						function: FunctionTypeAny,
						right:    "viewer",
					},
					&RelationReference{relationName: "approved"},
				},
			},
			&ExclusionOperation{
				left: &FunctionedArrowReference{
					left:     "organization",
					function: FunctionTypeAll,
					right:    "member",
				},
				right: &RelationReference{relationName: "suspended"},
			},
		},
	}

	perm := &Permission{
		name:      "access",
		operation: op,
	}

	def := &Definition{
		name:      "resource",
		relations: make(map[string]*Relation),
		permissions: map[string]*Permission{
			"access": perm,
		},
	}
	perm.parent = def

	original := &Schema{
		definitions: map[string]*Definition{
			"resource": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = original

	cloned := CloneSchema(original)

	require.NotNil(t, cloned)
	clonedDef := cloned.definitions["resource"]
	clonedPerm := clonedDef.permissions["access"]
	clonedUnion := clonedPerm.operation.(*UnionOperation)

	// Verify the entire tree is properly cloned
	require.NotSame(t, op, clonedUnion)
	require.Len(t, clonedUnion.children, 3)

	// Check intersection with FunctionedArrowReference
	clonedIntersection := clonedUnion.children[1].(*IntersectionOperation)
	require.NotSame(t, op.children[1], clonedIntersection)
	clonedFuncOp1 := clonedIntersection.children[0].(*FunctionedArrowReference)
	require.Equal(t, "parent", clonedFuncOp1.left)
	require.Equal(t, FunctionTypeAny, clonedFuncOp1.function)
	require.Equal(t, "viewer", clonedFuncOp1.right)

	// Check exclusion with FunctionedArrowReference
	clonedExclusion := clonedUnion.children[2].(*ExclusionOperation)
	require.NotSame(t, op.children[2], clonedExclusion)
	clonedFuncOp2 := clonedExclusion.left.(*FunctionedArrowReference)
	require.Equal(t, "organization", clonedFuncOp2.left)
	require.Equal(t, FunctionTypeAll, clonedFuncOp2.function)
	require.Equal(t, "member", clonedFuncOp2.right)
}

func TestClone_ResolvedFunctionedArrowReference(t *testing.T) {
	// Create a test schema with a functioned arrow
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

	// Step 4: Clone the resolved schema
	cloned := resolved.schema.clone()
	require.NotNil(t, cloned)

	// Verify that cloning worked correctly
	clonedDef := cloned.definitions["document"]
	require.NotNil(t, clonedDef)
	clonedPerm := clonedDef.permissions["view"]
	require.NotNil(t, clonedPerm)

	// The operation should be a ResolvedFunctionedArrowReference
	_, ok := clonedPerm.operation.(*ResolvedFunctionedArrowReference)
	require.True(t, ok, "expected ResolvedFunctionedArrowReference after clone")
}

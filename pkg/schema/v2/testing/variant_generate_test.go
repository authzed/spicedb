package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// relationshipValid returns whether the relationship has a place in the schema.
func relationshipValid(s *schema.Schema, rel tuple.Relationship) bool {
	definition, ok := s.GetTypeDefinition(rel.Resource.ObjectType)
	if !ok {
		return false
	}

	relation, ok := definition.GetRelation(rel.Resource.Relation)
	if !ok {
		return false
	}

	for _, baseRelation := range relation.BaseRelations() {
		if baseRelation.Type() == rel.Subject.ObjectType && baseRelation.Subrelation() == rel.Subject.Relation {
			return true
		}
	}
	return false
}

// rewriteFor returns the compiled expression of the permission.
func rewriteFor(t require.TestingT, s *schema.Schema, definitionName, permissionName string) *core.UsersetRewrite {
	definitions, _, err := s.ToDefinitions()
	require.NoError(t, err)

	for _, definition := range definitions {
		if definition.Name != definitionName {
			continue
		}
		for _, relation := range definition.Relation {
			if relation.Name == permissionName {
				return relation.UsersetRewrite
			}
		}
	}

	require.Failf(t, "permission not found", "%s#%s", definitionName, permissionName)
	return nil
}

// requireVariantProperties asserts the properties that hold for every generated variant.
func requireVariantProperties(t *rapid.T, v Variant) {
	require.NotEmpty(t, v.Mutations)

	type target struct{ definition, name string }
	targets := map[target]bool{}
	for _, mutation := range v.Mutations {
		key := target{mutation.Definition, mutation.Name}
		require.False(t, targets[key], "found multiple mutations for %v", key)
		targets[key] = true
	}

	// The mutations are complete: applying them again produces the same schema.
	reapplied, err := NewVariant(v.Base, v.Mutations)
	require.NoError(t, err)
	require.Equal(t, schemaText(t, v.Schema), schemaText(t, reapplied.Schema))
	require.NotEqual(t, schemaText(t, v.Base), schemaText(t, v.Schema))

	// Applying the inverse of the mutations produces the base once more.
	inverse := v.Inverse()
	reverted, err := NewVariant(v.Schema, inverse.Mutations)
	require.NoError(t, err)
	require.Equal(t, schemaText(t, v.Base), schemaText(t, reverted.Schema))
	require.Equal(t, schemaText(t, v.Base), schemaText(t, inverse.Schema))
	require.Len(t, inverse.Mutations, len(v.Mutations))

	for _, mutation := range v.Mutations {
		switch mutation.Kind {
		case AddUnionChild:
			// The child was added to a union with at least one other child.
			after, _ := v.Schema.Definitions()[mutation.Definition].GetPermission(mutation.Name)
			union, ok := after.Operation().(*schema.UnionOperation)
			require.True(t, ok, "expected a union for %s", mutation)
			require.GreaterOrEqual(t, len(union.Children()), 2)

		case RemoveUnionChild:
			// The child was removed from a union with at least one other child.
			before, _ := v.Base.Definitions()[mutation.Definition].GetPermission(mutation.Name)
			union, ok := before.Operation().(*schema.UnionOperation)
			require.True(t, ok, "expected a union for %s", mutation)
			require.GreaterOrEqual(t, len(union.Children()), 2)

		case ChangePermission:
		default:
			continue
		}

		before := rewriteFor(t, v.Base, mutation.Definition, mutation.Name)
		after := rewriteFor(t, v.Schema, mutation.Definition, mutation.Name)
		require.False(t, proto.Equal(before, after), "found unchanged permission for %s", mutation)

		// The change must also survive being written out as schema text.
		before = rewriteFor(t, mustSchema(t, schemaText(t, v.Base)), mutation.Definition, mutation.Name)
		after = rewriteFor(t, mustSchema(t, schemaText(t, v.Schema)), mutation.Definition, mutation.Name)
		require.False(t, proto.Equal(before, after), "found unchanged permission as text for %s", mutation)
	}

	// Every arrow has its target on every subject type allowed by its left relation.
	for definitionName, definition := range v.Schema.Definitions() {
		for permissionName, permission := range definition.Permissions() {
			walkOperation(permission.Operation(), func(op schema.Operation) {
				left, right, ok := arrowNames(op)
				if !ok {
					return
				}

				relation, ok := definition.GetRelation(left)
				require.True(t, ok, "missing left relation %s for arrow in %s#%s", left, definitionName, permissionName)
				for _, baseRelation := range relation.BaseRelations() {
					subjectDefinition, ok := v.Schema.GetTypeDefinition(baseRelation.Type())
					require.True(t, ok)

					_, isRelation := subjectDefinition.GetRelation(right)
					_, isPermission := subjectDefinition.GetPermission(right)
					require.True(t, isRelation || isPermission, "missing arrow target %s->%s on %s in %s#%s", left, right, baseRelation.Type(), definitionName, permissionName)
				}
			})
		}
	}
}

func TestGenerateVariantProperties(t *testing.T) {
	CheckWithSchemaVariant(t, func(t *rapid.T, v Variant, _ RelationshipGenerator) {
		requireVariantProperties(t, v)
	})
}

func TestGenerateVariantChains(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		current, _ := GenerateSchema(t)
		for range 3 {
			v := GenerateVariant(t, current)
			requireVariantProperties(t, v)
			current = v.Schema
		}
	})
}

func TestGenerateVariantMinimalBase(t *testing.T) {
	base := mustSchema(t, `
		definition user {}

		definition doc {
			relation viewer: user
		}`)

	rapid.Check(t, func(t *rapid.T) {
		requireVariantProperties(t, GenerateVariant(t, base))
	})
}

func TestGenerateVariantHandWrittenBase(t *testing.T) {
	base := mustSchema(t, `
		definition user {}

		definition group {
			relation member: user | group#member
			permission membership = member
		}

		definition folder {
			relation parent: folder
			relation owner: user | group#member
			permission manage = owner + parent->manage
		}

		definition doc {
			relation folder: folder
			relation viewer: user | group#membership
			permission view = viewer + folder->manage
			permission audit = view & viewer
		}`)

	rapid.Check(t, func(t *rapid.T) {
		current := base
		for range 3 {
			v := GenerateVariant(t, current)
			requireVariantProperties(t, v)
			current = v.Schema
		}
	})
}

func TestGenerateVariantRejectsUnsupportedBase(t *testing.T) {
	_, err := NewVariant(mustSchema(t, `
		definition user {}

		definition doc {
			relation viewer: user:*
		}`), nil)
	require.ErrorContains(t, err, "unsupported schema feature: wildcard at doc#viewer")
}

func TestGenerateVariantReachesEveryKind(t *testing.T) {
	seen := map[MutationKind]bool{}
	seenSubjectRelationRemoval := false

	rapid.Check(t, func(t *rapid.T) {
		current, _ := GenerateSchema(t)
		for range 3 {
			v := GenerateVariant(t, current)
			for _, mutation := range v.Mutations {
				seen[mutation.Kind] = true

				if mutation.Kind != RemoveRelation {
					continue
				}
				for _, definition := range v.Base.Definitions() {
					for _, relation := range definition.Relations() {
						for _, baseRelation := range relation.BaseRelations() {
							if baseRelation.Type() == mutation.Definition && baseRelation.Subrelation() == mutation.Name {
								seenSubjectRelationRemoval = true
							}
						}
					}
				}
			}
			current = v.Schema
		}
	})

	for kind := AddDefinition; kind <= RemoveUnionChild; kind++ {
		require.True(t, seen[kind], "never generated %s", kind)
	}
	require.True(t, seenSubjectRelationRemoval, "never removed a relation used as a subject relation")
}

func TestVariantRelationshipGenerators(t *testing.T) {
	CheckWithSchemaVariant(t, func(t *rapid.T, v Variant, _ RelationshipGenerator) {
		// NOTE: removing a definition only breaks data if the definition had relations.
		breaksData := false
		allowsNewData := false
		for _, mutation := range v.Mutations {
			switch mutation.Kind {
			case RemoveRelation, RemoveAllowedType:
				breaksData = true
			case RemoveDefinition:
				breaksData = breaksData || len(v.Base.Definitions()[mutation.Definition].Relations()) > 0
			case AddDefinition, AddRelation, AddAllowedType:
				allowsNewData = true
			}
		}

		removedCount := 0
		for rel := range v.GenerateRemovedRelationships(t) {
			require.NotEmpty(t, v.Invalidated(rel), "relationship %s is not invalidated", rel)
			require.True(t, relationshipValid(v.Base, rel))
			require.False(t, relationshipValid(v.Schema, rel))
			_ = tuple.MustParse(rel.String())

			removedCount++
			if removedCount >= 10 {
				break
			}
		}
		require.Equal(t, breaksData, removedCount > 0)

		addedCount := 0
		for rel := range v.GenerateAddedRelationships(t) {
			require.True(t, relationshipValid(v.Schema, rel), "relationship %s is not valid in the variant", rel)
			require.False(t, relationshipValid(v.Base, rel), "relationship %s is valid in the base", rel)
			require.Empty(t, v.Invalidated(rel))
			_ = tuple.MustParse(rel.String())

			addedCount++
			if addedCount >= 10 {
				break
			}
		}
		require.Equal(t, allowsNewData, addedCount > 0)
	})
}

func TestVariantBuilderDoesNotReuseRemovedNames(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		vb := &variantBuilder{
			t:                   t,
			working:             modelFromSchema(mustSchema(t, mutationBaseSchemaWithFolder)),
			targetedNames:       map[memberTarget]struct{}{},
			targetedDefinitions: map[string]struct{}{},
		}

		require.True(t, vb.nameAvailable("doc", "auditor"))
		require.False(t, vb.nameAvailable("doc", "viewer"))
		require.True(t, vb.definitionNameAvailable("project"))
		require.False(t, vb.definitionNameAvailable("folder"))

		vb.record(Mutation{Kind: RemovePermission, Definition: "doc", Name: "edit"})
		vb.record(Mutation{Kind: RemoveDefinition, Definition: "folder"})

		// NOTE: reusing the name of something removed would be an overlapping mutation.
		require.False(t, vb.nameAvailable("doc", "edit"))
		require.False(t, vb.definitionNameAvailable("folder"))
	})
}

func TestVariantRelationshipGeneratorsWithObjectIDs(t *testing.T) {
	objectIDs := []string{"first", "second"}

	CheckWithSchemaVariant(t, func(t *rapid.T, v Variant, _ RelationshipGenerator) {
		v = v.WithObjectIDs(objectIDs...)

		check := func(generated func(yield func(tuple.Relationship) bool)) {
			counter := 0
			for rel := range generated {
				require.Contains(t, objectIDs, rel.Resource.ObjectID)
				require.Contains(t, objectIDs, rel.Subject.ObjectID)
				counter++
				if counter >= 5 {
					break
				}
			}
		}

		check(v.GenerateRemovedRelationships(t))
		check(v.GenerateAddedRelationships(t))
		check(v.Relationships.GenerateRelationships(t))
	})
}

func TestGenerateVariantWithMutationKinds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base, _ := GenerateSchema(t)
		v := GenerateVariant(t, base, WithMutationKinds(AddUnionChild, AddRelation))
		requireVariantProperties(t, v)

		for _, mutation := range v.Mutations {
			require.Contains(t, []MutationKind{AddUnionChild, AddRelation}, mutation.Kind)
		}
	})
}

func TestGenerateVariantWithMutationKindsRestrictsRequiredMutations(t *testing.T) {
	// NOTE: removing a relation used by a permission requires changing the permission, which
	// is not one of the kinds allowed.
	rapid.Check(t, func(t *rapid.T) {
		base, _ := GenerateSchema(t)
		v := GenerateVariant(t, base, WithMutationKinds(RemoveRelation, AddPermission))
		requireVariantProperties(t, v)

		for _, mutation := range v.Mutations {
			require.Contains(t, []MutationKind{RemoveRelation, AddPermission}, mutation.Kind)
		}
	})
}

func TestGenerateVariantWithMutationCount(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base, _ := GenerateSchema(t)
		v := GenerateVariant(t, base,
			WithMutationKinds(AddUnionChild),
			WithMutationCount(1, 1),
			WithRelationUnionChildren(),
		)
		requireVariantProperties(t, v)

		require.Len(t, v.Mutations, 1)
		require.Equal(t, AddUnionChild, v.Mutations[0].Kind)

		_, isRelation := relationReferenceName(v.Mutations[0].Operation)
		require.True(t, isRelation, "expected a relation for the child of %s", v.Mutations[0])
	})
}

func TestGenerateVariantWithMutationKindWeight(t *testing.T) {
	weighted, total := 0, 0

	rapid.Check(t, func(t *rapid.T) {
		base, _ := GenerateSchema(t)
		v := GenerateVariant(t, base, WithMutationKindWeight(AddUnionChild, 50), WithMutationCount(1, 1))
		requireVariantProperties(t, v)

		total++
		if v.Mutations[0].Kind == AddUnionChild {
			weighted++
		}
	})

	require.Greater(t, weighted*2, total, "expected most variants to add a union child")
}

func TestGenerateVariantWithMutationKindWeightOfZero(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base, _ := GenerateSchema(t)
		v := GenerateVariant(t, base, WithMutationKindWeight(AddDefinition, 0), WithMutationKindWeight(ChangePermission, 0))
		requireVariantProperties(t, v)

		for _, mutation := range v.Mutations {
			require.NotContains(t, []MutationKind{AddDefinition, ChangePermission}, mutation.Kind)
		}
	})
}

func TestGenerateVariantSkipsWithoutApplicableKinds(t *testing.T) {
	withUnion := mustSchema(t, mutationBaseSchema)
	withoutUnion := mustSchema(t, `
		definition user {}

		definition doc {
			relation viewer: user
			permission view = viewer
		}`)

	found := 0
	rapid.Check(t, func(t *rapid.T) {
		base := withoutUnion
		if rapid.Bool().Draw(t, "withUnion") {
			base = withUnion
		}

		// NOTE: the test case is skipped if there is no union from which to remove a child.
		v := GenerateVariant(t, base, WithMutationKinds(RemoveUnionChild))
		require.Same(t, withUnion, base)
		require.Len(t, v.Mutations, 1)
		found++
	})
	require.Positive(t, found)
}

func TestCheckWithSchemaVariantOptions(t *testing.T) {
	CheckWithSchemaVariant(t, func(t *rapid.T, v Variant, _ RelationshipGenerator) {
		for _, mutation := range v.Mutations {
			require.Equal(t, AddRelation, mutation.Kind)
		}
	}, WithMutationKinds(AddRelation))
}

func TestGenerateVariantEmptyBase(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		requireVariantProperties(t, GenerateVariant(t, schema.NewSchemaBuilder().Build()))
	})
}

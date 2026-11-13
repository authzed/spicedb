package testing

import (
	"iter"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RelationshipGenerator is a helper for generating relationships for a schema.
type RelationshipGenerator struct {
	schema *schema.ResolvedSchema
}

// See parsing.go for reference regexes. max length is 64. We subtract 4 due to "o_" and first and last character
// expressed in the regex first and last segments.
const objectExpr = "[a-z0-9_][a-z0-9_]{0,59}[a-z0-9]"

// GenerateRelationships generates an infinite sequence of relationships for the schema.
// Relationships are randomly generated but valid according to the schema. A
// small shared ID pool connects resources to userset/arrow targets. The stream
// first visits every writable relation, including those on subject definitions,
// then samples relations indefinitely. A schema without writable relations
// produces an empty sequence. Use DrawScenario for coordinated initial graphs
// and update batches.
func (rg *RelationshipGenerator) GenerateRelationships(t *rapid.T) iter.Seq[tuple.Relationship] {
	return func(yield func(tuple.Relationship) bool) {
		var writable []tuple.RelationReference
		for _, typeName := range slices.Sorted(maps.Keys(rg.schema.Schema().Definitions())) {
			def, _ := rg.schema.Schema().GetTypeDefinition(typeName)
			for _, relationName := range slices.Sorted(maps.Keys(def.Relations())) {
				writable = append(writable, tuple.RelationReference{ObjectType: typeName, Relation: relationName})
			}
		}
		if len(writable) == 0 {
			return
		}
		objectIDs := rapid.SliceOfNDistinct(rapid.StringMatching("o_"+objectExpr), 2, 4, rapid.ID[string]).Draw(t, "objectIDs")
		for i := 0; ; i++ {
			var resourceRelation tuple.RelationReference
			if i < len(writable) {
				resourceRelation = writable[i]
			} else {
				resourceRelation = rapid.SampledFrom(writable).Draw(t, "resourceRelation")
			}
			resourceTypeName := resourceRelation.ObjectType
			relationName := resourceRelation.Relation
			resourceID := rapid.SampledFrom(objectIDs).Draw(t, "resourceID")
			resourceTypeDef, _ := rg.schema.Schema().GetTypeDefinition(resourceTypeName)

			// Lookup the available subject types for the relation.
			relationDef, _ := resourceTypeDef.GetRelation(relationName)
			allowedSubjectTypes := relationDef.BaseRelations()

			// Select a random subject type from the allowed types.
			allowedSubjectType := rapid.SampledFrom(allowedSubjectTypes).Draw(t, resourceTypeName+"#"+relationName+"-"+"subjectTypeName")

			// Generate a random subject ID.
			subjectID := rapid.SampledFrom(objectIDs).Draw(t, "subjectID")

			relationship := tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: resourceTypeName,
						ObjectID:   resourceID,
						Relation:   relationName,
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: allowedSubjectType.Type(),
						ObjectID:   subjectID,
						Relation:   allowedSubjectType.Subrelation(),
					},
				},
			}

			if !yield(relationship) {
				return
			}
		}
	}
}

// CheckWithSchema runs the provided handler with a randomly generated schema.
func CheckWithSchema(t *testing.T, handler func(t *rapid.T, schema *schema.Schema, relationshipGenerator RelationshipGenerator)) {
	t.Helper()
	rapid.Check(t, func(t *rapid.T) {
		rapidRelationString := rapid.StringMatching("r_" + objectExpr)
		rapidPermissionString := rapid.StringMatching("p_" + objectExpr)
		rapidSubjectDefinitionString := rapid.StringMatching("s_" + objectExpr)
		rapidResourceDefinitionString := rapid.StringMatching("d_" + objectExpr)

		builder := schema.NewSchemaBuilder()

		// Generate between 1 and 3 types to represent subjects.
		subjectTypeNames := rapid.SliceOfNDistinct(rapidSubjectDefinitionString, 1, 3, rapid.ID[string]).Draw(t, "subjectTypeNames")
		subjectTypeRelationMap := map[string]string{}
		for _, subjectTypeName := range subjectTypeNames {
			builder = builder.AddDefinition(subjectTypeName).Done()

			// Generate an optional subject relation.
			if rapid.Bool().Draw(t, "subjectRelationPresent-"+subjectTypeName) {
				// Generate a relation name.
				relationName := rapidRelationString.Draw(t, subjectTypeName+"-relationName")

				builder = builder.AddDefinition(subjectTypeName).
					AddRelation(relationName).
					AllowedDirectRelation(subjectTypeName).
					Done().
					Done()

				subjectTypeRelationMap[subjectTypeName] = relationName
			}
		}

		// Generate between 1 and 3 types to represent resources.
		resourceTypeNames := rapid.SliceOfNDistinct(rapidResourceDefinitionString, 1, 3, rapid.ID[string]).Draw(t, "resourceTypeNames")
		for _, resourceTypeName := range resourceTypeNames {
			resourceBuilder := builder.AddDefinition(resourceTypeName)
			arrowChoices := make([]arrowChoice, 0)

			// Generate between 3 and 5 relations per resource.
			relationNames := rapid.SliceOfNDistinct(rapidRelationString, 3, 5, rapid.ID[string]).Draw(t, resourceTypeName+"-relationNames")
			for _, relationName := range relationNames {
				relationBuilder := resourceBuilder.AddRelation(relationName)

				// Link the relation to between 1 and 3 subject types.
				subjectTypeNames := rapid.SliceOfNDistinct(rapid.SampledFrom(subjectTypeNames), 1, len(subjectTypeNames), rapid.ID[string]).Draw(t, resourceTypeName+"-"+relationName+"-subjectTypeNames")
				for _, subjectTypeName := range subjectTypeNames {
					subjectRelationName, ok := subjectTypeRelationMap[subjectTypeName]
					if ok {
						relationBuilder = relationBuilder.AllowedRelation(subjectTypeName, subjectRelationName)
					}

					relationBuilder = relationBuilder.AllowedDirectRelation(subjectTypeName)
				}
				// An arrow target must exist on every type allowed by its left relation.
				if len(subjectTypeNames) > 0 {
					target := subjectTypeRelationMap[subjectTypeNames[0]]
					if target != "" {
						valid := true
						for _, subjectTypeName := range subjectTypeNames[1:] {
							if subjectTypeRelationMap[subjectTypeName] != target {
								valid = false
								break
							}
						}
						if valid {
							arrowChoices = append(arrowChoices, arrowChoice{relationName, target})
						}
					}
				}

				resourceBuilder = relationBuilder.Done()
			}

			// Generate between 1 and 5 permissions per resource.
			permissionNames := rapid.SliceOfNDistinct(rapidPermissionString, 1, 5, rapid.ID[string]).Draw(t, resourceTypeName+"-permissionNames")
			referenceNames := slices.Clone(relationNames)
			for _, permissionName := range permissionNames {
				permBuilder := resourceBuilder.AddPermission(permissionName)
				op := mustGenerateOperation(t, referenceNames, arrowChoices, 3, resourceTypeName+"/"+permissionName)
				resourceBuilder = permBuilder.Operation(op).Done()
				referenceNames = append(referenceNames, permissionName)
			}
		}

		built := builder.Build()
		resolved, err := schema.ResolveSchema(built)
		require.NoError(t, err)

		handler(t, built, RelationshipGenerator{schema: resolved})
	})
}

type arrowChoice struct{ left, right string }

func mustGenerateOperation(t *rapid.T, relationNames []string, arrowChoices []arrowChoice, depthRemaining int, path string) schema.Operation {
	if depthRemaining <= 0 {
		if len(arrowChoices) > 0 && rapid.Bool().Draw(t, path+"::leafIsArrow") {
			choice := rapid.SampledFrom(arrowChoices).Draw(t, path+"::arrow")
			return schema.NewArrow(choice.left, choice.right)
		}

		// Base case: direct relation.
		relationName := rapid.SampledFrom(relationNames).Draw(t, "baseCaseRelationName")
		return schema.NewRelationRef(relationName)
	}

	maxChoice := 3
	if len(arrowChoices) > 0 {
		maxChoice = 4
	}
	choice := rapid.IntRange(0, maxChoice).Draw(t, path+"::permissionTypeChoice")
	switch choice {
	case 0:
		// Direct relation.
		relationName := rapid.SampledFrom(relationNames).Draw(t, path+"::directRelationName")
		return schema.NewRelationRef(relationName)

	case 1:
		// Union
		numChildren := rapid.IntRange(1, 3).Draw(t, path+"::unionNumChildren")
		unionBuilder := schema.NewUnion()
		for i := range numChildren {
			childOp := mustGenerateOperation(t, relationNames, arrowChoices, depthRemaining-1, path+"::unionChild#"+strconv.Itoa(i))
			unionBuilder = unionBuilder.Add(childOp)
		}
		return unionBuilder.Build()

	case 2:
		// Intersection
		numChildren := rapid.IntRange(1, 3).Draw(t, "intersectionNumChildren")
		intersectionBuilder := schema.NewIntersection()
		for i := range numChildren {
			childOp := mustGenerateOperation(t, relationNames, arrowChoices, depthRemaining-1, path+"::intersectionChild#"+strconv.Itoa(i))
			intersectionBuilder = intersectionBuilder.Add(childOp)
		}
		return intersectionBuilder.Build()

	case 3:
		// Exclusion
		leftOp := mustGenerateOperation(t, relationNames, arrowChoices, depthRemaining-1, path+"::exclusionLeft")
		rightOp := mustGenerateOperation(t, relationNames, arrowChoices, depthRemaining-1, path+"::exclusionRight")
		exclusionBuilder := schema.NewExclusion().Base(leftOp).Exclude(rightOp)
		return exclusionBuilder.Build()
	case 4:
		choice := rapid.SampledFrom(arrowChoices).Draw(t, path+"::arrow")
		return schema.NewArrow(choice.left, choice.right)

	default:
		panic("unsupported operation type")
	}
}

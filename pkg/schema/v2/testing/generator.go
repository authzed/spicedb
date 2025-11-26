package testing

import (
	"iter"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RelationshipGenerator is a helper for generating relationships for a schema.
type RelationshipGenerator struct {
	schema            *schema.ResolvedSchema
	resourceTypeNames []string
	subjectTypeNames  []string
}

// See parsing.go for reference regexes. max length is 64. We subtract 4 due to "o_" and first and last character
// expressed in the regex first and last segments.
const objectExpr = "[a-z0-9_][a-z0-9_]{0,59}[a-z0-9]"

// GenerateRelationships generates an infinite sequence of relationships for the schema.
// Relationships are randomly generated but valid according to the schema.
func (rg *RelationshipGenerator) GenerateRelationships(t *rapid.T) iter.Seq[tuple.Relationship] {
	rapidObjectString := rapid.StringMatching("o_" + objectExpr)

	return func(yield func(tuple.Relationship) bool) {
		for {
			// Select a random resource type.
			resourceTypeName := rapid.SampledFrom(rg.resourceTypeNames).Draw(t, "resourceTypeName")

			// Generate a random resource ID.
			resourceID := rapidObjectString.Draw(t, "resourceID")

			// Select a random resource relation.
			resourceTypeDef, _ := rg.schema.Schema().GetTypeDefinition(resourceTypeName)
			relationNames := slices.Collect(maps.Keys(resourceTypeDef.Relations()))
			relationName := rapid.SampledFrom(relationNames).Draw(t, "relationName")

			// Lookup the available subject types for the relation.
			relationDef, _ := resourceTypeDef.GetRelation(relationName)
			allowedSubjectTypes := relationDef.BaseRelations()

			// Select a random subject type from the allowed types.
			allowedSubjectType := rapid.SampledFrom(allowedSubjectTypes).Draw(t, resourceTypeName+"#"+relationName+"-"+"subjectTypeName")

			// Generate a random subject ID.
			subjectID := rapidObjectString.Draw(t, "subjectID")

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
		rapidDefinitionString := rapid.StringMatching("d_" + objectExpr)

		builder := schema.NewSchemaBuilder()

		// Generate between 1 and 3 types to represent subjects.
		subjectTypeNames := rapid.SliceOfNDistinct(rapidDefinitionString, 1, 3, rapid.ID[string]).Draw(t, "subjectTypeNames")
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
		resourceTypeNames := rapid.SliceOfNDistinct(rapidDefinitionString, 1, 3, rapid.ID[string]).Draw(t, "resourceTypeNames")
		for _, resourceTypeName := range resourceTypeNames {
			resourceBuilder := builder.AddDefinition(resourceTypeName)

			// Generate between 3 and 5 relations per resource.
			relationNames := rapid.SliceOfNDistinct(rapidRelationString, 3, 5, rapid.ID[string]).Draw(t, resourceTypeName+"-relationNames")
			for _, relationName := range relationNames {
				relationBuilder := resourceBuilder.AddRelation(relationName)

				// Link the relation to between 1 and 3 subject types.
				subjectTypeNames := rapid.SliceOfNDistinct(rapid.SampledFrom(subjectTypeNames), 1, 3, rapid.ID[string]).Draw(t, resourceTypeName+"-"+relationName+"-subjectTypeNames")
				addedSubjectTypeNames := mapz.NewSet[string]()
				for _, subjectTypeName := range subjectTypeNames {
					if !addedSubjectTypeNames.Add(subjectTypeName) {
						continue
					}

					subjectRelationName, ok := subjectTypeRelationMap[subjectTypeName]
					if ok {
						relationBuilder = relationBuilder.AllowedRelation(subjectTypeName, subjectRelationName)
					}

					relationBuilder = relationBuilder.AllowedDirectRelation(subjectTypeName)
				}

				resourceBuilder = relationBuilder.Done()
			}

			// Generate between 1 and 5 permissions per resource.
			subjectRelationNames := slices.Collect(maps.Values(subjectTypeRelationMap))
			permissionNames := rapid.SliceOfNDistinct(rapidPermissionString, 1, 5, rapid.ID[string]).Draw(t, resourceTypeName+"-permissionNames")
			for _, permissionName := range permissionNames {
				permBuilder := resourceBuilder.AddPermission(permissionName)
				op := mustGenerateOperation(t, relationNames, subjectRelationNames, 3, "")
				resourceBuilder = permBuilder.Operation(op).Done()
			}
		}

		built := builder.Build()
		resolved, err := schema.ResolveSchema(built)
		require.NoError(t, err)

		handler(t, built, RelationshipGenerator{
			schema:            resolved,
			resourceTypeNames: resourceTypeNames,
			subjectTypeNames:  subjectTypeNames,
		})
	})
}

func mustGenerateOperation(t *rapid.T, relationNames []string, subjectRelationNames []string, depthRemaining int, path string) schema.Operation {
	if depthRemaining <= 0 {
		if rapid.Bool().Draw(t, path+"::leafIsArrow") && len(subjectRelationNames) > 0 {
			leftRelationName := rapid.SampledFrom(relationNames).Draw(t, path+"::leftSideRelationName")
			rightRelationName := rapid.SampledFrom(subjectRelationNames).Draw(t, path+"::rightSideRelationName")
			return schema.NewArrow(leftRelationName, rightRelationName)
		}

		// Base case: direct relation.
		relationName := rapid.SampledFrom(relationNames).Draw(t, "baseCaseRelationName")
		return schema.NewRelationRef(relationName)
	}

	choice := rapid.IntRange(0, 3).Draw(t, path+"::permissionTypeChoice")
	switch choice {
	case 0:
		// Direct relation.
		relationName := rapid.SampledFrom(relationNames).Draw(t, path+"::directRelationName")
		return schema.NewRelationRef(relationName)

	case 1:
		// Union
		numChildren := rapid.IntRange(1, 3).Draw(t, path+"::unionNumChildren")
		unionBuilder := schema.NewUnion()
		for i := 0; i < numChildren; i++ {
			childOp := mustGenerateOperation(t, relationNames, subjectRelationNames, depthRemaining-1, path+"::unionChild#"+strconv.Itoa(i))
			unionBuilder = unionBuilder.Add(childOp)
		}
		return unionBuilder.Build()

	case 2:
		// Intersection
		numChildren := rapid.IntRange(1, 3).Draw(t, "intersectionNumChildren")
		intersectionBuilder := schema.NewIntersection()
		for i := 0; i < numChildren; i++ {
			childOp := mustGenerateOperation(t, relationNames, subjectRelationNames, depthRemaining-1, path+"::intersectionChild#"+strconv.Itoa(i))
			intersectionBuilder = intersectionBuilder.Add(childOp)
		}
		return intersectionBuilder.Build()

	case 3:
		// Exclusion
		leftOp := mustGenerateOperation(t, relationNames, subjectRelationNames, depthRemaining-1, path+"::exclusionLeft")
		rightOp := mustGenerateOperation(t, relationNames, subjectRelationNames, depthRemaining-1, path+"::exclusionRight")
		exclusionBuilder := schema.NewExclusion().Base(leftOp).Exclude(rightOp)
		return exclusionBuilder.Build()

	default:
		panic("unsupported operation type")
	}
}

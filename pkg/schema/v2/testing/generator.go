package testing

import (
	"errors"
	"fmt"
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

	// objectIDs are the object IDs to use in relationships, if any. If empty, object IDs
	// are randomly generated.
	objectIDs []string
}

// See parsing.go for reference regexes. max length is 64. We subtract 4 due to "o_" and first and last character
// expressed in the regex first and last segments.
const objectExpr = "[a-z0-9_][a-z0-9_]{0,59}[a-z0-9]"

// WithObjectIDs returns a copy of the generator that draws the IDs of resources and subjects
// from the given object IDs, rather than randomly generating them. Randomly generated IDs
// are almost never shared between relationships; a small set of object IDs produces
// relationships that connect to one another.
func (rg RelationshipGenerator) WithObjectIDs(objectIDs ...string) RelationshipGenerator {
	rg.objectIDs = slices.Clone(objectIDs)
	return rg
}

// objectIDGenerator returns the generator for object IDs: one sampling from the given object
// IDs, or one generating random IDs if there are none.
func objectIDGenerator(objectIDs []string) *rapid.Generator[string] {
	if len(objectIDs) > 0 {
		return rapid.SampledFrom(objectIDs)
	}
	return rapid.StringMatching("o_" + objectExpr)
}

// GenerateRelationships generates an infinite sequence of relationships for the schema.
// Relationships are randomly generated but valid according to the schema. The sequence is
// empty if the generator is the zero value, as it is for a schema without any relations.
func (rg RelationshipGenerator) GenerateRelationships(t *rapid.T) iter.Seq[tuple.Relationship] {
	rapidObjectString := objectIDGenerator(rg.objectIDs)

	return func(yield func(tuple.Relationship) bool) {
		if len(rg.resourceTypeNames) == 0 {
			return
		}

		for {
			// Select a random resource type.
			resourceTypeName := rapid.SampledFrom(rg.resourceTypeNames).Draw(t, "resourceTypeName")

			// Generate a random resource ID.
			resourceID := rapidObjectString.Draw(t, "resourceID")

			// Select a random resource relation.
			resourceTypeDef, _ := rg.schema.Schema().GetTypeDefinition(resourceTypeName)
			relationNames := slices.Collect(maps.Keys(resourceTypeDef.Relations()))
			slices.Sort(relationNames)
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
		built, relationshipGenerator := GenerateSchema(t)
		handler(t, built, relationshipGenerator)
	})
}

// GenerateSchema generates a random schema, along with a generator of relationships
// for it. It can be called multiple times within a single rapid check.
func GenerateSchema(t *rapid.T) (*schema.Schema, RelationshipGenerator) {
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
		for _, permissionName := range permissionNames {
			permBuilder := resourceBuilder.AddPermission(permissionName)
			op := mustGenerateOperation(t, relationNames, arrowChoices, 3, "")
			resourceBuilder = permBuilder.Operation(op).Done()
		}
	}

	built := builder.Build()
	resolved, err := schema.ResolveSchema(built)
	require.NoError(t, err)

	return built, RelationshipGenerator{
		schema:            resolved,
		resourceTypeNames: resourceTypeNames,
		subjectTypeNames:  subjectTypeNames,
	}
}

// NewRelationshipGenerator returns a generator of relationships for an existing schema,
// such as one compiled from schema text. Every definition with at least one relation is
// treated as a resource type.
func NewRelationshipGenerator(s *schema.Schema) (RelationshipGenerator, error) {
	if err := checkSupported(s); err != nil {
		return RelationshipGenerator{}, err
	}

	resolved, err := schema.ResolveSchema(s)
	if err != nil {
		return RelationshipGenerator{}, err
	}

	resourceTypeNames := make([]string, 0, len(s.Definitions()))
	subjectTypeNames := mapz.NewSet[string]()
	for _, definitionName := range sortedKeys(s.Definitions()) {
		hasRelation := false
		for _, relation := range s.Definitions()[definitionName].Relations() {
			for _, baseRelation := range relation.BaseRelations() {
				hasRelation = true
				subjectTypeNames.Add(baseRelation.Type())
			}
		}
		if hasRelation {
			resourceTypeNames = append(resourceTypeNames, definitionName)
		}
	}

	if len(resourceTypeNames) == 0 {
		return RelationshipGenerator{}, errors.New("schema has no relations to generate relationships for")
	}

	sortedSubjectTypeNames := subjectTypeNames.AsSlice()
	slices.Sort(sortedSubjectTypeNames)

	return RelationshipGenerator{
		schema:            resolved,
		resourceTypeNames: resourceTypeNames,
		subjectTypeNames:  sortedSubjectTypeNames,
	}, nil
}

// checkSupported returns an error if the schema makes use of a feature that the
// generators in this package do not (yet) understand.
func checkSupported(s *schema.Schema) error {
	unsupported := func(feature, definitionName, name string) error {
		return fmt.Errorf("unsupported schema feature: %s at %s#%s", feature, definitionName, name)
	}

	for _, definitionName := range sortedKeys(s.Definitions()) {
		definition := s.Definitions()[definitionName]
		for _, relationName := range sortedKeys(definition.Relations()) {
			for _, baseRelation := range definition.Relations()[relationName].BaseRelations() {
				switch {
				case baseRelation.Caveat() != "":
					return unsupported("caveat", definitionName, relationName)
				case baseRelation.Wildcard():
					return unsupported("wildcard", definitionName, relationName)
				case baseRelation.Expiration():
					return unsupported("expiration", definitionName, relationName)
				}
			}
		}

		for _, permissionName := range sortedKeys(definition.Permissions()) {
			var feature string
			walkOperation(definition.Permissions()[permissionName].Operation(), func(op schema.Operation) {
				switch op.(type) {
				case *schema.SelfReference:
					feature = "self"
				case *schema.NilReference:
					feature = "nil"
				case *schema.FunctionedArrowReference, *schema.ResolvedFunctionedArrowReference:
					feature = "functioned arrow"
				}
			})
			if feature != "" {
				return unsupported(feature, definitionName, permissionName)
			}
		}
	}

	for _, caveatName := range sortedKeys(s.Caveats()) {
		return fmt.Errorf("unsupported schema feature: caveat definition `%s`", caveatName)
	}

	return nil
}

// walkOperation invokes the visitor for the operation and all operations found under it.
func walkOperation(op schema.Operation, visitor func(op schema.Operation)) {
	visitor(op)

	switch typed := op.(type) {
	case *schema.UnionOperation:
		for _, child := range typed.Children() {
			walkOperation(child, visitor)
		}
	case *schema.IntersectionOperation:
		for _, child := range typed.Children() {
			walkOperation(child, visitor)
		}
	case *schema.ExclusionOperation:
		walkOperation(typed.Left(), visitor)
		walkOperation(typed.Right(), visitor)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

type arrowChoice struct{ left, right string }

func mustGenerateOperation(t *rapid.T, relationNames []string, arrowChoices []arrowChoice, depthRemaining int, path string) schema.Operation {
	if depthRemaining <= 0 {
		if len(arrowChoices) > 0 && rapid.Bool().Draw(t, path+"::leafIsArrow") {
			choice := rapid.SampledFrom(arrowChoices).Draw(t, path+"::arrow")
			return schema.NewArrow(choice.left, choice.right)
		}

		// Base case: direct relation.
		relationName := rapid.SampledFrom(relationNames).Draw(t, path+"::baseCaseRelationName")
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
		for i := range numChildren {
			childOp := mustGenerateOperation(t, relationNames, arrowChoices, depthRemaining-1, path+"::unionChild#"+strconv.Itoa(i))
			unionBuilder = unionBuilder.Add(childOp)
		}
		return unionBuilder.Build()

	case 2:
		// Intersection
		numChildren := rapid.IntRange(1, 3).Draw(t, path+"::intersectionNumChildren")
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

	default:
		panic("unsupported operation type")
	}
}

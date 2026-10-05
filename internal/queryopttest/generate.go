package queryopttest

import (
	"maps"
	"slices"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/schema/v2"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func broadCase(t *rapid.T, s *schema.Schema, rg schematesting.RelationshipGenerator) Case {
	c := Case{Schema: s}
	ids := []string{"o_a", "o_b", "o_c"}
	count := rapid.IntRange(0, 20).Draw(t, "relationshipCount")
	if count > 0 {
		for rel := range rg.GenerateRelationships(t) {
			rel.Resource.ObjectID = rapid.SampledFrom(ids).Draw(t, "resourceID")
			rel.Subject.ObjectID = rapid.SampledFrom(ids).Draw(t, "subjectID")
			c.Relationships = append(c.Relationships, rel)
			if len(c.Relationships) == count {
				break
			}
		}
	}
	names := slices.Sorted(maps.Keys(s.Definitions()))
	type target struct{ definition, permission string }
	var targets []target
	for _, name := range names {
		for _, permission := range slices.Sorted(maps.Keys(s.Definitions()[name].Permissions())) {
			targets = append(targets, target{name, permission})
		}
	}
	for i := 0; i < 2; i++ {
		target := rapid.SampledFrom(targets).Draw(t, "target")
		subjectType := rapid.SampledFrom(names).Draw(t, "subjectType")
		relation := tuple.Ellipsis
		if rapid.Bool().Draw(t, "namedSubject") {
			relations := slices.Sorted(maps.Keys(s.Definitions()[subjectType].Relations()))
			if len(relations) > 0 {
				relation = rapid.SampledFrom(relations).Draw(t, "subjectRelation")
			}
		}
		resourceID := rapid.SampledFrom(append(slices.Clone(ids), "absent")).Draw(t, "queryResourceID")
		subjectID := rapid.SampledFrom(append(slices.Clone(ids), "absent")).Draw(t, "querySubjectID")
		for _, op := range []query.Operation{query.OperationCheck, query.OperationIterSubjects, query.OperationIterResources} {
			c.Requests = append(c.Requests, Request{Params: optimization.RequestParams{Operation: op, SubjectType: subjectType, SubjectRelation: relation}, Resource: query.NewObject(target.definition, resourceID), Permission: target.permission, Subject: query.NewObject(subjectType, subjectID).WithRelation(relation)})
		}
	}
	return c
}

// ParseCase compiles a targeted schema and preserves caveat parameter types.
func ParseCase(t *rapid.T, text string) Case {
	compiled, err := compiler.Compile(compiler.InputSchema{Source: input.Source("property"), SchemaString: text}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	s, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
	require.NoError(t, err)
	return Case{Schema: s, CaveatDefinitions: compiled.CaveatDefinitions}
}

// Requests supplies bounded positive/negative anchors across all operations.
// A targeted generator can append named-subject or recursive self-edge requests.
func Requests(resourceType, permission, subjectType string) []Request {
	result := make([]Request, 0, 9)
	for _, id := range []string{"o_a", "o_b", "absent"} {
		for _, op := range []query.Operation{query.OperationCheck, query.OperationIterSubjects, query.OperationIterResources} {
			result = append(result, Request{Params: optimization.RequestParams{Operation: op, SubjectType: subjectType, SubjectRelation: tuple.Ellipsis}, Resource: query.NewObject(resourceType, id), Permission: permission, Subject: query.NewObject(subjectType, id).WithEllipses()})
		}
	}
	return result
}

// Relationships randomizes a valid candidate pool while retaining the first
// relationship so targeted cases have some connected, positive data.
func Relationships(t *rapid.T, candidates ...string) []tuple.Relationship {
	var rels []tuple.Relationship
	for i, text := range candidates {
		if i == 0 || rapid.Bool().Draw(t, "include:"+text) {
			rels = append(rels, tuple.MustParse(text))
		}
	}
	return rels
}

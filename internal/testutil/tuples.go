package testutil

import (
	"fmt"

	"golang.org/x/exp/slices"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var ONR = tuple.ObjectAndRelation

func JoinTuples(first []*core.RelationTuple, others ...[]*core.RelationTuple) []*core.RelationTuple {
	combined := slices.Clone(first)
	for _, other := range others {
		combined = append(combined, other...)
	}
	return combined
}

func GenTuplesWithOffset(resourceName string, relation string, subjectName string, subjectID string, offset int, number int) []*core.RelationTuple {
	return GenTuplesWithCaveat(resourceName, relation, subjectName, subjectID, "", nil, offset, number)
}

func GenTuples(resourceName string, relation string, subjectName string, subjectID string, number int) []*core.RelationTuple {
	return GenTuplesWithOffset(resourceName, relation, subjectName, subjectID, 0, number)
}

func GenResourceTuples(resourceName string, resourceID string, relation string, subjectName string, subjectRelation string, number int) []*core.RelationTuple {
	return GenResourceTuplesWithOffset(resourceName, resourceID, relation, subjectName, subjectRelation, 0, number)
}

func GenResourceTuplesWithOffset(resourceName string, resourceID string, relation string, subjectName string, subjectRelation string, offset int, number int) []*core.RelationTuple {
	tuples := make([]*core.RelationTuple, 0, number)
	for i := 0; i < number; i++ {
		tpl := &core.RelationTuple{
			ResourceAndRelation: ONR(resourceName, resourceID, relation),
			Subject:             ONR(subjectName, fmt.Sprintf("%s-%d", subjectName, i+offset), subjectRelation),
		}
		tuples = append(tuples, tpl)
	}
	return tuples
}

func GenResourceTuplesWithCaveat(resourceName string, resourceID string, relation string, subjectName string, subjectRelation string, caveatName string, context map[string]any, number int) []*core.RelationTuple {
	tuples := make([]*core.RelationTuple, 0, number)
	for i := 0; i < number; i++ {
		tpl := &core.RelationTuple{
			ResourceAndRelation: ONR(resourceName, resourceID, relation),
			Subject:             ONR(subjectName, fmt.Sprintf("%s-%d", subjectName, i), subjectRelation),
		}
		if caveatName != "" {
			tpl = tuple.MustWithCaveat(tpl, caveatName, context)
		}
		tuples = append(tuples, tpl)
	}
	return tuples
}

func GenSubjectTuples(resourceName string, relation string, subjectName string, subjectRelation string, number int) []*core.RelationTuple {
	tuples := make([]*core.RelationTuple, 0, number)
	for i := 0; i < number; i++ {
		tpl := &core.RelationTuple{
			ResourceAndRelation: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i), relation),
			Subject:             ONR(subjectName, fmt.Sprintf("%s-%d", subjectName, i), subjectRelation),
		}
		tuples = append(tuples, tpl)
	}
	return tuples
}

func GenSubjectIdsWithOffset(subjectName string, offset int, number int) []string {
	subjectIDs := make([]string, 0, number)
	for i := 0; i < number; i++ {
		subjectIDs = append(subjectIDs, fmt.Sprintf("%s-%d", subjectName, i+offset))
	}
	return subjectIDs
}

func GenSubjectIds(subjectName string, number int) []string {
	return GenSubjectIdsWithOffset(subjectName, 0, number)
}

func GenTuplesWithCaveat(resourceName string, relation string, subjectName string, subjectID string, caveatName string, context map[string]any, offset int, number int) []*core.RelationTuple {
	tuples := make([]*core.RelationTuple, 0, number)
	for i := 0; i < number; i++ {
		tpl := &core.RelationTuple{
			ResourceAndRelation: ONR(resourceName, fmt.Sprintf("%s-%d", resourceName, i+offset), relation),
			Subject:             ONR(subjectName, subjectID, "..."),
		}
		if caveatName != "" {
			tpl = tuple.MustWithCaveat(tpl, caveatName, context)
		}
		tuples = append(tuples, tpl)
	}
	return tuples
}

func GenResourceIds(resourceName string, number int) []string {
	resourceIDs := make([]string, 0, number)
	for i := 0; i < number; i++ {
		resourceIDs = append(resourceIDs, fmt.Sprintf("%s-%d", resourceName, i))
	}
	return resourceIDs
}

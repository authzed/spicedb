package tuple

import (
	"sort"
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/stretchr/testify/require"
)

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func TestONRByTypeSet(t *testing.T) {
	assertHasObjectIds := func(s *ONRByTypeSet, rr *core.RelationReference, expected []string) {
		wasFound := false
		s.ForEachType(func(foundRR *core.RelationReference, objectIds []string) {
			if rr.EqualVT(foundRR) {
				sort.Strings(objectIds)
				require.Equal(t, expected, objectIds)
				wasFound = true
			}
		})
		require.True(t, wasFound)
	}

	set := NewONRByTypeSet()
	require.True(t, set.IsEmpty())

	// Add some ONRs.
	set.Add(ParseONR("document:foo#viewer"))
	set.Add(ParseONR("document:bar#viewer"))
	set.Add(ParseONR("team:something#member"))
	set.Add(ParseONR("team:other#member"))
	set.Add(ParseONR("team:other#manager"))
	require.False(t, set.IsEmpty())

	// Run for each type over the set
	assertHasObjectIds(set, RR("document", "viewer"), []string{"bar", "foo"})
	assertHasObjectIds(set, RR("team", "member"), []string{"other", "something"})
	assertHasObjectIds(set, RR("team", "manager"), []string{"other"})

	// Map
	mapped, err := set.Map(func(rr *core.RelationReference) (*core.RelationReference, error) {
		if rr.Namespace == "document" {
			return RR("doc", rr.Relation), nil
		}

		return rr, nil
	})
	require.NoError(t, err)

	assertHasObjectIds(mapped, RR("doc", "viewer"), []string{"bar", "foo"})
	assertHasObjectIds(mapped, RR("team", "member"), []string{"other", "something"})
	assertHasObjectIds(mapped, RR("team", "manager"), []string{"other"})
}

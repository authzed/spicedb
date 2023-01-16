package datasets

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func TestSubjectByTypeSet(t *testing.T) {
	assertHasObjectIds := func(s *SubjectByTypeSet, rr *core.RelationReference, expected []string) {
		wasFound := false
		s.ForEachType(func(foundRR *core.RelationReference, subjects SubjectSet) {
			objectIds := make([]string, 0, len(subjects.AsSlice()))
			for _, subject := range subjects.AsSlice() {
				require.Empty(t, subject.GetExcludedSubjects())
				objectIds = append(objectIds, subject.SubjectId)
			}

			if rr.Namespace == foundRR.Namespace && rr.Relation == foundRR.Relation {
				sort.Strings(objectIds)
				require.Equal(t, expected, objectIds)
				wasFound = true
			}
		})
		require.True(t, wasFound)
	}

	set := NewSubjectByTypeSet()
	require.True(t, set.IsEmpty())

	// Add some concrete subjects.
	err := set.AddConcreteSubject(tuple.ParseONR("document:foo#viewer"))
	require.NoError(t, err)

	err = set.AddConcreteSubject(tuple.ParseONR("document:bar#viewer"))
	require.NoError(t, err)

	err = set.AddConcreteSubject(tuple.ParseONR("team:something#member"))
	require.NoError(t, err)

	err = set.AddConcreteSubject(tuple.ParseONR("team:other#member"))
	require.NoError(t, err)

	err = set.AddConcreteSubject(tuple.ParseONR("team:other#manager"))
	require.NoError(t, err)

	// Add a caveated subject.
	err = set.AddSubjectOf(tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "first"))
	require.NoError(t, err)

	require.False(t, set.IsEmpty())

	// Run for each type over the set
	assertHasObjectIds(set, RR("document", "viewer"), []string{"bar", "foo"})
	assertHasObjectIds(set, RR("team", "member"), []string{"other", "something"})
	assertHasObjectIds(set, RR("team", "manager"), []string{"other"})
	assertHasObjectIds(set, RR("user", "..."), []string{"tom"})

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
	assertHasObjectIds(mapped, RR("user", "..."), []string{"tom"})
}

func TestSubjectSetByTypeWithCaveats(t *testing.T) {
	set := NewSubjectByTypeSet()
	require.True(t, set.IsEmpty())

	err := set.AddSubjectOf(tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "first"))
	require.NoError(t, err)

	ss, ok := set.SubjectSetForType(&core.RelationReference{
		Namespace: "user",
		Relation:  "...",
	})
	require.True(t, ok)

	tom, ok := ss.Get("tom")
	require.True(t, ok)

	require.Equal(t,
		caveatexpr("first"),
		tom.GetCaveatExpression(),
	)
}

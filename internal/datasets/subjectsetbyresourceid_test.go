package datasets

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSubjectSetByResourceIDBasicOperations(t *testing.T) {
	ssr := NewSubjectSetByResourceID()
	require.True(t, ssr.IsEmpty())

	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.False(t, ssr.IsEmpty())

	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))
	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	expected := map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "tom"},
				{SubjectId: "sarah"},
			},
		},
		"seconddoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "fred"},
			},
		},
	}
	asMap := ssr.AsMap()

	sort.Sort(sortFoundSubjects(expected["firstdoc"].FoundSubjects))
	sort.Sort(sortFoundSubjects(expected["seconddoc"].FoundSubjects))

	sort.Sort(sortFoundSubjects(asMap["firstdoc"].FoundSubjects))
	sort.Sort(sortFoundSubjects(asMap["seconddoc"].FoundSubjects))

	require.Equal(t, expected, asMap)
}

func TestSubjectSetByResourceIDUnionWith(t *testing.T) {
	ssr := NewSubjectSetByResourceID()
	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))
	require.NoError(t, ssr.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	err := ssr.UnionWith(map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "tom"},
				{SubjectId: "micah"},
			},
		},
		"thirddoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "george"},
			},
		},
	})
	require.NoError(t, err)

	found := ssr.AsMap()
	sort.Sort(sortFoundSubjects(found["firstdoc"].FoundSubjects))

	require.Equal(t, map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "micah"},
				{SubjectId: "sarah"},
				{SubjectId: "tom"},
			},
		},
		"seconddoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "fred"},
			},
		},
		"thirddoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "george"},
			},
		},
	}, found)
}

type sortFoundSubjects []*v1.FoundSubject

func (a sortFoundSubjects) Len() int      { return len(a) }
func (a sortFoundSubjects) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortFoundSubjects) Less(i, j int) bool {
	return strings.Compare(a[i].SubjectId, a[j].SubjectId) < 0
}

func TestSubjectSetByResourceIDIntersectionDifference(t *testing.T) {
	first := NewSubjectSetByResourceID()
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	second := NewSubjectSetByResourceID()
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:george#...")))

	err := first.IntersectionDifference(second)
	require.NoError(t, err)

	require.Equal(t, map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "tom"},
			},
		},
	}, first.AsMap())
}

func TestSubjectSetByResourceIDIntersectionDifferenceMissingKey(t *testing.T) {
	first := NewSubjectSetByResourceID()
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	second := NewSubjectSetByResourceID()
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#...")))

	err := first.IntersectionDifference(second)
	require.NoError(t, err)

	require.Equal(t, map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "tom"},
			},
		},
	}, first.AsMap())
}

func TestSubjectSetByResourceIDIntersectionDifferenceItemInSecondSet(t *testing.T) {
	first := NewSubjectSetByResourceID()
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))

	second := NewSubjectSetByResourceID()
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	err := first.IntersectionDifference(second)
	require.NoError(t, err)

	require.Equal(t, map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "tom"},
			},
		},
	}, first.AsMap())
}

func TestSubjectSetByResourceIDSubtractAll(t *testing.T) {
	first := NewSubjectSetByResourceID()
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#...")))

	second := NewSubjectSetByResourceID()
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#...")))
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:george#...")))

	first.SubtractAll(second)

	require.Equal(t, map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "sarah"},
			},
		},
		"seconddoc": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "fred"},
			},
		},
	}, first.AsMap())
}

func TestSubjectSetByResourceIDSubtractAllEmpty(t *testing.T) {
	first := NewSubjectSetByResourceID()
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#...")))
	require.NoError(t, first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:mi#...")))

	second := NewSubjectSetByResourceID()
	require.NoError(t, second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:*#...")))

	first.SubtractAll(second)

	require.Equal(t, map[string]*v1.FoundSubjects{}, first.AsMap())
}

func TestSubjectSetByResourceIDBasicCaveatedOperations(t *testing.T) {
	ssr := NewSubjectSetByResourceID()
	require.True(t, ssr.IsEmpty())

	require.NoError(t, ssr.AddFromRelationship(tuple.MustWithCaveat(tuple.MustParse("document:firstdoc#viewer@user:tom#..."), "first")))
	require.NoError(t, ssr.AddFromRelationship(tuple.MustWithCaveat(tuple.MustParse("document:firstdoc#viewer@user:tom#..."), "second")))

	expected := map[string]*v1.FoundSubjects{
		"firstdoc": {
			FoundSubjects: []*v1.FoundSubject{
				{
					SubjectId:        "tom",
					CaveatExpression: caveatOr(caveatexpr("first"), caveatexpr("second")),
				},
			},
		},
	}
	asMap := ssr.AsMap()

	sort.Sort(sortFoundSubjects(expected["firstdoc"].FoundSubjects))
	sort.Sort(sortFoundSubjects(asMap["firstdoc"].FoundSubjects))

	require.Equal(t, expected, asMap)
}

func TestSubjectSetByResoureIDUnionWithBadMap(t *testing.T) {
	ssr := NewSubjectSetByResourceID()
	require.True(t, ssr.IsEmpty())

	err := ssr.UnionWith(map[string]*v1.FoundSubjects{
		"isnil": nil,
	})
	require.NotNil(t, err)
}

package util

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

	ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	require.False(t, ssr.IsEmpty())

	ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))
	ssr.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

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
	ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	ssr.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))
	ssr.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

	ssr.UnionWith(map[string]*v1.FoundSubjects{
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
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))
	first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

	second := NewSubjectSetByResourceID()
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#..."))
	second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:george#..."))

	first.IntersectionDifference(second)

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
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))
	first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

	second := NewSubjectSetByResourceID()
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#..."))

	first.IntersectionDifference(second)

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
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))

	second := NewSubjectSetByResourceID()
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#..."))
	second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

	first.IntersectionDifference(second)

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
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:sarah#..."))
	first.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:fred#..."))

	second := NewSubjectSetByResourceID()
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:micah#..."))
	second.AddFromRelationship(tuple.MustParse("document:seconddoc#viewer@user:george#..."))

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
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:tom#..."))
	first.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:mi#..."))

	second := NewSubjectSetByResourceID()
	second.AddFromRelationship(tuple.MustParse("document:firstdoc#viewer@user:*#..."))

	first.SubtractAll(second)

	require.Equal(t, map[string]*v1.FoundSubjects{}, first.AsMap())
}

func TestSubjectSetByResourceIDBasicCaveatedOperations(t *testing.T) {
	ssr := NewSubjectSetByResourceID()
	require.True(t, ssr.IsEmpty())

	ssr.AddFromRelationship(tuple.WithCaveat(tuple.MustParse("document:firstdoc#viewer@user:tom#..."), "first"))
	ssr.AddFromRelationship(tuple.WithCaveat(tuple.MustParse("document:firstdoc#viewer@user:tom#..."), "second"))

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

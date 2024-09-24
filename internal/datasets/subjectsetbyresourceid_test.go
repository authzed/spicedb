package datasets

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testutil"
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

	slices.SortFunc(expected["firstdoc"].FoundSubjects, testutil.CmpSubjects)
	slices.SortFunc(expected["seconddoc"].FoundSubjects, testutil.CmpSubjects)

	slices.SortFunc(asMap["firstdoc"].FoundSubjects, testutil.CmpSubjects)
	slices.SortFunc(asMap["seconddoc"].FoundSubjects, testutil.CmpSubjects)

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
	slices.SortFunc(found["firstdoc"].FoundSubjects, testutil.CmpSubjects)

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

	slices.SortFunc(expected["firstdoc"].FoundSubjects, testutil.CmpSubjects)
	slices.SortFunc(asMap["firstdoc"].FoundSubjects, testutil.CmpSubjects)

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

package graph

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestResourcesSubjectsMap2Basic(t *testing.T) {
	rsm := newResourcesSubjectMap2(&core.RelationReference{
		Namespace: "document",
		Relation:  "view",
	})

	require.Equal(t, rsm.resourceType.Namespace, "document")
	require.Equal(t, rsm.resourceType.Relation, "view")
	require.Equal(t, 0, rsm.len())

	rsm.addSubjectIDAsFoundResourceID("first")
	require.Equal(t, 1, rsm.len())

	rsm.addSubjectIDAsFoundResourceID("second")
	require.Equal(t, 2, rsm.len())

	err := rsm.addRelationship(tuple.MustParse("document:third#view@user:tom"), nil)
	require.NoError(t, err)
	require.Equal(t, 3, rsm.len())

	err = rsm.addRelationship(tuple.MustParse("document:fourth#view@user:sarah[somecaveat]"), []string{"somecontext"})
	require.NoError(t, err)
	require.Equal(t, 4, rsm.len())

	locked := rsm.asReadOnly()
	require.False(t, locked.isEmpty())

	directAsResources := locked.asPossibleResources()
	testutil.RequireProtoSlicesEqual(t, []*v1.PossibleResource{
		{
			ResourceId:    "first",
			ForSubjectIds: []string{"first"},
		},
		{
			ResourceId:           "fourth",
			ForSubjectIds:        []string{"sarah"},
			MissingContextParams: []string{"somecontext"},
		},
		{
			ResourceId:    "second",
			ForSubjectIds: []string{"second"},
		},
		{
			ResourceId:    "third",
			ForSubjectIds: []string{"tom"},
		},
	}, directAsResources, nil, "different resources")
}

type relAndMissingContext struct {
	rel            tuple.Relationship
	missingContext []string
}

func TestResourcesSubjectsMap2MapFoundResources(t *testing.T) {
	tcs := []struct {
		name           string
		rels           []relAndMissingContext
		foundResources []*v1.PossibleResource
		expected       []*v1.PossibleResource
	}{
		{
			"empty",
			[]relAndMissingContext{},
			[]*v1.PossibleResource{},
			[]*v1.PossibleResource{},
		},
		{
			"basic no caveats",
			[]relAndMissingContext{
				{rel: tuple.MustParse("group:firstgroup#member@organization:foo")},
				{rel: tuple.MustParse("group:firstgroup#member@organization:bar")},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
				},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"foo", "bar"},
				},
			},
		},
		{
			"caveated all found",
			[]relAndMissingContext{
				{rel: tuple.MustParse("group:firstgroup#member@organization:foo[somecaveat]")},
				{rel: tuple.MustParse("group:firstgroup#member@organization:bar[somecvaeat]")},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
				},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"bar", "foo"},
				},
			},
		},
		{
			"multi-input all caveated",
			[]relAndMissingContext{
				{rel: tuple.MustParse("group:firstgroup#member@organization:bar[anothercaveat]"), missingContext: []string{"somecontext"}},
				{rel: tuple.MustParse("group:secondgroup#member@organization:foo[somecaveat]"), missingContext: []string{"anothercaveat"}},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:           "somedoc",
					ForSubjectIds:        []string{"firstgroup", "secondgroup"},
					MissingContextParams: []string{"somecontext"},
				},
			},
			[]*v1.PossibleResource{
				{
					ResourceId:           "somedoc",
					ForSubjectIds:        []string{"bar", "foo"},
					MissingContextParams: []string{"anothercaveat", "somecontext"},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rsm := newResourcesSubjectMap2(&core.RelationReference{
				Namespace: "group",
				Relation:  "member",
			})

			for _, rel := range tc.rels {
				err := rsm.addRelationship(rel.rel, rel.missingContext)
				require.NoError(t, err)
			}

			expected := make([]*v1.PossibleResource, 0, len(tc.expected))
			for _, expectedResource := range tc.expected {
				cloned := expectedResource.CloneVT()
				sort.Strings(cloned.ForSubjectIds)
				expected = append(expected, cloned)
			}

			locked := rsm.asReadOnly()

			resources := make([]*v1.PossibleResource, 0, len(tc.foundResources))
			for _, resource := range tc.foundResources {
				r, err := locked.mapPossibleResource(resource)
				require.NoError(t, err)
				resources = append(resources, r)
			}

			for _, r := range resources {
				sort.Strings(r.ForSubjectIds)
				sort.Strings(r.MissingContextParams)
			}

			testutil.RequireProtoSlicesEqual(t, expected, resources, sortPossibleByResource, "different resources")
		})
	}
}

func TestFilterSubjectIDsToDispatch(t *testing.T) {
	rsm := newResourcesSubjectMap2(&core.RelationReference{
		Namespace: "group",
		Relation:  "member",
	})

	err := rsm.addRelationship(tuple.MustParse("group:firstgroup#member@organization:foo"), nil)
	require.NoError(t, err)

	err = rsm.addRelationship(tuple.MustParse("group:firstgroup#member@organization:bar"), nil)
	require.NoError(t, err)

	err = rsm.addRelationship(tuple.MustParse("group:secondgroup#member@organization:foo"), nil)
	require.NoError(t, err)

	locked := rsm.asReadOnly()
	onrSet := NewSyncONRSet()
	filtered := locked.filterSubjectIDsToDispatch(onrSet, &core.RelationReference{
		Namespace: "group",
		Relation:  "...",
	})

	sort.Strings(filtered)
	require.Equal(t, []string{"firstgroup", "secondgroup"}, filtered)

	filtered2 := locked.filterSubjectIDsToDispatch(onrSet, &core.RelationReference{
		Namespace: "group",
		Relation:  "...",
	})
	require.Empty(t, filtered2)

	filtered3 := locked.filterSubjectIDsToDispatch(onrSet, &core.RelationReference{
		Namespace: "somethingelse",
		Relation:  "...",
	})

	sort.Strings(filtered3)
	require.Equal(t, []string{"firstgroup", "secondgroup"}, filtered3)
}

func sortPossibleByResource(first *v1.PossibleResource, second *v1.PossibleResource) int {
	return strings.Compare(first.ResourceId, second.ResourceId)
}

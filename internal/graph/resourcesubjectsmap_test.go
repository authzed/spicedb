package graph

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestResourcesSubjectsMapBasic(t *testing.T) {
	rsm := newResourcesSubjectMap(&core.RelationReference{
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

	err := rsm.addRelationship(tuple.MustParse("document:third#view@user:tom"))
	require.NoError(t, err)
	require.Equal(t, 3, rsm.len())

	err = rsm.addRelationship(tuple.MustParse("document:fourth#view@user:sarah[somecaveat]"))
	require.NoError(t, err)
	require.Equal(t, 4, rsm.len())

	filtered := rsm.filterForDispatch(&syncONRSet{})
	require.False(t, filtered.isEmpty())

	directAsResources := filtered.asReachableResources(true)
	testutil.RequireProtoSlicesEqual(t, []*v1.ReachableResource{
		{
			ResourceId:    "first",
			ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
			ForSubjectIds: []string{"first"},
		},
		{
			ResourceId:    "second",
			ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
			ForSubjectIds: []string{"second"},
		},
		{
			ResourceId:    "third",
			ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
			ForSubjectIds: []string{"tom"},
		},
		{
			ResourceId:    "fourth",
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			ForSubjectIds: []string{"sarah"},
		},
	}, directAsResources, sortByResource, "different resources")

	notDirectAsResources := filtered.asReachableResources(false)
	testutil.RequireProtoSlicesEqual(t, []*v1.ReachableResource{
		{
			ResourceId:    "first",
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			ForSubjectIds: []string{"first"},
		},
		{
			ResourceId:    "second",
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			ForSubjectIds: []string{"second"},
		},
		{
			ResourceId:    "third",
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			ForSubjectIds: []string{"tom"},
		},
		{
			ResourceId:    "fourth",
			ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			ForSubjectIds: []string{"sarah"},
		},
	}, notDirectAsResources, sortByResource, "different resources")
}

func TestResourcesSubjectsMapAsReachableResources(t *testing.T) {
	tcs := []struct {
		name     string
		rels     []*core.RelationTuple
		expected []*v1.ReachableResource
	}{
		{
			"empty",
			[]*core.RelationTuple{},
			[]*v1.ReachableResource{},
		},
		{
			"basic",
			[]*core.RelationTuple{
				tuple.MustParse("document:first#view@user:tom"),
				tuple.MustParse("document:second#view@user:sarah"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
					ForSubjectIds: []string{"tom"},
				},
				{
					ResourceId:    "second",
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
					ForSubjectIds: []string{"sarah"},
				},
			},
		},
		{
			"caveated and non-caveated",
			[]*core.RelationTuple{
				tuple.MustParse("document:first#view@user:tom"),
				tuple.MustParse("document:first#view@user:sarah[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
					ForSubjectIds: []string{"tom"},
				},
			},
		},
		{
			"all caveated",
			[]*core.RelationTuple{
				tuple.MustParse("document:first#view@user:tom[anothercaveat]"),
				tuple.MustParse("document:first#view@user:sarah[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
					ForSubjectIds: []string{"tom", "sarah"},
				},
			},
		},
		{
			"full",
			[]*core.RelationTuple{
				tuple.MustParse("document:first#view@user:tom[anothercaveat]"),
				tuple.MustParse("document:first#view@user:sarah[somecaveat]"),
				tuple.MustParse("document:second#view@user:tom"),
				tuple.MustParse("document:second#view@user:sarah[somecaveat]"),
				tuple.MustParse("document:third#view@user:tom"),
				tuple.MustParse("document:third#view@user:sarah"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
					ForSubjectIds: []string{"tom", "sarah"},
				},
				{
					ResourceId:    "second",
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
					ForSubjectIds: []string{"tom"},
				},
				{
					ResourceId:    "third",
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
					ForSubjectIds: []string{"tom", "sarah"},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, isDirectEntrypoint := range []bool{true, false} {
				t.Run(fmt.Sprintf("%v", isDirectEntrypoint), func(t *testing.T) {
					rsm := newResourcesSubjectMap(&core.RelationReference{
						Namespace: "document",
						Relation:  "view",
					})

					for _, rel := range tc.rels {
						err := rsm.addRelationship(rel)
						require.NoError(t, err)
					}

					expected := make([]*v1.ReachableResource, 0, len(tc.expected))
					for _, expectedResource := range tc.expected {
						cloned := expectedResource.CloneVT()
						if !isDirectEntrypoint {
							cloned.ResultStatus = v1.ReachableResource_REQUIRES_CHECK
						}
						expected = append(expected, cloned)
					}

					filtered := rsm.filterForDispatch(&syncONRSet{})
					resources := filtered.asReachableResources(isDirectEntrypoint)
					testutil.RequireProtoSlicesEqual(t, expected, resources, sortByResource, "different resources")
				})
			}
		})
	}
}

func TestResourcesSubjectsMapMapFoundResources(t *testing.T) {
	tcs := []struct {
		name           string
		rels           []*core.RelationTuple
		foundResources []*v1.ReachableResource
		expected       []*v1.ReachableResource
	}{
		{
			"empty",
			[]*core.RelationTuple{},
			[]*v1.ReachableResource{},
			[]*v1.ReachableResource{},
		},
		{
			"basic no caveats",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:foo"),
				tuple.MustParse("group:firstgroup#member@organization:bar"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"foo", "bar"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
		},
		{
			"caveated all found",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:foo[somecaveat]"),
				tuple.MustParse("group:firstgroup#member@organization:bar[somecvaeat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"bar", "foo"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
		},
		{
			"simple short circuit",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:foo[somecaveat]"),
				tuple.MustParse("group:firstgroup#member@organization:bar"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"bar"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
		},
		{
			"check requires on incoming subject",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:foo"),
				tuple.MustParse("group:firstgroup#member@organization:bar"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"firstgroup"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "first",
					ForSubjectIds: []string{"foo", "bar"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
		},
		{
			"multi-input short circuit",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:foo"),
				tuple.MustParse("group:firstgroup#member@organization:bar"),
				tuple.MustParse("group:secondgroup#member@organization:foo[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"firstgroup", "secondgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"foo", "bar"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
		},
		{
			"multi-input short circuit from single input",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:bar"),
				tuple.MustParse("group:secondgroup#member@organization:foo[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"firstgroup", "secondgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"bar"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
		},
		{
			"multi-input short circuit from single input with check required on parent",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:bar"),
				tuple.MustParse("group:secondgroup#member@organization:foo[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"firstgroup", "secondgroup"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"bar"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
		},
		{
			"multi-input all caveated",
			[]*core.RelationTuple{
				tuple.MustParse("group:firstgroup#member@organization:bar[anothercaveat]"),
				tuple.MustParse("group:secondgroup#member@organization:foo[somecaveat]"),
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"firstgroup", "secondgroup"},
					ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				},
			},
			[]*v1.ReachableResource{
				{
					ResourceId:    "somedoc",
					ForSubjectIds: []string{"bar", "foo"},
					ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, isDirectEntrypoint := range []bool{true, false} {
				t.Run(fmt.Sprintf("%v", isDirectEntrypoint), func(t *testing.T) {
					rsm := newResourcesSubjectMap(&core.RelationReference{
						Namespace: "group",
						Relation:  "member",
					})

					for _, rel := range tc.rels {
						err := rsm.addRelationship(rel)
						require.NoError(t, err)
					}

					expected := make([]*v1.ReachableResource, 0, len(tc.expected))
					for _, expectedResource := range tc.expected {
						cloned := expectedResource.CloneVT()
						if !isDirectEntrypoint {
							cloned.ResultStatus = v1.ReachableResource_REQUIRES_CHECK
						}
						sort.Strings(cloned.ForSubjectIds)
						expected = append(expected, cloned)
					}

					filtered := rsm.filterForDispatch(&syncONRSet{})
					resources, err := filtered.mapFoundResources(tc.foundResources, isDirectEntrypoint)
					require.NoError(t, err)

					for _, r := range resources {
						sort.Strings(r.ForSubjectIds)
					}

					testutil.RequireProtoSlicesEqual(t, expected, resources, sortByResource, "different resources")
				})
			}
		})
	}
}

func sortByResource(first *v1.ReachableResource, second *v1.ReachableResource) int {
	return strings.Compare(first.ResourceId, second.ResourceId)
}

func TestSubjectIDsToResourcesMap(t *testing.T) {
	rsm := subjectIDsToResourcesMap(&core.RelationReference{
		Namespace: "document",
		Relation:  "view",
	}, []string{"first", "second", "third"})

	require.Equal(t, 3, rsm.len())
}

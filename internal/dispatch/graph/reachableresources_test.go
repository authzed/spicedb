package graph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type reachableResource struct {
	onr           string
	hasPermission bool
}

func reachable(onr *core.ObjectAndRelation, hasPermission bool) reachableResource {
	return reachableResource{
		tuple.StringONR(onr), hasPermission,
	}
}

func TestSimpleReachableResources(t *testing.T) { // nolint: paralleltest
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		start     *core.RelationReference
		target    *core.ObjectAndRelation
		reachable []reachableResource
	}{
		{
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]reachableResource{},
		},
		{
			RR("document", "view"),
			ONR("user", "eng_lead", "..."),
			[]reachableResource{
				reachable(ONR("document", "masterplan", "view"), true),
			},
		},
		{
			RR("document", "view"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "view"), true),
			},
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]reachableResource{
				reachable(ONR("document", "companyplan", "view"), true),
				reachable(ONR("document", "masterplan", "view"), true),
			},
		},
		{
			RR("document", "view"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "view"), true),
			},
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "view_and_edit"), false),
			},
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "missingrolegal", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "view_and_edit"), false),
			},
		},
		{
			RR("document", "view"),
			ONR("user", "villan", "..."),
			[]reachableResource{},
		},
		{
			RR("document", "view"),
			ONR("user", "owner", "..."),
			[]reachableResource{
				reachable(ONR("document", "companyplan", "view"), true),
				reachable(ONR("document", "masterplan", "view"), true),
			},
		},
		{
			RR("folder", "view"),
			ONR("folder", "company", "view"),
			[]reachableResource{
				reachable(ONR("folder", "strategy", "view"), true),
				reachable(ONR("folder", "company", "view"), true),
			},
		},
		{
			RR("document", "view"),
			ONR("user", "chief_financial_officer", "..."),
			[]reachableResource{
				reachable(ONR("document", "healthplan", "view"), true),
				reachable(ONR("document", "masterplan", "view"), true),
			},
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]reachableResource{
				reachable(ONR("folder", "company", "view"), true),
				reachable(ONR("folder", "strategy", "view"), true),
			},
		},
		{
			RR("document", "view"),
			ONR("document", "masterplan", "view"),
			[]reachableResource{
				reachable(ONR("document", "masterplan", "view"), true),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.Namespace,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			ctx, dispatcher, revision := newLocalDispatcher(t)

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
			err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: tc.start,
				SubjectRelation: &core.RelationReference{
					Namespace: tc.target.Namespace,
					Relation:  tc.target.Relation,
				},
				SubjectIds: []string{tc.target.ObjectId},
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			results := []reachableResource{}
			for _, streamResult := range stream.Results() {
				for _, found := range streamResult.Resources {
					results = append(results, reachableResource{
						tuple.StringONR(&core.ObjectAndRelation{
							Namespace: tc.start.Namespace,
							ObjectId:  found.ResourceId,
							Relation:  tc.start.Relation,
						}),
						found.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
					})
				}
			}
			sort.Sort(byONRAndPermission(results))
			sort.Sort(byONRAndPermission(tc.reachable))

			require.NoError(err)
			require.Equal(tc.reachable, results, "Found: %v, Expected: %v", results, tc.reachable)
		})
	}
}

func TestMaxDepthreachableResources(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	ctx, dispatcher, revision := newLocalDispatcher(t)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
	err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"legal"},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

type byONRAndPermission []reachableResource

func (a byONRAndPermission) Len() int { return len(a) }
func (a byONRAndPermission) Less(i, j int) bool {
	return strings.Compare(
		fmt.Sprintf("%s:%v", a[i].onr, a[i].hasPermission),
		fmt.Sprintf("%s:%v", a[j].onr, a[j].hasPermission),
	) < 0
}
func (a byONRAndPermission) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func BenchmarkReachableResources(b *testing.B) {
	testCases := []struct {
		start  *core.RelationReference
		target *core.ObjectAndRelation
	}{
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
		},
		{
			RR("document", "view"),
			ONR("user", "multiroleguy", "..."),
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
		},
		{
			RR("document", "view"),
			ONR("user", "owner", "..."),
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.Namespace,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		require := require.New(b)
		rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
		require.NoError(err)

		ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

		dispatcher := NewLocalOnlyDispatcher(10)

		ctx := datastoremw.ContextWithHandle(context.Background())
		require.NoError(datastoremw.SetInContext(ctx, ds))

		b.Run(name, func(t *testing.B) {
			for n := 0; n < b.N; n++ {
				stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
				err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
					ResourceRelation: tc.start,
					SubjectRelation: &core.RelationReference{
						Namespace: tc.target.Namespace,
						Relation:  tc.target.Relation,
					},
					SubjectIds: []string{tc.target.ObjectId},
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
					},
				}, stream)
				require.NoError(err)

				results := []*core.ObjectAndRelation{}
				for _, streamResult := range stream.Results() {
					for _, found := range streamResult.Resources {
						results = append(results, &core.ObjectAndRelation{
							Namespace: tc.start.Namespace,
							ObjectId:  found.ResourceId,
							Relation:  tc.start.Relation,
						})
					}
				}
				require.GreaterOrEqual(len(results), 0)
			}
		})
	}
}

func TestCaveatedReachableResources(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		schema        string
		relationships []*core.RelationTuple
		start         *core.RelationReference
		target        *core.ObjectAndRelation
		reachable     []reachableResource
	}{
		{
			"unknown subject",
			`definition user {}
			
			definition document {
				relation viewer: user
				permission view = viewer
			}
			`,
			nil,
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]reachableResource{},
		},
		{
			"no caveats",
			`definition user {}
			
			definition document {
				relation viewer: user
				permission view = viewer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:foo#viewer@user:tom"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{{"document:foo#view", true}},
		},
		{
			"basic caveats",
			`definition user {}
			
			definition document {
				relation viewer: user with testcaveat
				permission view = viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{{"document:foo#view", false}},
		},
		{
			"nested caveats",
			`definition user {}

			definition organization {
				relation viewer: user with testcaveat
			}
			
			definition document {
				relation parent: organization
				permission view = parent->viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#parent@organization:foo"),
				tuple.WithCaveat(tuple.MustParse("organization:foo#viewer@user:tom"), "testcaveat"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{{"document:somedoc#view", false}},
		},
		{
			"arrowed caveats",
			`definition user {}

			definition organization {
				relation viewer: user
			}
			
			definition document {
				relation parent: organization with testcaveat
				permission view = parent->viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("organization:foo#viewer@user:tom"),
				tuple.WithCaveat(tuple.MustParse("document:somedoc#parent@organization:foo"), "testcaveat"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{{"document:somedoc#view", false}},
		},
		{
			"mixture",
			`definition user {}
			
			definition document {
				relation viewer: user | user with testcaveat
				permission view = viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
				tuple.MustParse("document:bar#viewer@user:tom"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#view", false},
				{"document:bar#view", true},
			},
		},
		{
			"intersection example",
			`definition user {}
			
			definition document {
				relation viewer: user | user with testcaveat
				relation editor: user
				permission can_do_things = viewer & editor
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:bar#editor@user:tom"),
				tuple.MustParse("document:bar#viewer@user:tom"),
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
				tuple.MustParse("document:foo#editor@user:tom"),
			},
			RR("document", "can_do_things"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#can_do_things", false},
				{"document:bar#can_do_things", false},
			},
		},
		{
			"intersection reverse example",
			`definition user {}
			
			definition document {
				relation viewer: user | user with testcaveat
				relation editor: user
				permission can_do_things = editor & viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
				tuple.MustParse("document:foo#editor@user:tom"),
			},
			RR("document", "can_do_things"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#can_do_things", false},
			},
		},
		{
			"exclusion example",
			`definition user {}
			
			definition document {
				relation viewer: user | user with testcaveat
				relation banned: user
				permission can_do_things = viewer - banned
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
				tuple.MustParse("document:foo#banned@user:tom"),
			},
			RR("document", "can_do_things"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#can_do_things", false},
			},
		},
		{
			"short circuited arrow example",
			`definition user {}
			
			definition folder {
				relation viewer: user
			}

			definition document {
				relation folder: folder | folder with testcaveat
				permission view = folder->viewer
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#folder@folder:maybe"), "testcaveat"),
				tuple.MustParse("document:foo#folder@folder:always"),
				tuple.MustParse("folder:always#viewer@user:tom"),
				tuple.MustParse("folder:maybe#viewer@user:tom"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#view", true},
			},
		},
		{
			"multiple relation example",
			`definition user {}

			definition document {
				relation viewer: user with testcaveat
				relation editor: user
				permission view = viewer + editor
			}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}
			`,
			[]*core.RelationTuple{
				tuple.WithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
				tuple.MustParse("document:foo#editor@user:tom"),
			},
			RR("document", "view"),
			ONR("user", "tom", "..."),
			[]reachableResource{
				{"document:foo#view", true},
				{"document:foo#view", false},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)

			dispatcher := NewLocalOnlyDispatcher(10)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(err)

			ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

			ctx := datastoremw.ContextWithHandle(context.Background())
			require.NoError(datastoremw.SetInContext(ctx, ds))

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
			err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: tc.start,
				SubjectRelation: &core.RelationReference{
					Namespace: tc.target.Namespace,
					Relation:  tc.target.Relation,
				},
				SubjectIds: []string{tc.target.ObjectId},
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			results := []reachableResource{}
			for _, streamResult := range stream.Results() {
				for _, found := range streamResult.Resources {
					results = append(results, reachableResource{
						tuple.StringONR(&core.ObjectAndRelation{
							Namespace: tc.start.Namespace,
							ObjectId:  found.ResourceId,
							Relation:  tc.start.Relation,
						}),
						found.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
					})
				}
			}
			sort.Sort(byONRAndPermission(results))
			sort.Sort(byONRAndPermission(tc.reachable))

			require.NoError(err)
			require.Equal(tc.reachable, results, "Found: %v, Expected: %v", results, tc.reachable)
		})
	}
}

package graph

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
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

func TestSimpleReachableResources(t *testing.T) {
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
				reachable(ONR("document", "ownerplan", "view"), true),
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
		name := fmt.Sprintf(
			"%s#%s->%s",
			tc.start.Namespace,
			tc.start.Relation,
			tuple.StringONR(tc.target),
		)

		tc := tc
		t.Run(name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			require := require.New(t)

			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

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
				OptionalLimit: 100000000,
			}, stream)

			results := []reachableResource{}
			for _, streamResult := range stream.Results() {
				results = append(results, reachableResource{
					tuple.StringONR(&core.ObjectAndRelation{
						Namespace: tc.start.Namespace,
						ObjectId:  streamResult.Resource.ResourceId,
						Relation:  tc.start.Relation,
					}),
					streamResult.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
				})
			}
			dispatcher.Close()

			slices.SortFunc(results, byONRAndPermission)
			slices.SortFunc(tc.reachable, byONRAndPermission)

			require.NoError(err)
			require.Equal(tc.reachable, results, "Found: %v, Expected: %v", results, tc.reachable)
		})
	}
}

func TestMaxDepthreachableResources(t *testing.T) {
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
		OptionalLimit: 100000000,
	}, stream)

	require.Error(err)
}

func byONRAndPermission(a, b reachableResource) int {
	return cmp.Compare(
		fmt.Sprintf("%s:%v", a.onr, a.hasPermission),
		fmt.Sprintf("%s:%v", b.onr, b.hasPermission),
	)
}

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

		tc := tc
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
					results = append(results, &core.ObjectAndRelation{
						Namespace: tc.start.Namespace,
						ObjectId:  streamResult.Resource.ResourceId,
						Relation:  tc.start.Relation,
					})
				}
				require.GreaterOrEqual(len(results), 0)
			}
		})
	}
}

func TestCaveatedReachableResources(t *testing.T) {
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("organization:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:somedoc#parent@organization:foo"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#folder@folder:maybe"), "testcaveat"),
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
				tuple.MustWithCaveat(tuple.MustParse("document:foo#viewer@user:tom"), "testcaveat"),
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
				results = append(results, reachableResource{
					tuple.StringONR(&core.ObjectAndRelation{
						Namespace: tc.start.Namespace,
						ObjectId:  streamResult.Resource.ResourceId,
						Relation:  tc.start.Relation,
					}),
					streamResult.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
				})
			}
			slices.SortFunc(results, byONRAndPermission)
			slices.SortFunc(tc.reachable, byONRAndPermission)

			require.NoError(err)
			require.Equal(tc.reachable, results, "Found: %v, Expected: %v", results, tc.reachable)
		})
	}
}

func TestReachableResourcesWithConsistencyLimitOf1(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	ctx, dispatcher, revision := newLocalDispatcherWithConcurrencyLimit(t, 1)
	defer dispatcher.Close()

	target := ONR("user", "owner", "...")
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
	err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("folder", "view"),
		SubjectRelation: &core.RelationReference{
			Namespace: target.Namespace,
			Relation:  target.Relation,
		},
		SubjectIds: []string{target.ObjectId},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 100000000,
	}, stream)
	require.NoError(t, err)

	for range stream.Results() {
		// Break early
		break
	}
	dispatcher.Close()
}

func TestReachableResourcesMultipleEntrypointEarlyCancel(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)
	for i := 0; i < 25; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#viewer@user:tom", i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#namespace@namespace:ns%d", i, i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("namespace:ns%d#parent@namespace:ns%d", i+1, i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("organization:org%d#member@user:tom", i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("namespace:ns%d#viewer@user:tom", i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:someresource#org@organization:org%d", i)))
	}

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition organization {
				relation direct_member: user
				relation admin: user
				permission member = direct_member + admin
			}

			definition namespace {
				relation parent: namespace
				relation viewer: user
				relation admin: user
				permission view = viewer + admin + parent->view
			}

			definition resource {
				relation org: organization
				relation admin: user
				relation writer: user
				relation viewer: user
				relation namespace: namespace
				permission view = viewer + writer + admin + namespace->view + org->member
			}
		`,
		testRels,
		require.New(t),
	)
	dispatcher := NewLocalOnlyDispatcher(2)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	// Dispatch reachable resources but terminate the stream early by canceling.
	ctxWithCancel, cancel := context.WithCancel(ctx)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
	err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("resource", "view"),
		SubjectRelation: &core.RelationReference{
			Namespace: "user",
			Relation:  "...",
		},
		SubjectIds: []string{"tom"},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.NoError(t, err)

	for range stream.Results() {
		// Break early
		break
	}

	// Cancel, which should terminate all the existing goroutines in the dispatch.
	cancel()
}

func TestReachableResourcesCursors(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)

	// tom and sarah have access via a single role on each.
	for i := 0; i < 410; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#viewer@user:tom", i)))
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#editor@user:sarah", i)))
	}

	// fred is half on viewer and half on editor.
	for i := 0; i < 410; i++ {
		if i > 200 {
			testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#viewer@user:fred", i)))
		} else {
			testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%d#editor@user:fred", i)))
		}
	}

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	subjects := []string{"tom", "sarah", "fred"}

	for _, subject := range subjects {
		t.Run(subject, func(t *testing.T) {
			dispatcher := NewLocalOnlyDispatcher(2)

			ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
			require.NoError(t, datastoremw.SetInContext(ctx, ds))

			// Dispatch reachable resources but stop reading after the second chunk of results.
			ctxWithCancel, cancel := context.WithCancel(ctx)
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
			err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: RR("resource", "view"),
				SubjectRelation: &core.RelationReference{
					Namespace: "user",
					Relation:  "...",
				},
				SubjectIds: []string{"tom"},
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)
			require.NoError(t, err)
			defer cancel()

			foundResources := mapz.NewSet[string]()
			var cursor *v1.Cursor

			for index, result := range stream.Results() {
				require.True(t, foundResources.Add(result.Resource.ResourceId))

				// Break on the 200th result.
				if index == 199 {
					cursor = result.AfterResponseCursor
					cancel()
					break
				}
			}

			// Ensure we've found a cursor and that we got 200 resources back in the first + second result.
			require.NotNil(t, cursor, "got no cursor and %d results", foundResources.Len())
			require.Equal(t, foundResources.Len(), 200)

			// Call reachable resources again with the cursor, which should continue in the second result
			// and then move forward from there.
			stream2 := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
			err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: RR("resource", "view"),
				SubjectRelation: &core.RelationReference{
					Namespace: "user",
					Relation:  "...",
				},
				SubjectIds: []string{"tom"},
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalCursor: cursor,
			}, stream2)
			require.NoError(t, err)

			count := 0
			for _, result := range stream2.Results() {
				count++
				foundResources.Add(result.Resource.ResourceId)
			}
			require.LessOrEqual(t, count, 310)

			// Ensure *all* results were found.
			for i := 0; i < 410; i++ {
				require.True(t, foundResources.Has("res"+strconv.Itoa(i)), "missing res%d", i)
			}
		})
	}
}

func TestReachableResourcesPaginationWithLimit(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)

	for i := 0; i < 410; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#viewer@user:tom", i)))
	}

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	for _, limit := range []uint32{1, 10, 50, 100, 150, 250, 500} {
		limit := limit
		t.Run(fmt.Sprintf("limit-%d", limit), func(t *testing.T) {
			dispatcher := NewLocalOnlyDispatcher(2)
			var cursor *v1.Cursor
			foundResources := mapz.NewSet[string]()

			for i := 0; i < (410/int(limit))+1; i++ {
				ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
				require.NoError(t, datastoremw.SetInContext(ctx, ds))

				ctxWithCancel, cancel := context.WithCancel(ctx)
				stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
				err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
					ResourceRelation: RR("resource", "view"),
					SubjectRelation: &core.RelationReference{
						Namespace: "user",
						Relation:  "...",
					},
					SubjectIds: []string{"tom"},
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
					},
					OptionalCursor: cursor,
					OptionalLimit:  limit,
				}, stream)
				require.NoError(t, err)
				defer cancel()

				newFound := 0
				existingCursor := cursor
				for _, result := range stream.Results() {
					require.True(t, foundResources.Add(result.Resource.ResourceId), "found duplicate %s for iteration %d with cursor %s", result.Resource.ResourceId, i, existingCursor)
					newFound++

					cursor = result.AfterResponseCursor
				}
				require.LessOrEqual(t, newFound, int(limit))
				if newFound == 0 {
					break
				}
			}

			// Ensure *all* results were found.
			for i := 0; i < 410; i++ {
				resourceID := fmt.Sprintf("res%03d", i)
				require.True(t, foundResources.Has(resourceID), "missing %s", resourceID)
			}
		})
	}
}

func TestReachableResourcesWithQueryError(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)

	for i := 0; i < 410; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#viewer@user:tom", i)))
	}

	baseds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	dispatcher := NewLocalOnlyDispatcher(2)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))

	bds := breakingDatastore{baseds}
	require.NoError(t, datastoremw.SetInContext(ctx, bds))

	ctxWithCancel, cancel := context.WithCancel(ctx)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
	err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("resource", "view"),
		SubjectRelation: &core.RelationReference{
			Namespace: "user",
			Relation:  "...",
		},
		SubjectIds: []string{"tom"},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.Error(t, err)
	defer cancel()
}

type breakingDatastore struct {
	datastore.Datastore
}

func (bds breakingDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegate := bds.Datastore.SnapshotReader(rev)
	return &breakingReader{delegate, 0}
}

type breakingReader struct {
	datastore.Reader
	counter int
}

func (br *breakingReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	br.counter++
	if br.counter > 1 {
		return nil, fmt.Errorf("some sort of error")
	}
	return br.Reader.ReverseQueryRelationships(ctx, subjectsFilter, options...)
}

func TestReachableResourcesOverSchema(t *testing.T) {
	testCases := []struct {
		name                string
		schema              string
		relationships       []*core.RelationTuple
		permission          *core.RelationReference
		subject             *core.ObjectAndRelation
		expectedResourceIDs []string
	}{
		{
			"basic union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genTuples("document", "viewer", "user", "tom", 1510),
				genTuples("document", "editor", "user", "tom", 1510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1510),
		},
		{
			"basic exclusion",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			genTuples("document", "viewer", "user", "tom", 1010),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1010),
		},
		{
			"basic intersection",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer & editor
  			 }`,
			joinTuples(
				genTuples("document", "viewer", "user", "tom", 510),
				genTuples("document", "editor", "user", "tom", 510),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 510),
		},
		{
			"union and exclused union",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				relation banned: user
				permission can_view = viewer - banned
				permission view = can_view + editor
  			 }`,
			joinTuples(
				genTuples("document", "viewer", "user", "tom", 1310),
				genTuplesWithOffset("document", "editor", "user", "tom", 1250, 1200),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"basic caveats",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genTuplesWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{"somecondition": 42}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"excluded items",
			`definition user {}
		
		 	 definition document {
				relation banned: user
				relation viewer: user
				permission view = viewer - banned
  			 }`,
			joinTuples(
				genTuples("document", "viewer", "user", "tom", 1310),
				genTuplesWithOffset("document", "banned", "user", "tom", 1210, 100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 1310),
		},
		{
			"basic caveats with missing field",
			`definition user {}

 			 caveat somecaveat(somecondition int) {
				somecondition == 42
			 }
		
		 	 definition document {
				relation viewer: user with somecaveat
				permission view = viewer
  			 }`,
			genTuplesWithCaveat("document", "viewer", "user", "tom", "somecaveat", map[string]any{}, 0, 2450),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 2450),
		},
		{
			"larger arrow dispatch",
			`definition user {}
	
			 definition folder {
				relation viewer: user
			 }

		 	 definition document {
				relation folder: folder
				permission view = folder->viewer
  			 }`,
			joinTuples(
				genTuples("folder", "viewer", "user", "tom", 150),
				genSubjectTuples("document", "folder", "folder", "...", 150),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 150),
		},
		{
			"big",
			`definition user {}
		
		 	 definition document {
				relation editor: user
				relation viewer: user
				permission view = viewer + editor
  			 }`,
			joinTuples(
				genTuples("document", "viewer", "user", "tom", 15100),
				genTuples("document", "editor", "user", "tom", 15100),
			),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			genResourceIds("document", 15100),
		},
		{
			"chunked arrow with chunked redispatch",
			`definition user {}
		
			 definition folder {
			 	relation viewer: user
				permission view = viewer
			 }

		 	 definition document {
				relation parent: folder
				permission view = parent->view
  			 }`,
			(func() []*core.RelationTuple {
				// Generate 200 folders with tom as a viewer
				tuples := make([]*core.RelationTuple, 0, 200*200)
				for folderID := 0; folderID < 200; folderID++ {
					tpl := &core.RelationTuple{
						ResourceAndRelation: ONR("folder", fmt.Sprintf("folder-%d", folderID), "viewer"),
						Subject:             ONR("user", "tom", "..."),
					}
					tuples = append(tuples, tpl)

					// Generate 200 documents for each folder.
					for documentID := 0; documentID < 200; documentID++ {
						docID := fmt.Sprintf("doc-%d-%d", folderID, documentID)
						tpl := &core.RelationTuple{
							ResourceAndRelation: ONR("document", docID, "parent"),
							Subject:             ONR("folder", fmt.Sprintf("folder-%d", folderID), "..."),
						}
						tuples = append(tuples, tpl)
					}
				}

				return tuples
			})(),
			RR("document", "view"),
			ONR("user", "tom", "..."),
			(func() []string {
				docIDs := make([]string, 0, 200*200)
				for folderID := 0; folderID < 200; folderID++ {
					for documentID := 0; documentID < 200; documentID++ {
						docID := fmt.Sprintf("doc-%d-%d", folderID, documentID)
						docIDs = append(docIDs, docID)
					}
				}
				return docIDs
			})(),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, pageSize := range []int{0, 100, 1000} {
				pageSize := pageSize
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10)

					ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					foundResourceIDs := mapz.NewSet[string]()

					var currentCursor *v1.Cursor
					for {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
						err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
							ResourceRelation: tc.permission,
							SubjectRelation: &core.RelationReference{
								Namespace: tc.subject.Namespace,
								Relation:  tc.subject.Relation,
							},
							SubjectIds: []string{tc.subject.ObjectId},
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalCursor: currentCursor,
							OptionalLimit:  uint32(pageSize),
						}, stream)
						require.NoError(err)

						if pageSize > 0 {
							require.LessOrEqual(len(stream.Results()), pageSize)
						}

						for _, result := range stream.Results() {
							foundResourceIDs.Add(result.Resource.ResourceId)
							currentCursor = result.AfterResponseCursor
						}

						if pageSize == 0 || len(stream.Results()) < pageSize {
							break
						}
					}

					foundResourceIDsSlice := foundResourceIDs.AsSlice()
					sort.Strings(foundResourceIDsSlice)
					sort.Strings(tc.expectedResourceIDs)

					require.Equal(tc.expectedResourceIDs, foundResourceIDsSlice)
				})
			}
		})
	}
}

func TestReachableResourcesWithPreCancelation(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)

	for i := 0; i < 410; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#viewer@user:tom", i)))
	}

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	dispatcher := NewLocalOnlyDispatcher(2)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
	require.NoError(t, datastoremw.SetInContext(ctx, ds))

	ctxWithCancel, cancel := context.WithCancel(ctx)

	// Cancel now
	cancel()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
	err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("resource", "view"),
		SubjectRelation: &core.RelationReference{
			Namespace: "user",
			Relation:  "...",
		},
		SubjectIds: []string{"tom"},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReachableResourcesWithUnexpectedContextCancelation(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)

	for i := 0; i < 410; i++ {
		testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#viewer@user:tom", i)))
	}

	baseds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	dispatcher := NewLocalOnlyDispatcher(2)

	ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))

	cds := cancelingDatastore{baseds}
	require.NoError(t, datastoremw.SetInContext(ctx, cds))

	ctxWithCancel, cancel := context.WithCancel(ctx)
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctxWithCancel)
	err = dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: RR("resource", "view"),
		SubjectRelation: &core.RelationReference{
			Namespace: "user",
			Relation:  "...",
		},
		SubjectIds: []string{"tom"},
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
	}, stream)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	defer cancel()
}

type cancelingDatastore struct {
	datastore.Datastore
}

func (cds cancelingDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegate := cds.Datastore.SnapshotReader(rev)
	return &cancelingReader{delegate, 0}
}

type cancelingReader struct {
	datastore.Reader
	counter int
}

func (cr *cancelingReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	cr.counter++
	if cr.counter > 1 {
		return nil, context.Canceled
	}
	return cr.Reader.ReverseQueryRelationships(ctx, subjectsFilter, options...)
}

func TestReachableResourcesWithCachingInParallelTest(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	testRels := make([]*core.RelationTuple, 0)
	expectedResources := mapz.NewSet[string]()

	for i := 0; i < 410; i++ {
		if i < 250 {
			expectedResources.Add(fmt.Sprintf("res%03d", i))
			testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#viewer@user:tom", i)))
		}

		if i > 200 {
			expectedResources.Add(fmt.Sprintf("res%03d", i))
			testRels = append(testRels, tuple.MustParse(fmt.Sprintf("resource:res%03d#editor@user:tom", i)))
		}
	}

	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(
		rawDS,
		`
			definition user {}

			definition resource {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			}
		`,
		testRels,
		require.New(t),
	)

	dispatcher := NewLocalOnlyDispatcher(50)
	cachingDispatcher, err := caching.NewCachingDispatcher(caching.DispatchTestCache(t), false, "", &keys.CanonicalKeyHandler{})
	require.NoError(t, err)

	cachingDispatcher.SetDelegate(dispatcher)

	g := errgroup.Group{}
	for i := 0; i < 100; i++ {
		g.Go(func() error {
			ctx := log.Logger.WithContext(datastoremw.ContextWithHandle(context.Background()))
			require.NoError(t, datastoremw.SetInContext(ctx, ds))

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
			err = cachingDispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: RR("resource", "view"),
				SubjectRelation: &core.RelationReference{
					Namespace: "user",
					Relation:  "...",
				},
				SubjectIds: []string{"tom"},
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)
			require.NoError(t, err)

			foundResources := mapz.NewSet[string]()
			for _, result := range stream.Results() {
				foundResources.Add(result.Resource.ResourceId)
			}

			expectedResourcesSlice := expectedResources.AsSlice()
			foundResourcesSlice := foundResources.AsSlice()

			sort.Strings(expectedResourcesSlice)
			sort.Strings(foundResourcesSlice)

			require.Equal(t, expectedResourcesSlice, foundResourcesSlice)
			return nil
		})
	}

	require.NoError(t, g.Wait())
}

package graph

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const veryLargeLimit = 1000000000

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func resolvedRes(resourceID string) *v1.ResolvedResource {
	return &v1.ResolvedResource{
		ResourceId:     resourceID,
		Permissionship: v1.ResolvedResource_HAS_PERMISSION,
	}
}

func TestSimpleLookupResources(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		start                 *core.RelationReference
		target                *core.ObjectAndRelation
		expectedResources     []*v1.ResolvedResource
		expectedDispatchCount uint32
		expectedDepthRequired uint32
	}{
		{
			RR("document", "view"),
			ONR("user", "unknown", "..."),
			[]*v1.ResolvedResource{},
			0,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "eng_lead", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			1,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			0,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("companyplan"),
				resolvedRes("masterplan"),
			},
			6,
			3,
		},
		{
			RR("document", "view_and_edit"),
			ONR("user", "multiroleguy", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("specialplan"),
			},
			7,
			3,
		},
		{
			RR("folder", "view"),
			ONR("user", "owner", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("strategy"),
				resolvedRes("company"),
			},
			8,
			4,
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

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)

			require.NoError(err)

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount := processResults(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxCachedDispatchCount)

			// We have to sleep a while to let the cache converge:
			// https://github.com/outcaste-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
			time.Sleep(10 * time.Millisecond)

			// Run again with the cache available.
			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: veryLargeLimit,
			}, stream)
			dispatcher.Close()

			require.NoError(err)

			foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount = processResults(stream)
			require.ElementsMatch(tc.expectedResources, foundResources, "Found: %v, Expected: %v", foundResources, tc.expectedResources)
			require.Equal(tc.expectedDepthRequired, maxDepthRequired, "Depth required mismatch")
			require.LessOrEqual(maxCachedDispatchCount, tc.expectedDispatchCount, "Found dispatch count greater than expected")
			require.Equal(uint32(0), maxDispatchCount)
		})
	}
}

func TestSimpleLookupResourcesWithCursor(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	for _, tc := range []struct {
		subject        string
		expectedFirst  []string
		expectedSecond []string
	}{
		{
			subject:        "owner",
			expectedFirst:  []string{"ownerplan"},
			expectedSecond: []string{"companyplan", "masterplan", "ownerplan"},
		},
		{
			subject:        "chief_financial_officer",
			expectedFirst:  []string{"healthplan"},
			expectedSecond: []string{"healthplan", "masterplan"},
		},
		{
			subject:        "auditor",
			expectedFirst:  []string{"companyplan"},
			expectedSecond: []string{"companyplan", "masterplan"},
		},
	} {
		tc := tc
		t.Run(tc.subject, func(t *testing.T) {
			require := require.New(t)
			ctx, dispatcher, revision := newLocalDispatcher(t)
			defer dispatcher.Close()

			found := mapz.NewSet[string]()

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: RR("document", "view"),
				Subject:        ONR("user", tc.subject, "..."),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalLimit: 1,
			}, stream)

			require.NoError(err)

			require.Equal(1, len(stream.Results()))

			found.Add(stream.Results()[0].ResolvedResource.ResourceId)
			require.Equal(tc.expectedFirst, found.AsSlice())

			cursor := stream.Results()[0].AfterResponseCursor
			require.NotNil(cursor)

			stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
			err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
				ObjectRelation: RR("document", "view"),
				Subject:        ONR("user", tc.subject, "..."),
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				OptionalCursor: cursor,
				OptionalLimit:  2,
			}, stream)

			require.NoError(err)

			for _, result := range stream.Results() {
				found.Add(result.ResolvedResource.ResourceId)
			}

			foundResults := found.AsSlice()
			slices.Sort(foundResults)

			require.Equal(tc.expectedSecond, foundResults)
		})
	}
}

func TestLookupResourcesCursorStability(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	require := require.New(t)
	ctx, dispatcher, revision := newLocalDispatcher(t)
	defer dispatcher.Close()

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)

	// Make the first first request.
	err := dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "owner", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursor := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)

	// Make the same request and ensure the cursor has not changed.
	stream = dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "owner", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		OptionalLimit: 2,
	}, stream)

	require.NoError(err)

	require.NoError(err)
	require.Equal(2, len(stream.Results()))

	cursorAgain := stream.Results()[1].AfterResponseCursor
	require.NotNil(cursor)
	require.Equal(cursor, cursorAgain)
}

func processResults(stream *dispatch.CollectingDispatchStream[*v1.DispatchLookupResourcesResponse]) ([]*v1.ResolvedResource, uint32, uint32, uint32) {
	foundResources := []*v1.ResolvedResource{}
	var maxDepthRequired uint32
	var maxDispatchCount uint32
	var maxCachedDispatchCount uint32
	for _, result := range stream.Results() {
		foundResources = append(foundResources, result.ResolvedResource)
		maxDepthRequired = max(maxDepthRequired, result.Metadata.DepthRequired)
		maxDispatchCount = max(maxDispatchCount, result.Metadata.DispatchCount)
		maxCachedDispatchCount = max(maxCachedDispatchCount, result.Metadata.CachedDispatchCount)
	}
	return foundResources, maxDepthRequired, maxDispatchCount, maxCachedDispatchCount
}

func TestMaxDepthLookup(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	require.NoError(datastoremw.SetInContext(ctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

func joinTuples(first []*core.RelationTuple, second []*core.RelationTuple) []*core.RelationTuple {
	return append(first, second...)
}

func genTuplesWithOffset(resourceName string, relation string, subjectName string, subjectID string, offset int, number int) []*core.RelationTuple {
	return genTuplesWithCaveat(resourceName, relation, subjectName, subjectID, "", nil, offset, number)
}

func genTuples(resourceName string, relation string, subjectName string, subjectID string, number int) []*core.RelationTuple {
	return genTuplesWithOffset(resourceName, relation, subjectName, subjectID, 0, number)
}

func genSubjectTuples(resourceName string, relation string, subjectName string, subjectRelation string, number int) []*core.RelationTuple {
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

func genTuplesWithCaveat(resourceName string, relation string, subjectName string, subjectID string, caveatName string, context map[string]any, offset int, number int) []*core.RelationTuple {
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

func genResourceIds(resourceName string, number int) []string {
	resourceIDs := make([]string, 0, number)
	for i := 0; i < number; i++ {
		resourceIDs = append(resourceIDs, fmt.Sprintf("%s-%d", resourceName, i))
	}
	return resourceIDs
}

func TestLookupResourcesOverSchemaWithCursors(t *testing.T) {
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
			genResourceIds("document", 1210),
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
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, pageSize := range []int{0, 104, 1023} {
				pageSize := pageSize
				t.Run(fmt.Sprintf("ps-%d_", pageSize), func(t *testing.T) {
					require := require.New(t)

					dispatcher := NewLocalOnlyDispatcher(10)

					ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
					require.NoError(err)

					ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)

					ctx := datastoremw.ContextWithHandle(context.Background())
					require.NoError(datastoremw.SetInContext(ctx, ds))

					var currentCursor *v1.Cursor
					foundResourceIDs := mapz.NewSet[string]()
					for {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](ctx)
						err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
							ObjectRelation: tc.permission,
							Subject:        tc.subject,
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
							},
							OptionalLimit:  uint32(pageSize),
							OptionalCursor: currentCursor,
						}, stream)
						require.NoError(err)

						if pageSize > 0 {
							require.LessOrEqual(len(stream.Results()), pageSize)
						}

						for _, result := range stream.Results() {
							foundResourceIDs.Add(result.ResolvedResource.ResourceId)
							currentCursor = result.AfterResponseCursor
						}

						if pageSize == 0 || len(stream.Results()) < pageSize {
							break
						}
					}

					foundResourceIDsSlice := foundResourceIDs.AsSlice()
					slices.Sort(foundResourceIDsSlice)
					slices.Sort(tc.expectedResourceIDs)

					require.Equal(tc.expectedResourceIDs, foundResourceIDsSlice)
				})
			}
		})
	}
}

func TestLookupResourcesImmediateTimeout(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](cctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 10,
		},
	}, stream)

	require.ErrorIs(err, context.DeadlineExceeded)
	require.ErrorContains(err, "context deadline exceeded")
}

func TestLookupResourcesWithError(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(rawDS, require)

	dispatcher := NewLocalOnlyDispatcher(10)
	defer dispatcher.Close()

	ctx := datastoremw.ContextWithHandle(context.Background())
	cctx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	require.NoError(datastoremw.SetInContext(cctx, ds))
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResourcesResponse](cctx)

	err = dispatcher.DispatchLookupResources(&v1.DispatchLookupResourcesRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 1, // Set depth 1 to cause an error within reachable resources
		},
	}, stream)

	require.Error(err)
}

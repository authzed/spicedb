package graph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/util"

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
			2,
		},
		{
			RR("document", "owner"),
			ONR("user", "product_manager", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("masterplan"),
			},
			2,
			1,
		},
		{
			RR("document", "view"),
			ONR("user", "legal", "..."),
			[]*v1.ResolvedResource{
				resolvedRes("companyplan"),
				resolvedRes("masterplan"),
			},
			6,
			4,
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
			5,
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

			found := util.NewSet[string]()

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
			sort.Strings(foundResults)

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

func max(a, b uint32) uint32 {
	if b > a {
		return b
	}
	return a
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

type OrderedResolved []*v1.ResolvedResource

func (a OrderedResolved) Len() int { return len(a) }

func (a OrderedResolved) Less(i, j int) bool {
	return strings.Compare(a[i].ResourceId, a[j].ResourceId) < 0
}

func (a OrderedResolved) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

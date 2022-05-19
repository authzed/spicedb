package graph

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
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
	testCases := []struct {
		start     *core.RelationReference
		target    *core.ObjectAndRelation
		reachable []reachableResource
	}{
		{
			RR("document", "viewer"),
			ONR("user", "unknown", "..."),
			[]reachableResource{},
		},
		{
			RR("document", "viewer"),
			ONR("user", "eng_lead", "..."),
			[]reachableResource{
				reachable(ONR("document", "masterplan", "viewer"), true),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "viewer"), true),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "legal", "..."),
			[]reachableResource{
				reachable(ONR("document", "companyplan", "viewer"), true),
				reachable(ONR("document", "masterplan", "viewer"), true),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "viewer"), true),
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "multiroleguy", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "viewer_and_editor"), false),
			},
		},
		{
			RR("document", "viewer_and_editor"),
			ONR("user", "missingrolegal", "..."),
			[]reachableResource{
				reachable(ONR("document", "specialplan", "viewer_and_editor"), false),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "villan", "..."),
			[]reachableResource{},
		},
		{
			RR("document", "viewer"),
			ONR("user", "owner", "..."),
			[]reachableResource{
				reachable(ONR("document", "companyplan", "viewer"), true),
				reachable(ONR("document", "masterplan", "viewer"), true),
			},
		},
		{
			RR("folder", "viewer"),
			ONR("folder", "company", "viewer"),
			[]reachableResource{
				reachable(ONR("folder", "strategy", "viewer"), true),
				reachable(ONR("folder", "company", "viewer"), true),
			},
		},
		{
			RR("document", "viewer"),
			ONR("user", "chief_financial_officer", "..."),
			[]reachableResource{
				reachable(ONR("document", "healthplan", "viewer"), true),
				reachable(ONR("document", "masterplan", "viewer"), true),
			},
		},
		{
			RR("folder", "viewer"),
			ONR("user", "owner", "..."),
			[]reachableResource{
				reachable(ONR("folder", "company", "viewer"), true),
				reachable(ONR("folder", "strategy", "viewer"), true),
			},
		},
		{
			RR("document", "viewer"),
			ONR("document", "masterplan", "viewer"),
			[]reachableResource{
				reachable(ONR("document", "masterplan", "viewer"), true),
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

		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			ctx, dispatcher, revision := newLocalDispatcher(require)

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
			err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ObjectRelation: tc.start,
				Subject:        tc.target,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
			}, stream)

			results := []reachableResource{}
			for _, streamResult := range stream.Results() {
				results = append(results, reachableResource{
					tuple.StringONR(streamResult.Resource.Resource),
					streamResult.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
				})
			}
			sort.Sort(byONR(results))
			sort.Sort(byONR(tc.reachable))

			require.NoError(err)
			require.Equal(tc.reachable, results, "Found: %v, Expected: %v", results, tc.reachable)
		})
	}
}

func TestMaxDepthreachableResources(t *testing.T) {
	require := require.New(t)

	ctx, dispatcher, revision := newLocalDispatcher(require)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchReachableResourcesResponse](ctx)
	err := dispatcher.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ObjectRelation: RR("document", "viewer"),
		Subject:        ONR("user", "legal", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 0,
		},
	}, stream)

	require.Error(err)
}

type byONR []reachableResource

func (a byONR) Len() int { return len(a) }
func (a byONR) Less(i, j int) bool {
	return strings.Compare(a[i].onr, a[j].onr) < 0
}
func (a byONR) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

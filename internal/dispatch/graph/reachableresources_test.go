package graph

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
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
				for _, rid := range streamResult.Resource.ResourceIds {
					results = append(results, reachableResource{
						tuple.StringONR(&core.ObjectAndRelation{
							Namespace: tc.start.Namespace,
							ObjectId:  rid,
							Relation:  tc.start.Relation,
						}),
						streamResult.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION,
					})
				}
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

type byONR []reachableResource

func (a byONR) Len() int { return len(a) }
func (a byONR) Less(i, j int) bool {
	return strings.Compare(a[i].onr, a[j].onr) < 0
}
func (a byONR) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

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
					for _, rid := range streamResult.Resource.ResourceIds {
						results = append(results, &core.ObjectAndRelation{
							Namespace: tc.start.Namespace,
							ObjectId:  rid,
							Relation:  tc.start.Relation,
						})
					}
				}
				require.GreaterOrEqual(len(results), 0)
			}
		})
	}
}

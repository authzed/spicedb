package caching

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type finiteBatchDelegate struct {
	dispatch.Dispatcher
	calls   atomic.Uint64
	paths   []*v1.ResultPath
	failure error
}

// DispatchQueryPlan produces a finite batch response or the specified failure.
func (d *finiteBatchDelegate) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	d.calls.Add(1)

	if d.failure != nil {
		return d.failure
	}

	for _, path := range d.paths {
		if err := stream.Publish(&v1.DispatchQueryPlanResponse{Paths: []*v1.ResultPath{path.CloneVT()}}); err != nil {
			return err
		}

		if req.ResultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
			break
		}
	}

	return nil
}

// Close releases the delegate's test-owned state; it owns no external resources.
func (*finiteBatchDelegate) Close() error { return nil }

func TestFinitePlanBatchesCacheCompleteResponses(t *testing.T) {
	for _, operation := range []v1.PlanOperation{
		v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
		v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS,
	} {
		for _, denial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/denial=%v", operation, denial), func(t *testing.T) {
				backing := DispatchTestCache(t)
				cd, err := NewCachingDispatcher(backing, dispatch.MetricsOptions{PrometheusRegistry: prometheus.NewRegistry()}, nil)
				require.NoError(t, err)
				delegate := &finiteBatchDelegate{}

				if !denial {
					delegate.paths = []*v1.ResultPath{
						{
							ResourceType:    "document",
							ResourceId:      "d0",
							Relation:        "view",
							SubjectType:     "user",
							SubjectId:       "u",
							SubjectRelation: "...",
						},
						{
							ResourceType:    "document",
							ResourceId:      "d1",
							Relation:        "view",
							SubjectType:     "user",
							SubjectId:       "u",
							SubjectRelation: "...",
						},
					}
				}

				cd.SetDelegate(delegate)
				t.Cleanup(func() { require.NoError(t, cd.Close()) })
				req := &v1.DispatchQueryPlanRequest{
					Operation: operation,
					Resource:  &core.ObjectAndRelation{},
					Subject:   tuple.CoreONR("user", "u", "..."),
					Many: []*core.ObjectAndRelation{
						{Namespace: "document", ObjectId: "d0"},
						{Namespace: "document", ObjectId: "d1"},
					},
					PlanContext: &v1.PlanContext{Revision: "1", MaxRecursionDepth: 20, InProgressKeys: []string{"document#view"}},
				}

				if operation == v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS {
					req.Resource = tuple.CoreONR("document", "d", "view")
					req.Subject = &core.ObjectAndRelation{}
					req.Many = []*core.ObjectAndRelation{tuple.CoreONR("user", "u0", "..."), tuple.CoreONR("user", "u1", "...")}

					for index, path := range delegate.paths {
						path.ResourceId = "d"
						path.SubjectId = fmt.Sprintf("u%d", index)
					}
				}

				first := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
				require.NoError(t, cd.DispatchQueryPlan(req, first))
				key, err := cd.keyHandler.PlanCheckCacheKey(t.Context(), req)
				require.NoError(t, err)
				require.Eventually(t, func() bool { _, found := backing.Get(key); return found }, time.Second, time.Millisecond)

				if !denial {
					first.Results()[0].Paths[0].SubjectId = "changed-by-consumer"
				}

				again := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
				require.NoError(t, cd.DispatchQueryPlan(req.CloneVT(), again))
				require.EqualValues(t, 1, delegate.calls.Load(), "a complete batch must be reused")
				require.Len(t, again.Results(), len(delegate.paths))

				if !denial {
					require.Equal(t, delegate.paths[0], again.Results()[0].Paths[0])
					require.Equal(t, delegate.paths[1], again.Results()[1].Paths[0])
				}
			})
		}
	}
}

func TestFinitePlanBatchDoesNotCacheFailedOrCanceledResponses(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			backing := DispatchTestCache(t)
			cd, err := NewCachingDispatcher(backing, dispatch.MetricsOptions{PrometheusRegistry: prometheus.NewRegistry()}, nil)
			require.NoError(t, err)
			delegate := &finiteBatchDelegate{}

			if !canceled {
				delegate.failure = errors.New("batch failed")
			}

			cd.SetDelegate(delegate)
			t.Cleanup(func() { require.NoError(t, cd.Close()) })
			req := &v1.DispatchQueryPlanRequest{
				Operation:   v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
				Resource:    &core.ObjectAndRelation{},
				Subject:     tuple.CoreONR("user", "u", "..."),
				PlanContext: &v1.PlanContext{Revision: "1", InProgressKeys: []string{"document#view"}},
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			if canceled {
				cancel()
			}

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](ctx)
			require.Error(t, cd.DispatchQueryPlan(req, stream))
			key, err := cd.keyHandler.PlanCheckCacheKey(t.Context(), req)
			require.NoError(t, err)
			_, found := backing.Get(key)
			require.False(t, found, "failed or canceled empty output is not a cached denial")
		})
	}
}

func TestExistentialBatchCacheCannotAnswerExhaustiveCheck(t *testing.T) {
	backing := DispatchTestCache(t)
	cd, err := NewCachingDispatcher(backing, dispatch.MetricsOptions{PrometheusRegistry: prometheus.NewRegistry()}, nil)
	require.NoError(t, err)
	delegate := &finiteBatchDelegate{paths: []*v1.ResultPath{
		{ResourceType: "document", ResourceId: "d0", SubjectType: "user", SubjectId: "u", SubjectRelation: "..."},
		{ResourceType: "document", ResourceId: "d1", SubjectType: "user", SubjectId: "u", SubjectRelation: "..."},
	}}
	cd.SetDelegate(delegate)
	t.Cleanup(func() { require.NoError(t, cd.Close()) })
	req := &v1.DispatchQueryPlanRequest{
		Operation:      v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
		Subject:        tuple.CoreONR("user", "u", "..."),
		Many:           []*core.ObjectAndRelation{{Namespace: "document", ObjectId: "d0"}, {Namespace: "document", ObjectId: "d1"}},
		ResultsSetting: v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
		PlanContext:    &v1.PlanContext{Revision: "1", MaxRecursionDepth: 20, InProgressKeys: []string{"document#view"}},
	}
	partial := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
	require.NoError(t, cd.DispatchQueryPlan(req, partial))
	require.Len(t, partial.Results(), 1)
	key, err := cd.keyHandler.PlanCheckCacheKey(t.Context(), req)
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, found := backing.Get(key); return found }, time.Second, time.Millisecond)

	// .all must visit every target even after an existential request has cached
	// an early grant for exactly the same target vector.
	req.ResultsSetting = v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	complete := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
	require.NoError(t, cd.DispatchQueryPlan(req, complete))
	require.Len(t, complete.Results(), 2)
	require.EqualValues(t, 2, delegate.calls.Load())
}

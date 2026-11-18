package singleflight

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type heldBatchDelegate struct {
	dispatch.Dispatcher
	calls   atomic.Uint64
	started chan context.Context
	release <-chan struct{}
}

// DispatchQueryPlan holds a finite response until release or execution cancellation.
func (d *heldBatchDelegate) DispatchQueryPlan(_ *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	d.calls.Add(1)
	d.started <- stream.Context()

	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-d.release:
		return stream.Publish(&v1.DispatchQueryPlanResponse{
			Paths: []*v1.ResultPath{
				{ResourceType: "document", ResourceId: "d", Relation: "view", SubjectType: "user", SubjectId: "u", SubjectRelation: "..."},
			},
		})
	}
}

func TestFinitePlanBatchSingleflightKeepsLivePeerAfterLeaderCancellation(t *testing.T) {
	for _, operation := range []v1.PlanOperation{
		v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
		v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS,
	} {
		t.Run(operation.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer close(release)
				delegate := &heldBatchDelegate{started: make(chan context.Context, 2), release: release}
				d := New(delegate, &keys.DirectKeyHandler{})
				req := &v1.DispatchQueryPlanRequest{
					Operation:   operation,
					Resource:    &core.ObjectAndRelation{},
					Subject:     tuple.CoreONR("user", "u", "..."),
					Many:        []*core.ObjectAndRelation{{Namespace: "document", ObjectId: "d"}},
					PlanContext: &v1.PlanContext{Revision: "1", MaxRecursionDepth: 20, InProgressKeys: []string{"document#view"}},
				}

				if operation == v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS {
					req.Resource = tuple.CoreONR("document", "d", "view")
					req.Subject = &core.ObjectAndRelation{}
					req.Many = []*core.ObjectAndRelation{tuple.CoreONR("user", "u", "...")}
				}

				leader, cancel := context.WithCancel(t.Context())
				defer cancel()
				leaderDone := make(chan error, 1)
				go func() {
					stream := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](leader)
					leaderDone <- d.DispatchQueryPlan(req.CloneVT(), stream)
				}()
				inner := <-delegate.started
				peer := dispatch.NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](t.Context())
				peerDone := make(chan error, 1)
				go func() { peerDone <- d.DispatchQueryPlan(req.CloneVT(), peer) }()
				synctest.Wait()
				require.EqualValues(t, 1, delegate.calls.Load(), "identical finite batches must share execution")
				cancel()
				require.ErrorIs(t, <-leaderDone, context.Canceled)
				require.NoError(t, inner.Err(), "a live peer keeps shared execution alive")
				release <- struct{}{}
				require.NoError(t, <-peerDone)
				require.Len(t, peer.Results(), 1)
				require.Equal(t, "d", peer.Results()[0].Paths[0].ResourceId)
			})
		})
	}
}

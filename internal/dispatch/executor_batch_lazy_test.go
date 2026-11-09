package dispatch

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/execscope"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

// TestCheckManyEarlyGrantSkipsQueuedPreparation guards the cost of unvisited chunks.
func TestCheckManyEarlyGrantSkipsQueuedPreparation(t *testing.T) {
	for _, resourcesVary := range []bool{true, false} {
		t.Run(fmt.Sprint(resourcesVary), func(t *testing.T) {
			resources := make([]query.Object, 10000)
			subjects := make([]query.ObjectAndRelation, len(resources))

			for i := range resources {
				resources[i] = query.NewObject("document", fmt.Sprint(i))
				subjects[i] = query.NewObject("user", fmt.Sprint(i)).WithEllipses()
			}

			// Width one makes the first chunk's grant decisive before any queued
			// task starts. Only its protocol targets should be constructed.
			receiver := &chunkingDispatcher{respond: func(req *v1.DispatchQueryPlanRequest) []*v1.ResultPath {
				resource, subject := req.Resource, req.Subject

				if resourcesVary {
					resource = req.Many[0]
				} else {
					subject = req.Many[0]
				}

				return []*v1.ResultPath{{
					ResourceType:    resource.Namespace,
					ResourceId:      resource.ObjectId,
					Relation:        "viewer",
					SubjectType:     subject.Namespace,
					SubjectId:       subject.ObjectId,
					SubjectRelation: subject.Relation,
				}}
			}}
			executor := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev"}, 100)
			iterator := NewDispatchIterator(query.NewAliasIterator("document", "viewer", query.NewFixedIterator()))
			scope := execscope.New()
			ctx := query.NewLocalContext(scope.Context(t.Context()), query.WithCheckConcurrency(1))
			var paths []*query.Path
			var err error
			allocations := testing.AllocsPerRun(10, func() {
				receiver.planCalls = nil

				if resourcesVary {
					paths, err = executor.CheckManyResources(ctx, iterator, resources, subjects[0], query.AllowSingleResult)
				} else {
					paths, err = executor.CheckManySubjects(ctx, iterator, resources[0], subjects, query.AllowSingleResult)
				}

				scope.Wait()
			})
			require.NoError(t, err)
			require.Len(t, paths, len(resources))
			require.NotNil(t, paths[0])
			require.Len(t, receiver.planCalls, 1)
			require.LessOrEqual(t, allocations, float64(500), "queued chunks must not allocate protocol requests")
		})
	}
}

// TestCheckManyLazyRequestsOwnCallerState checks that later chunks use the original snapshot.
func TestCheckManyLazyRequestsOwnCallerState(t *testing.T) {
	for _, resourcesVary := range []bool{true, false} {
		t.Run(fmt.Sprint(resourcesVary), func(t *testing.T) {
			resources := []query.Object{query.NewObject("document", "a"), query.NewObject("document", "b")}
			subjects := []query.ObjectAndRelation{query.NewObject("user", "a").WithEllipses(), query.NewObject("user", "b").WithEllipses()}
			parent := &v1.PlanContext{
				Revision:              "rev",
				SchemaHash:            []byte("original"),
				TargetSubjectRelation: &core.RelationReference{Namespace: "user", Relation: "..."},
			}
			receiver := &chunkingDispatcher{}
			receiver.respond = func(_ *v1.DispatchQueryPlanRequest) []*v1.ResultPath {
				// Simulate caller-owned state changing after the initial dispatch. The
				// next chunk must use owned inputs and a deep snapshot of nested metadata.
				resources[1].ObjectID = "changed"
				subjects[1].ObjectID = "changed"
				parent.SchemaHash[0] = 'X'
				parent.TargetSubjectRelation.Namespace = "changed"
				return nil
			}
			executor := NewDispatchExecutor(receiver, parent, 1)
			iterator := NewDispatchIterator(query.NewAliasIterator("document", "viewer", query.NewFixedIterator()))
			ctx := query.NewLocalContext(t.Context(), query.WithCheckConcurrency(1))
			var err error

			if resourcesVary {
				_, err = executor.CheckManyResources(ctx, iterator, resources, subjects[0])
			} else {
				_, err = executor.CheckManySubjects(ctx, iterator, resources[0], subjects)
			}

			require.NoError(t, err)
			require.Len(t, receiver.planCalls, 2)
			require.Equal(t, "b", receiver.planCalls[1].Many[0].ObjectId)

			for _, request := range receiver.planCalls {
				require.Equal(t, []byte("original"), request.PlanContext.SchemaHash)
				require.Equal(t, "user", request.PlanContext.TargetSubjectRelation.Namespace)
			}
		})
	}
}

// errorFirstBatchDispatcher lets a failed chunk precede a sibling's late grant.
type errorFirstBatchDispatcher struct {
	*blockingBatchDispatcher
	failure        error
	releaseFailure chan struct{}
}

// DispatchQueryPlan blocks the first target until the test releases its error.
func (d *errorFirstBatchDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream PlanStream) error {
	if req.Many[0].ObjectId == "bad" {
		d.started <- req
		<-d.releaseFailure
		return d.failure
	}

	return d.blockingBatchDispatcher.DispatchQueryPlan(req, stream)
}

// TestDispatchBatchFailurePrecedesLateGrant preserves first-observed outcome precedence.
func TestDispatchBatchFailurePrecedesLateGrant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("relationship read failed")
		finishCancellation := make(chan struct{})
		receiver := &errorFirstBatchDispatcher{
			blockingBatchDispatcher: &blockingBatchDispatcher{
				started:            make(chan *v1.DispatchQueryPlanRequest, 3),
				release:            make(chan struct{}),
				finishCancellation: finishCancellation,
				grantAfterCancel:   true,
			},
			failure:        failure,
			releaseFailure: make(chan struct{}),
		}
		scope := execscope.New()
		cleanup := sync.OnceFunc(func() { close(finishCancellation); scope.Wait() })
		defer cleanup()
		executor := NewDispatchExecutor(receiver, &v1.PlanContext{Revision: "rev"}, 1)
		iterator := NewDispatchIterator(query.NewAliasIterator("document", "viewer", query.NewFixedIterator()))
		ctx := query.NewLocalContext(scope.Context(t.Context()), query.WithCheckConcurrency(2))
		done := make(chan error, 1)
		go func() {
			_, err := executor.CheckManyResources(ctx, iterator,
				[]query.Object{
					query.NewObject("document", "bad"),
					query.NewObject("document", "late"),
					query.NewObject("document", "queued"),
				},
				query.NewObject("user", "alice").WithEllipses(), query.AllowSingleResult)
			done <- err
		}()
		<-receiver.started
		<-receiver.started
		close(receiver.releaseFailure)
		synctest.Wait()

		select {
		case err := <-done:
			require.ErrorIs(t, err, failure)
		default:
			t.Fatal("first error waited for a late sibling grant")
		}

		cleanup()
		require.Empty(t, receiver.started, "the queued chunk must remain unvisited")
	})
}

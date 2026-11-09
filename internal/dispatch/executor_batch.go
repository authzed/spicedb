package dispatch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/genutil/execscope"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
)

func (e *DispatchExecutor) dispatchCheckManySubjects(
	ctx *query.Context,
	it query.Iterator,
	resource query.Object,
	subjects []query.ObjectAndRelation,
	modes ...query.CheckResultMode,
) ([]*query.Path, error) {
	if len(subjects) == 0 {
		return nil, nil
	}

	plan, err := serializeCheckManyPlan(it)
	if err != nil {
		return nil, err
	}

	// Snapshot mutable caller state before starting workers. Protocol requests
	// are built only for active chunks, so an early grant avoids queued targets.
	// Canceled workers may outlive this call and must not retain caller-owned input.
	targets := slices.Clone(subjects)
	planContext := planContextForDispatch(e.planContext, plan.key, ctx.TopLevelOperation, ctx.TargetSubjectType).CloneVT()
	mode := dispatchBatchMode(modes)

	return e.dispatchCheckChunks(ctx, len(targets), mode, func(start, end int) *v1.DispatchQueryPlanRequest {
		return e.buildManyRequest(
			planContext,
			v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS,
			plan,
			resource,
			query.ObjectAndRelation{},
			nil,
			targets[start:end],
		)
	})
}

func (e *DispatchExecutor) dispatchCheckManyResources(
	ctx *query.Context,
	it query.Iterator,
	resources []query.Object,
	subject query.ObjectAndRelation,
	modes ...query.CheckResultMode,
) ([]*query.Path, error) {
	if len(resources) == 0 {
		return nil, nil
	}

	plan, err := serializeCheckManyPlan(it)
	if err != nil {
		return nil, err
	}

	// The shared wire plan and context snapshot are immutable. Each active chunk
	// owns its protocol messages; queued chunks need only their input range.
	targets := slices.Clone(resources)
	planContext := planContextForDispatch(e.planContext, plan.key, ctx.TopLevelOperation, ctx.TargetSubjectType).CloneVT()
	mode := dispatchBatchMode(modes)

	return e.dispatchCheckChunks(ctx, len(targets), mode, func(start, end int) *v1.DispatchQueryPlanRequest {
		return e.buildManyRequest(
			planContext,
			v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
			plan,
			query.Object{},
			subject,
			targets[start:end],
			nil,
		)
	})
}

// dispatchBatchMode preserves the exhaustive default used by CheckMany callers.
// Existential completion must be explicitly requested by the enclosing reducer.
func dispatchBatchMode(modes []query.CheckResultMode) query.CheckResultMode {
	if len(modes) == 0 {
		return query.AllCheckResults
	}

	return modes[0]
}

// dispatchCheckChunks applies one bounded runner to either batch dimension.
// Each worker owns its stream and output range; input order is independent
// of completion order. An unconditional grant may return before canceled siblings
// finish. Their workers and the completion barrier remain registered with the
// execution scope, and their output storage is never exposed to an early caller.
func (e *DispatchExecutor) dispatchCheckChunks(
	ctx *query.Context,
	total int,
	mode query.CheckResultMode,
	buildRequest func(start, end int) *v1.DispatchQueryPlanRequest,
) ([]*query.Path, error) {
	allowSingle := mode == query.AllowSingleResult
	out := make([]*query.Path, total)
	type decision struct {
		err   error
		start int
		paths []*query.Path
	}
	var decisions chan decision

	if allowSingle {
		decisions = make(chan decision, 1)
	}

	// A successful existential chunk cancels siblings and queued chunks. Its
	// private cause distinguishes this completion from request cancellation.
	batchContext, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	chunkSize := int(e.dispatchChunkSize)
	chunkCount := (total-1)/chunkSize + 1
	runner := taskrunner.NewPreloadedTaskRunner(batchContext, ctx.CheckConcurrencyLimit(), chunkCount)
	// The runner keeps its first error, which can be its own cancellation
	// before another active worker returns a real failure. Keep those failures
	// independently and inspect them only after every worker has joined.
	var errorLock sync.Mutex
	var workerError error

	for start := 0; start < total; start += chunkSize {
		end := min(start+chunkSize, total)
		runner.Add(func(taskContext context.Context) error {
			// Target messages are constructed after this chunk gets capacity.
			// Canceled queued chunks never pay for a request or context clone.
			req := buildRequest(start, end)

			if allowSingle {
				req.ResultsSetting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
			}

			stream := NewCollectingDispatchStream[*v1.DispatchQueryPlanResponse](taskContext)

			if err := e.dispatcher.DispatchQueryPlan(req, stream); err != nil {
				if errors.Is(context.Cause(batchContext), errBatchSatisfied) && isBatchCancellation(err) {
					return nil
				}

				errorLock.Lock()

				if workerError == nil {
					workerError = err
				}

				errorLock.Unlock()

				// In existential mode the first observed failure or grant is
				// decisive, just as in native's union reducer. Later outcomes
				// cannot retract an answer already returned to the caller.
				select {
				case decisions <- decision{err: err}:
				default:
				}

				return err
			}

			copyCheckBatchPaths(out[start:end], req, stream.Results())

			if req.ResultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
				for _, path := range out[start : start+len(req.Many)] {
					if path != nil && path.Caveat == nil {
						// Publish only this worker's completed range. Returning the
						// shared output vector would let late siblings mutate it.
						select {
						case decisions <- decision{start: start, paths: out[start : start+len(req.Many)]}:
						default:
						}

						cancel(errBatchSatisfied)
						break
					}
				}
			}

			return nil
		})
	}

	// Start all registered workers before waiting for a grant. The barrier is
	// tracked too, so benchmark phase changes cannot race with its cleanup.
	finish := execscope.Begin(ctx)
	runner.Start()
	completed := make(chan error, 1)
	go func() {
		defer finish()
		completed <- runner.Wait()
	}()

	var err error
	var found decision
	decided := false

	select {
	case found = <-decisions:
		decided = true
	case err = <-completed:
		// The barrier can finish before this goroutine receives the decision.
		// Prefer the already-published first outcome over later worker errors.
		select {
		case found = <-decisions:
			decided = true
		default:
		}
	}

	if decided {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if found.err != nil {
			return nil, found.err
		}

		result := make([]*query.Path, total)
		copy(result[found.start:], found.paths)
		return result, nil
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if workerError != nil {
		return nil, workerError
	}

	if err != nil && (!isBatchCancellation(err) || !errors.Is(context.Cause(batchContext), errBatchSatisfied)) {
		return nil, err
	}

	return out, nil
}

// copyCheckBatchPaths expands deduplicated receiver paths into the original
// target slots. Each slot owns its path so alias/reducer mutation stays local.
func copyCheckBatchPaths(
	out []*query.Path,
	req *v1.DispatchQueryPlanRequest,
	responses []*v1.DispatchQueryPlanResponse,
) {
	resourcesVary := req.Operation == v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES
	byEndpoint := map[query.ObjectAndRelation]*v1.ResultPath{}

	for _, response := range responses {
		for _, path := range response.Paths {
			key := query.ObjectAndRelation{ObjectType: path.SubjectType, ObjectID: path.SubjectId, Relation: path.SubjectRelation}

			if resourcesVary {
				key = query.ObjectAndRelation{ObjectType: path.ResourceType, ObjectID: path.ResourceId}
			}

			byEndpoint[key] = path
		}
	}

	for i, target := range req.Many {
		key := query.ObjectAndRelation{ObjectType: target.Namespace, ObjectID: target.ObjectId, Relation: target.Relation}

		if resourcesVary {
			key.Relation = ""
		}

		if response := byEndpoint[key]; response != nil {
			out[i] = resultPathToQueryPath(response.CloneVT())
		}
	}
}

// serializedCheckPlan holds the immutable subtree shared by one multi-check's
// chunks. It is request-local, so later calls still serialize their own iterator.
type serializedCheckPlan struct {
	wire []byte
	key  string
}

func serializeCheckManyPlan(it query.Iterator) (serializedCheckPlan, error) {
	// Keep the complete alias or recursive controller together with its body.
	// A receiver must establish the same sentinel bindings as a scalar dispatch.
	target, key, ok := dispatchTarget(it)
	if !ok {
		return serializedCheckPlan{}, fmt.Errorf("DispatchQueryPlan: CheckMany expected DispatchIterator(Alias or Recursive), got %T", it)
	}

	wire, err := serializePlan(target)
	if err != nil {
		return serializedCheckPlan{}, err
	}

	return serializedCheckPlan{wire: wire, key: key}, nil
}

// buildManyRequest constructs a chunk with its own mutable context and targets.
// Exactly one of manyResources and manySubjects is non-nil; the corresponding
// singular endpoint is left zero because the receiver reads Many instead.
func (e *DispatchExecutor) buildManyRequest(
	planContext *v1.PlanContext,
	op v1.PlanOperation,
	plan serializedCheckPlan,
	resource query.Object,
	subject query.ObjectAndRelation,
	manyResources []query.Object,
	manySubjects []query.ObjectAndRelation,
) *v1.DispatchQueryPlanRequest {
	// The current dispatch's canonical key rides on PlanContext.InProgressKeys
	// (appended by planContextForDispatch). Receiver-side cache key computation
	// reads it back from the last entry there; there is no top-level
	// canonical_key field on the request anymore.
	req := &v1.DispatchQueryPlanRequest{
		Operation: op,
		Resource: &core.ObjectAndRelation{
			Namespace: resource.ObjectType,
			ObjectId:  resource.ObjectID,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: subject.ObjectType,
			ObjectId:  subject.ObjectID,
			Relation:  subject.Relation,
		},
		PlanContext: planContext.CloneVT(),
		Plan:        plan.wire,
	}

	// Each target has its own message state and fields, but a single backing
	// array avoids one heap allocation per target in large fanout chunks.
	if len(manyResources) > 0 {
		targets := make([]core.ObjectAndRelation, len(manyResources))
		req.Many = make([]*core.ObjectAndRelation, len(manyResources))

		for i, r := range manyResources {
			targets[i] = core.ObjectAndRelation{
				Namespace: r.ObjectType,
				ObjectId:  r.ObjectID,
			}
			req.Many[i] = &targets[i]
		}
	}

	if len(manySubjects) > 0 {
		targets := make([]core.ObjectAndRelation, len(manySubjects))
		req.Many = make([]*core.ObjectAndRelation, len(manySubjects))

		for i, s := range manySubjects {
			targets[i] = core.ObjectAndRelation{
				Namespace: s.ObjectType,
				ObjectId:  s.ObjectID,
				Relation:  s.Relation,
			}
			req.Many[i] = &targets[i]
		}
	}

	return req
}

// errBatchSatisfied belongs only to an internally completed existential batch.
var errBatchSatisfied = errors.New("check batch has an unconditional grant")

// isBatchCancellation recognizes local and RPC cancellation without classifying
// other transport/datastore failures as successful early completion.
func isBatchCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

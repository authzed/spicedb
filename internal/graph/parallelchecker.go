package graph

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ParallelChecker is a helper for initiating checks over a large set of resources
// for a specific subject, and putting the results concurrently into a set.
type ParallelChecker struct {
	toCheck         chan *v1.DispatchCheckRequest
	enqueuedToCheck *tuple.ONRSet

	c             dispatch.Check
	g             *errgroup.Group
	checkCtx      context.Context
	subject       *core.ObjectAndRelation
	maxConcurrent uint16
	results       *tuple.ONRSet

	dispatchCount       uint32
	cachedDispatchCount uint32
	depthRequired       uint32

	mu sync.Mutex
}

// NewParallelChecker creates a new parallel checker, for a given subject.
func NewParallelChecker(ctx context.Context, c dispatch.Check, subject *core.ObjectAndRelation, maxConcurrent uint16) *ParallelChecker {
	g, checkCtx := errgroup.WithContext(ctx)
	toCheck := make(chan *v1.DispatchCheckRequest)
	return &ParallelChecker{toCheck, tuple.NewONRSet(), c, g, checkCtx, subject, maxConcurrent, tuple.NewONRSet(), 0, 0, 0, sync.Mutex{}}
}

// AddResult adds a result that has been already checked to the set.
func (pc *ParallelChecker) AddResult(resource *core.ObjectAndRelation) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.addResultsUnsafe(resource)
}

// DispatchCount returns the number of dispatches used for checks.
func (pc *ParallelChecker) DispatchCount() uint32 {
	return pc.dispatchCount
}

// CachedDispatchCount returns the number of cached dispatches used for checks.
func (pc *ParallelChecker) CachedDispatchCount() uint32 {
	return pc.cachedDispatchCount
}

// DepthRequired returns the maximum depth required for the checks.
func (pc *ParallelChecker) DepthRequired() uint32 {
	return pc.depthRequired
}

func (pc *ParallelChecker) addResultsUnsafe(resource *core.ObjectAndRelation) {
	pc.results.Add(resource)
}

func (pc *ParallelChecker) updateStatsUnsafe(metadata *v1.ResponseMeta) {
	pc.dispatchCount += metadata.DispatchCount
	pc.cachedDispatchCount += metadata.CachedDispatchCount
	pc.depthRequired = max(pc.depthRequired, metadata.DepthRequired)
}

// QueueCheck queues a resource to be checked.
func (pc *ParallelChecker) QueueCheck(resource *core.ObjectAndRelation, meta *v1.ResolverMeta) {
	queue := func() bool {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.enqueuedToCheck.Add(resource)
	}()
	if !queue {
		return
	}

	pc.toCheck <- &v1.DispatchCheckRequest{
		Metadata:            meta,
		ResourceAndRelation: resource,
		Subject:             pc.subject,
	}
}

// Start starts the parallel checks over those items added via QueueCheck.
func (pc *ParallelChecker) Start() {
	pc.g.Go(func() error {
		sem := semaphore.NewWeighted(int64(pc.maxConcurrent))
		for {
			if err := sem.Acquire(pc.checkCtx, 1); err != nil {
				return err
			}
			req, ok := <-pc.toCheck
			if !ok {
				sem.Release(1)
				break
			}

			pc.g.Go(func() error {
				defer sem.Release(1)
				res, err := pc.c.DispatchCheck(pc.checkCtx, req)
				if err != nil {
					return err
				}

				func() {
					pc.mu.Lock()
					defer pc.mu.Unlock()
					if res.Membership == v1.DispatchCheckResponse_MEMBER {
						pc.addResultsUnsafe(req.ResourceAndRelation)
					}
					pc.updateStatsUnsafe(res.Metadata)
				}()
				return nil
			})
		}
		if err := sem.Acquire(pc.checkCtx, int64(pc.maxConcurrent)); err != nil {
			return err
		}
		return nil
	})
}

// Wait waits for the parallel checker to finish performing all of its
// checks and returns the set of resources that checked, along with whether an
// error occurred. Once called, no new items can be added via QueueCheck.
func (pc *ParallelChecker) Wait() (*tuple.ONRSet, error) {
	close(pc.toCheck)
	if err := pc.g.Wait(); err != nil {
		return nil, err
	}

	return pc.results, nil
}

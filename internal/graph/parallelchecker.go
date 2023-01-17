package graph

import (
	"context"
	"sync"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/util"
)

// parallelChecker is a helper for initiating checks over a large set of resources of a specific
// type, for a specific subject, and putting the results concurrently into a set.
type parallelChecker struct {
	c        dispatch.Check
	g        *errgroup.Group
	checkCtx context.Context
	cancel   func()

	toCheck         chan string
	enqueuedToCheck *util.Set[string]

	lookupRequest ValidatedLookupRequest
	maxConcurrent uint16

	foundResourceIDs map[string]*v1.ResolvedResource

	dispatchCount       uint32
	cachedDispatchCount uint32
	depthRequired       uint32

	mu sync.Mutex
}

// newParallelChecker creates a new parallel checker, for a given subject.
func newParallelChecker(ctx context.Context, cancel func(), c dispatch.Check, req ValidatedLookupRequest, maxConcurrent uint16) *parallelChecker {
	g, checkCtx := errgroup.WithContext(ctx)
	toCheck := make(chan string, maxConcurrent)
	return &parallelChecker{
		checkCtx: checkCtx,
		cancel:   cancel,

		c: c,
		g: g,

		toCheck:         toCheck,
		enqueuedToCheck: util.NewSet[string](),

		lookupRequest: req,
		maxConcurrent: maxConcurrent,

		foundResourceIDs:    map[string]*v1.ResolvedResource{},
		dispatchCount:       0,
		cachedDispatchCount: 0,
		depthRequired:       0,

		mu: sync.Mutex{},
	}
}

// AddResolvedResource adds a resource that has been already checked to the set.
func (pc *parallelChecker) AddResolvedResource(resolvedResource *v1.ResolvedResource) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.addResultsUnsafe(resolvedResource)
}

// DispatchCount returns the number of dispatches used for checks.
func (pc *parallelChecker) DispatchCount() uint32 {
	return pc.dispatchCount
}

// CachedDispatchCount returns the number of cached dispatches used for checks.
func (pc *parallelChecker) CachedDispatchCount() uint32 {
	return pc.cachedDispatchCount
}

// DepthRequired returns the maximum depth required for the checks.
func (pc *parallelChecker) DepthRequired() uint32 {
	return pc.depthRequired
}

func (pc *parallelChecker) addResultsUnsafe(resolvedResource *v1.ResolvedResource) {
	// If the result being added is conditional and we've already found a valid permission, skip.
	if resolvedResource.Permissionship == v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION {
		existing, ok := pc.foundResourceIDs[resolvedResource.ResourceId]
		if ok && existing.Permissionship == v1.ResolvedResource_HAS_PERMISSION {
			return
		}
	}

	pc.foundResourceIDs[resolvedResource.ResourceId] = resolvedResource
	if len(pc.foundResourceIDs) >= int(pc.lookupRequest.Limit) {
		// Cancel any further work
		pc.cancel()
		return
	}
}

func (pc *parallelChecker) updateStatsUnsafe(metadata *v1.ResponseMeta) {
	pc.dispatchCount += metadata.DispatchCount
	pc.cachedDispatchCount += metadata.CachedDispatchCount
	pc.depthRequired = max(pc.depthRequired, metadata.DepthRequired)
}

// QueueToCheck queues a resource ID to be checked.
func (pc *parallelChecker) QueueToCheck(resourceID string) bool {
	queue := func() bool {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		if len(pc.foundResourceIDs) >= int(pc.lookupRequest.Limit) {
			close(pc.toCheck)
			return false
		}

		return pc.enqueuedToCheck.Add(resourceID)
	}()
	if !queue {
		return false
	}

	pc.toCheck <- resourceID
	return true
}

// Start starts the parallel checks over those items added via QueueToCheck.
func (pc *parallelChecker) Start() {
	meta := &v1.ResolverMeta{
		AtRevision:     pc.lookupRequest.Revision.String(),
		DepthRemaining: pc.lookupRequest.Metadata.DepthRemaining,
	}

	pc.g.Go(func() error {
		sem := semaphore.NewWeighted(int64(pc.maxConcurrent))
		for {
			if err := sem.Acquire(pc.checkCtx, 1); err != nil {
				return err
			}

			collected := make([]string, 0, maxDispatchChunkSize)

			for {
				req, ok := <-pc.toCheck
				if !ok {
					break
				}

				collected = append(collected, req)
				if len(collected) == int(maxDispatchChunkSize) {
					break
				}
			}

			if len(collected) == 0 {
				sem.Release(1)
				break
			}

			pc.g.Go(func() error {
				defer sem.Release(1)

				results, resultsMeta, err := computed.ComputeBulkCheck(pc.checkCtx, pc.c,
					computed.CheckParameters{
						ResourceType:  pc.lookupRequest.ObjectRelation,
						Subject:       pc.lookupRequest.Subject,
						CaveatContext: pc.lookupRequest.Context.AsMap(),
						AtRevision:    pc.lookupRequest.Revision,
						MaximumDepth:  meta.DepthRemaining,
						DebugOption:   computed.NoDebugging,
					},
					collected,
				)
				if err != nil {
					return err
				}

				pc.mu.Lock()
				pc.updateStatsUnsafe(resultsMeta)

				for resourceID, result := range results {
					if result.Membership == v1.ResourceCheckResult_MEMBER {
						pc.addResultsUnsafe(&v1.ResolvedResource{
							ResourceId:     resourceID,
							Permissionship: v1.ResolvedResource_HAS_PERMISSION,
						})
					} else if result.Membership == v1.ResourceCheckResult_CAVEATED_MEMBER {
						pc.addResultsUnsafe(&v1.ResolvedResource{
							ResourceId:             resourceID,
							Permissionship:         v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION,
							MissingRequiredContext: result.MissingExprFields,
						})
					}
				}
				pc.mu.Unlock()
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
// error occurred. Once called, no new items can be added via QueueToCheck.
func (pc *parallelChecker) Wait() ([]*v1.ResolvedResource, error) {
	close(pc.toCheck)
	if err := pc.g.Wait(); err != nil {
		return nil, err
	}

	return maps.Values(pc.foundResourceIDs), nil
}

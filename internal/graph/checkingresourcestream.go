package graph

import (
	"context"
	"sort"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/util"
)

// possibleResource is a resource that was returned by reachable resources and, after processing,
// may be returned by the lookup resources stream.
type possibleResource struct {
	// reachableResult is the result for this resource from the reachable resources stream.
	reachableResult *v1.DispatchReachableResourcesResponse

	// lookupResult is the result to be published by LookupResources, if the resource is actually
	// permissioned. Will be nil before processing and nil after processing IF the resource needed
	// to be checked and the check showed the resource was inaccessible.
	lookupResult *v1.DispatchLookupResourcesResponse

	// orderingIndex is the index of the resource result as returned by reachable resources. Used to
	// maintain strict publishing order of results.
	orderingIndex uint64
}

// resourceQueue is a queue for managing of possibleResources through the various states of the stream (queueing, processing and publishing).
type resourceQueue struct {
	ctx  context.Context
	lock sync.Mutex

	// toProcess are those resources (keyed by orderingIndex) that have not yet been processed (checked).
	toProcess map[uint64]possibleResource

	// toPublish are those resources (keyed by orderingIndex) that have been processed and
	// are ready for publishing. Note that resources whose Check calls showed NO_PERMISSION
	// will *also* be in this map, just with lookupResult set to nil. This is done to ensure
	// strict ordering of publishing.
	toPublish map[uint64]possibleResource

	// beingProcessed are those resources (keyed by orderingIndex) that are currently being processed.
	beingProcessed map[uint64]possibleResource
}

// addPossibleResource queues a resource for processing.
func (rq *resourceQueue) addPossibleResource(pr possibleResource) {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	rq.toProcess[pr.orderingIndex] = pr
}

// updateToBePublished marks a resource as ready for publishing.
func (rq *resourceQueue) updateToBePublished(pr possibleResource) {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	rq.toPublish[pr.orderingIndex] = pr
	delete(rq.beingProcessed, pr.orderingIndex)
}

// markResourceCompleted marks that all work has been completed on the given resources.
func (rq *resourceQueue) markResourceCompleted(pr possibleResource) {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	delete(rq.toPublish, pr.orderingIndex)
}

// selectResourcesToProcess marks all toProcess resources as being processing and returns those resources
// for processing.
//
// If alwaysReturn is false, then resources will only be returned if they meet the chunk size, to ensure
// checks occur in larger batches.
func (rq *resourceQueue) selectResourcesToProcess(alwaysReturn bool) []possibleResource {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	toProcess := maps.Values(rq.toProcess)
	if !alwaysReturn && len(toProcess) < int(datastore.FilterMaximumIDCount) {
		return nil
	}

	for _, pr := range toProcess {
		rq.beingProcessed[pr.orderingIndex] = pr
	}

	rq.toProcess = map[uint64]possibleResource{}
	return toProcess
}

// resourcesToPossiblyPublish returns all resources in the toPublish state. This does *not* mark the resources
// as published.
func (rq *resourceQueue) resourcesToPossiblyPublish() []possibleResource {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	return maps.Values(rq.toPublish)
}

// checkingResourceStream is a Stream[*v1.DispatchLookupResourcesResponse] that consumes reachable resource
// responses which are published to it, checks the resource (if necessary), and then publishes the resource
// if reachable. This stream performs Checks for the possibly-inaccessible resources in a parallel fashion
// but maintains the proper publishing order to the parent stream.
//
// Resources in the stream are processed as follows:
//  1. A reachable resource is published to the stream via a call to the Publish method
//  2. The resource is placed into the resourceQueue with an index indicating its publishing order
//  3. A processing worker (up to concurrency limits) grabs the resources to be processed in the queue, checks
//     those resources that need to have CheckPermission invoked, and places all resources processed in the queue
//     into the "ready for publishing" state.
//  4. The *single* publishing worker grabs resources to be published and publishes them in the correct order,
//     skipping any resources whose CheckPermission calls showed them as being inaccessible.
//  5. The waitForPublishing call waits for the stream to have fully processed and published all queued resources
//     before returning.
type checkingResourceStream struct {
	// ctx is the parent context for the LookupResources.
	// NOTE: This will be disconnected from the reachableContext below.
	ctx    context.Context
	cancel func()

	// reachableContext is the context to be returned by this stream for the ReachableResources call, and is
	// disconnected from the overall context.
	reachableContext context.Context

	// concurrencyLimit is the limit on the number on concurrency processing workers.
	concurrencyLimit uint16

	req          ValidatedLookupResourcesRequest
	checker      dispatch.Check
	parentStream dispatch.Stream[*v1.DispatchLookupResourcesResponse]

	// sem is a chan of length `concurrencyLimit` used to ensure the task runner does
	// not exceed the concurrencyLimit with spawned goroutines.
	sem chan token

	// rq is the resourceQueue for managing the state of all resources returned by the reachable resources call.
	rq *resourceQueue

	// reachableResourceAvailable is a channel which indicates to the processing worker(s) that work is available
	// for processing.
	reachableResourceAvailable chan struct{}

	// reachableResourcesCompleted is a channel used to indicate to each processing worker that reachable resources has
	// been completed, and that all further processing work should be done before shutting down.
	reachableResourcesCompleted chan struct{}

	// availableForPublishing is a channel which indicates to the publishing worker that work is available
	// for publishing. If given a false value, then the publishing worker should be terminated, as it indicates
	// there will be no further processed resources.
	availableForPublishing chan bool

	// limits is the limit tracker for the stream. Should *only* be accessed from the publishing goroutine.
	limits *limitTracker

	// orderingIndexToBePublished is the current index to be published. Should *only* be accessed from the publishing
	// goroutine.
	orderingIndexToBePublished uint64

	// reachableResourcesCount is the count of reachable resources received. Should *only* be accessed from queue()
	// and waitForPublishing() (after reachable resources has completed).
	reachableResourcesCount uint64

	// lastResourceCursor is the cursor from the last received reachable resource result. Should *only* be accessed from queue()
	// and waitForPublishing() (after reachable resources has completed).
	lastResourceCursor *v1.Cursor

	errSetter sync.Once
	err       error

	processingWaitGroup sync.WaitGroup
	publishingWaitGroup sync.WaitGroup
}

func newCheckingResourceStream(
	lookupContext context.Context,
	reachableContext context.Context,
	req ValidatedLookupResourcesRequest,
	checker dispatch.Check,
	parentStream dispatch.Stream[*v1.DispatchLookupResourcesResponse],
	limits *limitTracker,
	concurrencyLimit uint16,
) *checkingResourceStream {
	if concurrencyLimit == 0 {
		concurrencyLimit = 1
	}

	cancelCtx, cancel := context.WithCancel(lookupContext)

	crs := &checkingResourceStream{
		ctx:    cancelCtx,
		cancel: cancel,

		reachableContext: reachableContext,
		concurrencyLimit: concurrencyLimit,

		req:          req,
		checker:      checker,
		parentStream: parentStream,
		limits:       limits,

		sem: make(chan token, concurrencyLimit),

		rq: &resourceQueue{
			ctx:            lookupContext,
			toProcess:      map[uint64]possibleResource{},
			beingProcessed: map[uint64]possibleResource{},
			toPublish:      map[uint64]possibleResource{},
		},
		reachableResourceAvailable:  make(chan struct{}, concurrencyLimit),
		reachableResourcesCompleted: make(chan struct{}, concurrencyLimit),
		availableForPublishing:      make(chan bool, concurrencyLimit),

		orderingIndexToBePublished: 0,
		reachableResourcesCount:    0,

		errSetter: sync.Once{},
		err:       nil,

		processingWaitGroup: sync.WaitGroup{},
		publishingWaitGroup: sync.WaitGroup{},
	}

	// Spawn the goroutine that will publish resources to the parent stream in the proper order.
	crs.publishingWaitGroup.Add(1)
	go crs.resourcePublisher()
	return crs
}

// waitForPublishing waits for the publishing goroutine to complete its work, and returns the number
// of published *reachable* resources or the error that occurred during checking or publishing.
func (crs *checkingResourceStream) waitForPublishing() (uint64, *v1.Cursor, error) {
	// Mark that no new items will come in from the reachable resources stream.
	for i := 0; i < int(crs.concurrencyLimit); i++ {
		crs.reachableResourcesCompleted <- struct{}{}
	}

	// Wait for all processing to complete.
	crs.processingWaitGroup.Wait()

	// Mark publishing as ready for final publishing.
	crs.availableForPublishing <- false

	// Wait for any remaining publishing to complete.
	crs.publishingWaitGroup.Wait()

	return crs.reachableResourcesCount, crs.lastResourceCursor, crs.err
}

// resourcePublisher is the goroutine that publishes resources to the parent stream once they've been
// validated by the processing worker(s).
func (crs *checkingResourceStream) resourcePublisher() {
	defer crs.publishingWaitGroup.Done()

	for {
		select {
		case <-crs.ctx.Done():
			return

		case isStillRunning := <-crs.availableForPublishing:
			err := crs.publishResourcesIfPossible()
			if err != nil {
				crs.setError(err)
				return
			}

			if isStillRunning {
				continue
			}
			return
		}
	}
}

// publishResourcesIfPossible publishes the resources that have been processed, in the correct order, if any.
func (crs *checkingResourceStream) publishResourcesIfPossible() error {
	for {
		toPublish := crs.rq.resourcesToPossiblyPublish()
		if len(toPublish) == 0 {
			return nil
		}

		for {
			if len(toPublish) == 0 {
				break
			}

			// Sort to ensure they are in the publishable order.
			sort.Sort(byOrderingIndex(toPublish))

			// Ensure that the next resource to be published is the next in the order. If not,
			// we're still waiting on a resource to be checked.
			current := toPublish[0]
			if current.orderingIndex != crs.orderingIndexToBePublished {
				return nil
			}

			toPublish = toPublish[1:]
			crs.orderingIndexToBePublished++

			// NOTE: lookupResult will be `nil` if the Check for the resource found that the resource is
			// not actually accessible. The entry is kept in `toPublish` to ensure proper ordering is maintained
			// on the parent stream.
			if current.lookupResult != nil {
				ok, done := crs.limits.prepareForPublishing()
				defer done()
				if !ok {
					return nil
				}

				err := crs.parentStream.Publish(current.lookupResult)
				if err != nil {
					crs.setError(err)
					return err
				}
			}

			crs.rq.markResourceCompleted(current)
		}
	}
}

// setError sets an error that occurred.
func (crs *checkingResourceStream) setError(err error) {
	crs.errSetter.Do(func() {
		crs.err = err
		crs.cancel()
	})
}

// process is a processing worker for a reachable resources result, performing checks if necessary.
func (crs *checkingResourceStream) process() {
	defer crs.processingWaitGroup.Done()

	for {
		select {
		case <-crs.ctx.Done():
			return

		case <-crs.reachableResourcesCompleted:
			for {
				ok, err := crs.runProcess(true)
				if err != nil {
					crs.setError(err)
					return
				}
				if !ok {
					break
				}
			}
			return

		case <-crs.reachableResourceAvailable:
			for {
				ok, err := crs.runProcess(false)
				if err != nil {
					crs.setError(err)
					return
				}
				if !ok {
					break
				}
			}
			continue
		}
	}
}

func (crs *checkingResourceStream) runProcess(alwaysProcess bool) (bool, error) {
	// Collect any resources that need to be checked, up to the configured limit, and issue a check.
	// If a resource does not require a check, simply place on the toPublish queue.
	toCheck := util.NewMultiMap[string, possibleResource]()
	toProcess := crs.rq.selectResourcesToProcess(alwaysProcess)
	if len(toProcess) == 0 {
		return false, nil
	}

	for _, current := range toProcess {
		if current.reachableResult.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION {
			current.lookupResult = &v1.DispatchLookupResourcesResponse{
				ResolvedResource: &v1.ResolvedResource{
					ResourceId:     current.reachableResult.Resource.ResourceId,
					Permissionship: v1.ResolvedResource_HAS_PERMISSION,
				},
				Metadata:            addCallToResponseMetadata(current.reachableResult.Metadata),
				AfterResponseCursor: current.reachableResult.AfterResponseCursor,
			}
			crs.rq.updateToBePublished(current)
			continue
		}

		toCheck.Add(current.reachableResult.Resource.ResourceId, current)
	}

	if toCheck.IsEmpty() {
		crs.availableForPublishing <- true
		return true, nil
	}

	// Issue the bulk check over all the resources.
	results, resultsMetas, err := computed.ComputeBulkCheck(
		crs.ctx,
		crs.checker,
		computed.CheckParameters{
			ResourceType:  crs.req.ObjectRelation,
			Subject:       crs.req.Subject,
			CaveatContext: crs.req.Context.AsMap(),
			AtRevision:    crs.req.Revision,
			MaximumDepth:  crs.req.Metadata.DepthRemaining,
			DebugOption:   computed.NoDebugging,
		},
		toCheck.Keys(),
	)
	if err != nil {
		return true, err
	}

	for _, rai := range toCheck.Values() {
		checkResult := results[rai.reachableResult.Resource.ResourceId]

		var permissionship v1.ResolvedResource_Permissionship
		var missingFields []string

		switch {
		case checkResult == nil || checkResult.Membership == v1.ResourceCheckResult_NOT_MEMBER:
			// NOTE: we use `UNKNOWN` here to indicate that the resource was found to be inaccessible,
			// because ResolvedResource does not have such a state.
			permissionship = v1.ResolvedResource_UNKNOWN

		case checkResult != nil && checkResult.Membership == v1.ResourceCheckResult_MEMBER:
			permissionship = v1.ResolvedResource_HAS_PERMISSION

		case checkResult != nil && checkResult.Membership == v1.ResourceCheckResult_CAVEATED_MEMBER:
			permissionship = v1.ResolvedResource_CONDITIONALLY_HAS_PERMISSION
			missingFields = checkResult.MissingExprFields

		default:
			return true, spiceerrors.MustBugf("unknown check result status for reachable resources")
		}

		// Set the lookupResult iff the permissionship was a valid permission.
		var lookupResult *v1.DispatchLookupResourcesResponse
		if permissionship != v1.ResolvedResource_UNKNOWN {
			metadata := combineResponseMetadata(rai.reachableResult.Metadata, resultsMetas[rai.reachableResult.Resource.ResourceId])
			lookupResult = &v1.DispatchLookupResourcesResponse{
				ResolvedResource: &v1.ResolvedResource{
					ResourceId:             rai.reachableResult.Resource.ResourceId,
					Permissionship:         permissionship,
					MissingRequiredContext: missingFields,
				},
				Metadata:            metadata,
				AfterResponseCursor: rai.reachableResult.AfterResponseCursor,
			}
		}

		rai.lookupResult = lookupResult
		crs.rq.updateToBePublished(rai)
	}

	crs.availableForPublishing <- true
	return true, nil
}

// spawnIfAvailable spawns a processing working, if the concurrency limit has not been reached.
func (crs *checkingResourceStream) spawnIfAvailable() {
	// To spawn a processor, write a token to the sem channel. If the checker
	// is already at the concurrency limit, then this chan write will fail,
	// and nothing will be spawned. This also checks if the context has already
	// been canceled, in which case nothing needs to be done.
	select {
	case crs.sem <- token{}:
		crs.processingWaitGroup.Add(1)
		go crs.process()

	case <-crs.ctx.Done():
		return

	default:
		return
	}
}

// queue queues a reachable resources result to be processed by one of the processing worker(s), before publishing.
func (crs *checkingResourceStream) queue(result *v1.DispatchReachableResourcesResponse) bool {
	crs.rq.addPossibleResource(possibleResource{
		reachableResult: result,
		lookupResult:    nil,
		orderingIndex:   crs.reachableResourcesCount,
	})
	crs.reachableResourcesCount++
	crs.lastResourceCursor = result.AfterResponseCursor
	crs.reachableResourceAvailable <- struct{}{}
	return true
}

// Publish implements the Stream interface and is invoked by the ReachableResources call.
func (crs *checkingResourceStream) Publish(result *v1.DispatchReachableResourcesResponse) error {
	// Queue the result to be processed by the parallel workers.
	wasQueued := crs.queue(result)
	if wasQueued {
		crs.spawnIfAvailable()
	}
	return nil
}

// Context implements the Stream interface.
func (crs *checkingResourceStream) Context() context.Context {
	// NOTE: we return the reachable context here, because this is the stream to which the reachable resources
	// call is publishing.
	return crs.reachableContext
}

type byOrderingIndex []possibleResource

func (u byOrderingIndex) Len() int {
	return len(u)
}

func (u byOrderingIndex) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u byOrderingIndex) Less(i, j int) bool {
	return u[i].orderingIndex < u[j].orderingIndex
}

package graph

import (
	"context"
	"sync"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/typesystem"
)

// runDispatchAndChecker runs the dispatch and checker for a lookup resources call, and publishes
// the results to the parent stream. This function is responsible for handling the dispatching
// of the lookup resources call, and then checking the results to filter them.
func runDispatchAndChecker(
	ctx context.Context,
	parentReq ValidatedLookupResources2Request,
	foundResources dispatchableResourcesSubjectMap2,
	ci cursorInformation,
	parentStream dispatch.LookupResources2Stream,
	newSubjectType *core.RelationReference,
	filteredSubjectIDs []string,
	entrypoint typesystem.ReachabilityEntrypoint,
	lrDispatcher dispatch.LookupResources2,
	checkDispatcher dispatch.Check,
	concurrencyLimit uint16,
) error {
	// Only allow max one dispatcher and one checker to run concurrently.
	concurrencyLimit = min(concurrencyLimit, 2)

	rdc := &rdc{
		parentRequest:      parentReq,
		foundResources:     foundResources,
		ci:                 ci,
		parentStream:       parentStream,
		newSubjectType:     newSubjectType,
		filteredSubjectIDs: filteredSubjectIDs,
		entrypoint:         entrypoint,
		lrDispatcher:       lrDispatcher,
		checkDispatcher:    checkDispatcher,
		taskrunner:         taskrunner.NewTaskRunner(ctx, concurrencyLimit),
		lock:               &sync.Mutex{},
	}

	return rdc.runAndWait()
}

type rdc struct {
	parentRequest      ValidatedLookupResources2Request
	foundResources     dispatchableResourcesSubjectMap2
	ci                 cursorInformation
	parentStream       dispatch.LookupResources2Stream
	newSubjectType     *core.RelationReference
	filteredSubjectIDs []string
	entrypoint         typesystem.ReachabilityEntrypoint
	lrDispatcher       dispatch.LookupResources2
	checkDispatcher    dispatch.Check

	taskrunner *taskrunner.TaskRunner

	lock *sync.Mutex
}

func (rdc *rdc) dispatchAndCollect(ctx context.Context, cursor *v1.Cursor) ([]*v1.DispatchLookupResources2Response, error) {
	collectingStream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
	err := rdc.lrDispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: rdc.parentRequest.ResourceRelation,
		SubjectRelation:  rdc.newSubjectType,
		SubjectIds:       rdc.filteredSubjectIDs,
		TerminalSubject:  rdc.parentRequest.TerminalSubject,
		Metadata: &v1.ResolverMeta{
			AtRevision:     rdc.parentRequest.Revision.String(),
			DepthRemaining: rdc.parentRequest.Metadata.DepthRemaining - 1,
		},
		OptionalCursor: cursor,
		OptionalLimit:  uint32(datastore.FilterMaximumIDCount),
	}, collectingStream)
	return collectingStream.Results(), err
}

func (rdc *rdc) runDispatch(ctx context.Context, cursor *v1.Cursor) error {
	rdc.lock.Lock()
	if rdc.ci.limits.hasExhaustedLimit() {
		rdc.lock.Unlock()
		return nil
	}
	rdc.lock.Unlock()

	collected, err := rdc.dispatchAndCollect(ctx, cursor)
	if err != nil {
		return err
	}

	if len(collected) == 0 {
		return nil
	}

	// Kick off a worker to filter the results via a check and then publish what was found.
	rdc.taskrunner.Schedule(func(ctx context.Context) error {
		return rdc.runChecker(ctx, collected)
	})

	// Start another dispatch at the cursor of the last response, to run in the background
	// and collect more results for filtering while the checker is running.
	rdc.taskrunner.Schedule(func(ctx context.Context) error {
		return rdc.runDispatch(ctx, collected[len(collected)-1].AfterResponseCursor)
	})

	return nil
}

func (rdc *rdc) runChecker(ctx context.Context, collected []*v1.DispatchLookupResources2Response) error {
	rdc.lock.Lock()
	if rdc.ci.limits.hasExhaustedLimit() {
		rdc.lock.Unlock()
		return nil
	}
	rdc.lock.Unlock()

	checkHints := make(map[string]*v1.ResourceCheckResult, len(collected))
	resourceIDsToCheck := make([]string, 0, len(collected))
	for _, resource := range collected {
		hintKey, err := hintString(resource.Resource.ResourceId, rdc.entrypoint, rdc.parentRequest.TerminalSubject)
		if err != nil {
			return err
		}

		resourceIDsToCheck = append(resourceIDsToCheck, resource.Resource.ResourceId)

		checkHints[hintKey] = &v1.ResourceCheckResult{
			Membership: v1.ResourceCheckResult_MEMBER,
		}
	}

	// Batch check the results to filter to those visible and then publish just the visible resources.
	resultsByResourceID, checkMetadata, err := computed.ComputeBulkCheck(ctx, rdc.checkDispatcher, computed.CheckParameters{
		ResourceType:  rdc.parentRequest.ResourceRelation,
		Subject:       rdc.parentRequest.TerminalSubject,
		CaveatContext: rdc.parentRequest.Context.AsMap(),
		AtRevision:    rdc.parentRequest.Revision,
		MaximumDepth:  rdc.parentRequest.Metadata.DepthRemaining - 1,
		DebugOption:   computed.NoDebugging,
		CheckHints:    checkHints,
	}, resourceIDsToCheck)
	if err != nil {
		return err
	}

	// Publish any resources that are visible.
	metadata := checkMetadata
	for _, resource := range collected {
		result, ok := resultsByResourceID[resource.Resource.ResourceId]
		if !ok {
			continue
		}

		switch result.Membership {
		case v1.ResourceCheckResult_MEMBER:
			fallthrough

		case v1.ResourceCheckResult_CAVEATED_MEMBER:
			rdc.lock.Lock()
			if err := publishResultToParentStream(resource, rdc.ci, rdc.foundResources, result.MissingExprFields, metadata, rdc.parentStream); err != nil {
				rdc.lock.Unlock()
				return err
			}

			if rdc.ci.limits.hasExhaustedLimit() {
				rdc.lock.Unlock()
				return nil
			}
			rdc.lock.Unlock()

			metadata = emptyMetadata

		case v1.ResourceCheckResult_NOT_MEMBER:
			// Skip.
			continue

		default:
			return spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
		}
	}

	return nil
}

func (rdc *rdc) runAndWait() error {
	currentCursor := rdc.ci.currentCursor

	// Kick off a dispatch at the current cursor.
	rdc.taskrunner.Schedule(func(ctx context.Context) error {
		return rdc.runDispatch(ctx, currentCursor)
	})

	return rdc.taskrunner.Wait()
}

// unfilteredLookupResourcesDispatchStreamForEntrypoint creates a new dispatch stream that wraps
// the parent stream, and publishes the results of the lookup resources call to the parent stream,
// mapped via foundResources.
func unfilteredLookupResourcesDispatchStreamForEntrypoint(
	ctx context.Context,
	foundResources dispatchableResourcesSubjectMap2,
	parentStream dispatch.LookupResources2Stream,
	ci cursorInformation,
) dispatch.LookupResources2Stream {
	needsCallAddedToMetadata := true

	wrappedStream := dispatch.NewHandlingDispatchStream(ctx, func(result *v1.DispatchLookupResources2Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		metadata := emptyMetadata
		if needsCallAddedToMetadata {
			metadata = addCallToResponseMetadata(metadata)
			needsCallAddedToMetadata = false
		} else {
			metadata = addAdditionalDepthRequired(metadata)
		}

		return publishResultToParentStream(result, ci, foundResources, nil, metadata, parentStream)
	})

	return wrappedStream
}

// publishResultToParentStream publishes the result of a lookup resources call to the parent stream,
// mapped via foundResources.
func publishResultToParentStream(
	result *v1.DispatchLookupResources2Response,
	ci cursorInformation,
	foundResources dispatchableResourcesSubjectMap2,
	additionalMissingContext []string,
	additionalMetadata *v1.ResponseMeta,
	parentStream dispatch.LookupResources2Stream,
) error {
	// Map the found resources via the subject+resources used for dispatching, to determine
	// if any need to be made conditional due to caveats.
	mappedResource, err := foundResources.mapPossibleResource(result.Resource)
	if err != nil {
		return err
	}

	if !ci.limits.prepareForPublishing() {
		return nil
	}

	// The cursor for the response is that of the parent response + the cursor from the result itself.
	afterResponseCursor, err := combineCursors(
		ci.responsePartialCursor(),
		result.AfterResponseCursor,
	)
	if err != nil {
		return err
	}

	metadata := combineResponseMetadata(result.Metadata, additionalMetadata)

	missingContextParameters := mapz.NewSet(mappedResource.MissingContextParams...)
	missingContextParameters.Extend(result.Resource.MissingContextParams)
	missingContextParameters.Extend(additionalMissingContext)

	mappedResource.MissingContextParams = missingContextParameters.AsSlice()

	resp := &v1.DispatchLookupResources2Response{
		Resource:            mappedResource,
		Metadata:            metadata,
		AfterResponseCursor: afterResponseCursor,
	}

	return parentStream.Publish(resp)
}

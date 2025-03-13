package graph

import (
	"context"
	"strconv"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	"github.com/authzed/spicedb/internal/graph/hints"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// runCheckerAndDispatch runs the dispatch and checker for a lookup resources call, and publishes
// the results to the parent stream. This function is responsible for handling checking the
// results to filter them, and then dispatching those found.
func runCheckerAndDispatch(
	ctx context.Context,
	parentReq ValidatedLookupResources2Request,
	foundResources dispatchableResourcesSubjectMap2,
	ci cursorInformation,
	parentStream dispatch.LookupResources2Stream,
	newSubjectType *core.RelationReference,
	filteredSubjectIDs []string,
	entrypoint schema.ReachabilityEntrypoint,
	lrDispatcher dispatch.LookupResources2,
	checkDispatcher dispatch.Check,
	concurrencyLimit uint16,
	dispatchChunkSize uint16,
) error {
	// Only allow max one dispatcher and one checker to run concurrently.
	concurrencyLimit = min(concurrencyLimit, 2)

	currentCheckIndex, err := ci.integerSectionValue()
	if err != nil {
		return err
	}

	rdc := &checkAndDispatchRunner{
		parentRequest:      parentReq,
		foundResources:     foundResources,
		ci:                 ci,
		parentStream:       parentStream,
		newSubjectType:     newSubjectType,
		filteredSubjectIDs: filteredSubjectIDs,
		currentCheckIndex:  currentCheckIndex,
		entrypoint:         entrypoint,
		lrDispatcher:       lrDispatcher,
		checkDispatcher:    checkDispatcher,
		taskrunner:         taskrunner.NewTaskRunner(ctx, concurrencyLimit),
		lock:               &sync.Mutex{},
		dispatchChunkSize:  dispatchChunkSize,
	}

	return rdc.runAndWait()
}

type checkAndDispatchRunner struct {
	parentRequest     ValidatedLookupResources2Request
	foundResources    dispatchableResourcesSubjectMap2
	ci                cursorInformation
	parentStream      dispatch.LookupResources2Stream
	newSubjectType    *core.RelationReference
	entrypoint        schema.ReachabilityEntrypoint
	lrDispatcher      dispatch.LookupResources2
	checkDispatcher   dispatch.Check
	dispatchChunkSize uint16

	filteredSubjectIDs []string
	currentCheckIndex  int

	taskrunner *taskrunner.TaskRunner

	lock *sync.Mutex
}

func (rdc *checkAndDispatchRunner) runAndWait() error {
	// Kick off a check at the current cursor, to filter a portion of the initial results set.
	rdc.taskrunner.Schedule(func(ctx context.Context) error {
		return rdc.runChecker(ctx, rdc.currentCheckIndex)
	})

	return rdc.taskrunner.Wait()
}

func (rdc *checkAndDispatchRunner) runChecker(ctx context.Context, startingIndex int) error {
	rdc.lock.Lock()
	if rdc.ci.limits.hasExhaustedLimit() {
		rdc.lock.Unlock()
		return nil
	}
	rdc.lock.Unlock()

	endingIndex := min(startingIndex+int(rdc.dispatchChunkSize), len(rdc.filteredSubjectIDs))
	resourceIDsToCheck := rdc.filteredSubjectIDs[startingIndex:endingIndex]
	if len(resourceIDsToCheck) == 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "lr2Check", trace.WithAttributes(
		attribute.Int("resource-id-count", len(resourceIDsToCheck)),
	))
	defer span.End()

	checkHints := make([]*v1.CheckHint, 0, len(resourceIDsToCheck))
	for _, resourceID := range resourceIDsToCheck {
		checkHint, err := hints.HintForEntrypoint(
			rdc.entrypoint,
			resourceID,
			tuple.FromCoreObjectAndRelation(rdc.parentRequest.TerminalSubject),
			&v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			})
		if err != nil {
			return err
		}
		checkHints = append(checkHints, checkHint)
	}

	// NOTE: we are checking the containing permission here, *not* the target relation, as
	// the goal is to shear for the containing permission.
	resultsByResourceID, checkMetadata, _, err := computed.ComputeBulkCheck(ctx, rdc.checkDispatcher, computed.CheckParameters{
		ResourceType:  tuple.FromCoreRelationReference(rdc.newSubjectType),
		Subject:       tuple.FromCoreObjectAndRelation(rdc.parentRequest.TerminalSubject),
		CaveatContext: rdc.parentRequest.Context.AsMap(),
		AtRevision:    rdc.parentRequest.Revision,
		MaximumDepth:  rdc.parentRequest.Metadata.DepthRemaining - 1,
		DebugOption:   computed.NoDebugging,
		CheckHints:    checkHints,
	}, resourceIDsToCheck, rdc.dispatchChunkSize)
	if err != nil {
		return err
	}

	adjustedResources := rdc.foundResources.cloneAsMutable()

	// Dispatch any resources that are visible.
	resourceIDToDispatch := make([]string, 0, len(resourceIDsToCheck))
	for _, resourceID := range resourceIDsToCheck {
		result, ok := resultsByResourceID[resourceID]
		if !ok {
			continue
		}

		switch result.Membership {
		case v1.ResourceCheckResult_MEMBER:
			fallthrough

		case v1.ResourceCheckResult_CAVEATED_MEMBER:
			// Record any additional caveats missing from the check.
			adjustedResources.withAdditionalMissingContextForDispatchedResourceID(resourceID, result.MissingExprFields)
			resourceIDToDispatch = append(resourceIDToDispatch, resourceID)

		case v1.ResourceCheckResult_NOT_MEMBER:
			// Skip.
			continue

		default:
			return spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
		}
	}

	if len(resourceIDToDispatch) > 0 {
		// Schedule a dispatch of those resources.
		rdc.taskrunner.Schedule(func(ctx context.Context) error {
			return rdc.runDispatch(ctx, resourceIDToDispatch, adjustedResources.asReadOnly(), checkMetadata, startingIndex)
		})
	}

	// Start the next check chunk (if applicable).
	nextIndex := startingIndex + len(resourceIDsToCheck)
	if nextIndex < len(rdc.filteredSubjectIDs) {
		rdc.taskrunner.Schedule(func(ctx context.Context) error {
			return rdc.runChecker(ctx, nextIndex)
		})
	}

	return nil
}

func (rdc *checkAndDispatchRunner) runDispatch(
	ctx context.Context,
	resourceIDsToDispatch []string,
	adjustedResources dispatchableResourcesSubjectMap2,
	checkMetadata *v1.ResponseMeta,
	startingIndex int,
) error {
	rdc.lock.Lock()
	if rdc.ci.limits.hasExhaustedLimit() {
		rdc.lock.Unlock()
		return nil
	}
	rdc.lock.Unlock()

	ctx, span := tracer.Start(ctx, "lr2Dispatch", trace.WithAttributes(
		attribute.Int("resource-id-count", len(resourceIDsToDispatch)),
	))
	defer span.End()

	// NOTE: Since we extracted a custom section from the cursor at the beginning of this run, we have to add
	// the starting index to the cursor to ensure that the next run starts from the correct place, and we have
	// to use the *updated* cursor below on the dispatch.
	updatedCi, err := rdc.ci.withOutgoingSection(strconv.Itoa(startingIndex))
	if err != nil {
		return err
	}
	responsePartialCursor := updatedCi.responsePartialCursor()

	// Dispatch to the parent resource type and publish any results found.
	isFirstPublishCall := true

	wrappedStream := dispatch.NewHandlingDispatchStream(ctx, func(result *v1.DispatchLookupResources2Response) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := publishResultToParentStream(ctx, result, rdc.ci, responsePartialCursor, adjustedResources, nil, isFirstPublishCall, checkMetadata, rdc.parentStream); err != nil {
			return err
		}
		isFirstPublishCall = false
		return nil
	})

	return rdc.lrDispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: rdc.parentRequest.ResourceRelation,
		SubjectRelation:  rdc.newSubjectType,
		SubjectIds:       resourceIDsToDispatch,
		TerminalSubject:  rdc.parentRequest.TerminalSubject,
		Metadata: &v1.ResolverMeta{
			AtRevision:     rdc.parentRequest.Revision.String(),
			DepthRemaining: rdc.parentRequest.Metadata.DepthRemaining - 1,
		},
		OptionalCursor: updatedCi.currentCursor,
		OptionalLimit:  rdc.ci.limits.currentLimit,
		Context:        rdc.parentRequest.Context,
	}, wrappedStream)
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
	isFirstPublishCall := true

	wrappedStream := dispatch.NewHandlingDispatchStream(ctx, func(result *v1.DispatchLookupResources2Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		if err := publishResultToParentStream(ctx, result, ci, ci.responsePartialCursor(), foundResources, nil, isFirstPublishCall, emptyMetadata, parentStream); err != nil {
			return err
		}
		isFirstPublishCall = false
		return nil
	})

	return wrappedStream
}

// publishResultToParentStream publishes the result of a lookup resources call to the parent stream,
// mapped via foundResources.
func publishResultToParentStream(
	ctx context.Context,
	result *v1.DispatchLookupResources2Response,
	ci cursorInformation,
	responseCursor *v1.Cursor,
	foundResources dispatchableResourcesSubjectMap2,
	additionalMissingContext []string,
	isFirstPublishCall bool,
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
		responseCursor,
		result.AfterResponseCursor,
	)
	if err != nil {
		return err
	}

	metadata := result.Metadata
	if isFirstPublishCall {
		metadata = addCallToResponseMetadata(metadata)
		metadata = combineResponseMetadata(ctx, metadata, additionalMetadata)
	} else {
		metadata = addAdditionalDepthRequired(metadata)
	}

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

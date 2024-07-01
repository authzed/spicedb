package graph

import (
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph/computed"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)

func NewCursoredLookupResources2(dl dispatch.LookupResources2, dc dispatch.Check, concurrencyLimit uint16) *CursoredLookupResources2 {
	return &CursoredLookupResources2{dl, dc, concurrencyLimit}
}

type CursoredLookupResources2 struct {
	dl               dispatch.LookupResources2
	dc               dispatch.Check
	concurrencyLimit uint16
}

type ValidatedLookupResources2Request struct {
	*v1.DispatchLookupResources2Request
	Revision datastore.Revision
}

func (crr *CursoredLookupResources2) LookupResources2(
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
) error {
	if req.TerminalSubject == nil {
		return spiceerrors.MustBugf("no terminal subject given to lookup resources dispatch")
	}

	if slices.Contains(req.SubjectIds, tuple.PublicWildcard) {
		return NewWildcardNotAllowedErr("cannot perform lookup resources on wildcard", "subject_id")
	}

	if len(req.SubjectIds) == 0 {
		return spiceerrors.MustBugf("no subjects ids given to lookup resources dispatch")
	}

	// Sort for stability.
	sort.Strings(req.SubjectIds)

	ctx := stream.Context()
	limits := newLimitTracker(req.OptionalLimit)
	ci, err := newCursorInformation(req.OptionalCursor, limits, dispatchVersion)
	if err != nil {
		return err
	}

	return withSubsetInCursor(ci,
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			// If the resource type matches the subject type, yield directly as a one-to-one result
			// for each subjectID.
			if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
				req.SubjectRelation.Relation == req.ResourceRelation.Relation {
				for index, subjectID := range req.SubjectIds {
					if index < currentOffset {
						continue
					}

					if !ci.limits.prepareForPublishing() {
						return nil
					}

					err := stream.Publish(&v1.DispatchLookupResources2Response{
						Resource: &v1.PossibleResource{
							ResourceId:    subjectID,
							ForSubjectIds: []string{subjectID},
						},
						Metadata:            emptyMetadata,
						AfterResponseCursor: nextCursorWith(index + 1),
					})
					if err != nil {
						return err
					}
				}
			}
			return nil
		}, func(ci cursorInformation) error {
			// Once done checking for the matching subject type, yield by dispatching over entrypoints.
			return crr.afterSameType(ctx, ci, req, stream)
		})
}

func (crr *CursoredLookupResources2) afterSameType(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupResources2Request,
	parentStream dispatch.LookupResources2Stream,
) error {
	dispatched := &syncONRSet{}

	// Load the type system and reachability graph to find the entrypoints for the reachability.
	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(req.Revision)
	_, typeSystem, err := typesystem.ReadNamespaceAndTypes(ctx, req.ResourceRelation.Namespace, reader)
	if err != nil {
		return err
	}

	rg := typesystem.ReachabilityGraphFor(typeSystem)
	entrypoints, err := rg.OptimizedEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: req.SubjectRelation.Namespace,
		Relation:  req.SubjectRelation.Relation,
	}, req.ResourceRelation)
	if err != nil {
		return err
	}

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	return withParallelizedStreamingIterableInCursor(ctx, ci, entrypoints, parentStream, crr.concurrencyLimit,
		func(ctx context.Context, ci cursorInformation, entrypoint typesystem.ReachabilityEntrypoint, stream dispatch.LookupResources2Stream) error {
			switch entrypoint.EntrypointKind() {
			case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
				return crr.lookupRelationEntrypoint(ctx, ci, entrypoint, rg, reader, req, stream, dispatched)

			case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
				containingRelation := entrypoint.ContainingRelationOrPermission()
				rewrittenSubjectRelation := &core.RelationReference{
					Namespace: containingRelation.Namespace,
					Relation:  containingRelation.Relation,
				}

				rsm := subjectIDsToResourcesMap2(rewrittenSubjectRelation, req.SubjectIds)
				drsm := rsm.asReadOnly()

				return crr.redispatchOrReport(
					ctx,
					ci,
					rewrittenSubjectRelation,
					drsm,
					rg,
					entrypoint,
					stream,
					req,
					dispatched,
				)

			case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
				return crr.lookupTTUEntrypoint(ctx, ci, entrypoint, rg, reader, req, stream, dispatched)

			default:
				return spiceerrors.MustBugf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind())
			}
		})
}

func (crr *CursoredLookupResources2) lookupRelationEntrypoint(
	ctx context.Context,
	ci cursorInformation,
	entrypoint typesystem.ReachabilityEntrypoint,
	rg *typesystem.ReachabilityGraph,
	reader datastore.Reader,
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
	dispatched *syncONRSet,
) error {
	relationReference, err := entrypoint.DirectRelation()
	if err != nil {
		return err
	}

	_, relTypeSystem, err := typesystem.ReadNamespaceAndTypes(ctx, relationReference.Namespace, reader)
	if err != nil {
		return err
	}

	// Build the list of subjects to lookup based on the type information available.
	isDirectAllowed, err := relTypeSystem.IsAllowedDirectRelation(
		relationReference.Relation,
		req.SubjectRelation.Namespace,
		req.SubjectRelation.Relation,
	)
	if err != nil {
		return err
	}

	subjectIds := make([]string, 0, len(req.SubjectIds)+1)
	if isDirectAllowed == typesystem.DirectRelationValid {
		subjectIds = append(subjectIds, req.SubjectIds...)
	}

	if req.SubjectRelation.Relation == tuple.Ellipsis {
		isWildcardAllowed, err := relTypeSystem.IsAllowedPublicNamespace(relationReference.Relation, req.SubjectRelation.Namespace)
		if err != nil {
			return err
		}

		if isWildcardAllowed == typesystem.PublicSubjectAllowed {
			subjectIds = append(subjectIds, "*")
		}
	}

	// Lookup the subjects and then redispatch/report results.
	relationFilter := datastore.SubjectRelationFilter{
		NonEllipsisRelation: req.SubjectRelation.Relation,
	}

	if req.SubjectRelation.Relation == tuple.Ellipsis {
		relationFilter = datastore.SubjectRelationFilter{
			IncludeEllipsisRelation: true,
		}
	}

	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: subjectIds,
		RelationFilter:     relationFilter,
	}

	return crr.redispatchOrReportOverDatabaseQuery(
		ctx,
		redispatchOverDatabaseConfig2{
			ci:                 ci,
			reader:             reader,
			subjectsFilter:     subjectsFilter,
			sourceResourceType: relationReference,
			foundResourceType:  relationReference,
			entrypoint:         entrypoint,
			rg:                 rg,
			concurrencyLimit:   crr.concurrencyLimit,
			parentStream:       stream,
			parentRequest:      req,
			dispatched:         dispatched,
		},
	)
}

type redispatchOverDatabaseConfig2 struct {
	ci cursorInformation

	reader datastore.Reader

	subjectsFilter     datastore.SubjectsFilter
	sourceResourceType *core.RelationReference
	foundResourceType  *core.RelationReference

	entrypoint typesystem.ReachabilityEntrypoint
	rg         *typesystem.ReachabilityGraph

	concurrencyLimit uint16
	parentStream     dispatch.LookupResources2Stream
	parentRequest    ValidatedLookupResources2Request
	dispatched       *syncONRSet
}

func (crr *CursoredLookupResources2) redispatchOrReportOverDatabaseQuery(
	ctx context.Context,
	config redispatchOverDatabaseConfig2,
) error {
	return withDatastoreCursorInCursor(ctx, config.ci, config.parentStream, config.concurrencyLimit,
		// Find the target resources for the subject.
		func(queryCursor options.Cursor) ([]itemAndPostCursor[dispatchableResourcesSubjectMap2], error) {
			it, err := config.reader.ReverseQueryRelationships(
				ctx,
				config.subjectsFilter,
				options.WithResRelation(&options.ResourceRelation{
					Namespace: config.sourceResourceType.Namespace,
					Relation:  config.sourceResourceType.Relation,
				}),
				options.WithSortForReverse(options.BySubject),
				options.WithAfterForReverse(queryCursor),
			)
			if err != nil {
				return nil, err
			}
			defer it.Close()

			// Chunk based on the FilterMaximumIDCount, to ensure we never send more than that amount of
			// results to a downstream dispatch.
			rsm := newResourcesSubjectMap2WithCapacity(config.sourceResourceType, uint32(datastore.FilterMaximumIDCount))
			toBeHandled := make([]itemAndPostCursor[dispatchableResourcesSubjectMap2], 0)
			currentCursor := queryCursor

			for tpl := it.Next(); tpl != nil; tpl = it.Next() {
				if it.Err() != nil {
					return nil, it.Err()
				}

				var missingContextParameters []string

				// If a caveat exists on the relationship, run it and filter the results, marking those that have missing context.
				if tpl.Caveat != nil && tpl.Caveat.CaveatName != "" {
					caveatExpr := caveats.CaveatAsExpr(tpl.Caveat)
					runResult, err := caveats.RunCaveatExpression(ctx, caveatExpr, config.parentRequest.Context.AsMap(), config.reader, caveats.RunCaveatExpressionNoDebugging)
					if err != nil {
						return nil, err
					}

					// If a partial result is returned, collect the missing context parameters.
					if runResult.IsPartial() {
						missingNames, err := runResult.MissingVarNames()
						if err != nil {
							return nil, err
						}

						missingContextParameters = missingNames
					} else if !runResult.Value() {
						// If the run result shows the caveat does not apply, skip. This shears the tree of results early.
						continue
					}
				}

				if err := rsm.addRelationship(tpl, missingContextParameters); err != nil {
					return nil, err
				}

				if rsm.len() == int(datastore.FilterMaximumIDCount) {
					toBeHandled = append(toBeHandled, itemAndPostCursor[dispatchableResourcesSubjectMap2]{
						item:   rsm.asReadOnly(),
						cursor: currentCursor,
					})
					rsm = newResourcesSubjectMap2WithCapacity(config.sourceResourceType, uint32(datastore.FilterMaximumIDCount))
					currentCursor = tpl
				}
			}
			it.Close()

			if rsm.len() > 0 {
				toBeHandled = append(toBeHandled, itemAndPostCursor[dispatchableResourcesSubjectMap2]{
					item:   rsm.asReadOnly(),
					cursor: currentCursor,
				})
			}

			return toBeHandled, nil
		},

		// Redispatch or report the results.
		func(
			ctx context.Context,
			ci cursorInformation,
			drsm dispatchableResourcesSubjectMap2,
			currentStream dispatch.LookupResources2Stream,
		) error {
			return crr.redispatchOrReport(
				ctx,
				ci,
				config.foundResourceType,
				drsm,
				config.rg,
				config.entrypoint,
				currentStream,
				config.parentRequest,
				config.dispatched,
			)
		},
	)
}

func (crr *CursoredLookupResources2) lookupTTUEntrypoint(ctx context.Context,
	ci cursorInformation,
	entrypoint typesystem.ReachabilityEntrypoint,
	rg *typesystem.ReachabilityGraph,
	reader datastore.Reader,
	req ValidatedLookupResources2Request,
	stream dispatch.LookupResources2Stream,
	dispatched *syncONRSet,
) error {
	containingRelation := entrypoint.ContainingRelationOrPermission()

	_, ttuTypeSystem, err := typesystem.ReadNamespaceAndTypes(ctx, containingRelation.Namespace, reader)
	if err != nil {
		return err
	}

	tuplesetRelation, err := entrypoint.TuplesetRelation()
	if err != nil {
		return err
	}

	// Determine whether this TTU should be followed, which will be the case if the subject relation's namespace
	// is allowed in any form on the relation; since arrows ignore the subject's relation (if any), we check
	// for the subject namespace as a whole.
	allowedRelations, err := ttuTypeSystem.GetAllowedDirectNamespaceSubjectRelations(tuplesetRelation, req.SubjectRelation.Namespace)
	if err != nil {
		return err
	}

	if allowedRelations == nil {
		return nil
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: req.SubjectIds,
	}

	// Optimization: if there is a single allowed relation, pass it as a subject relation filter to make things faster
	// on querying.
	if allowedRelations.Len() == 1 {
		allowedRelationName := allowedRelations.AsSlice()[0]
		subjectsFilter.RelationFilter = datastore.SubjectRelationFilter{}.WithRelation(allowedRelationName)
	}

	tuplesetRelationReference := &core.RelationReference{
		Namespace: containingRelation.Namespace,
		Relation:  tuplesetRelation,
	}

	return crr.redispatchOrReportOverDatabaseQuery(
		ctx,
		redispatchOverDatabaseConfig2{
			ci:                 ci,
			reader:             reader,
			subjectsFilter:     subjectsFilter,
			sourceResourceType: tuplesetRelationReference,
			foundResourceType:  containingRelation,
			entrypoint:         entrypoint,
			rg:                 rg,
			parentStream:       stream,
			parentRequest:      req,
			dispatched:         dispatched,
		},
	)
}

func hintString(resourceID string, entrypoint typesystem.ReachabilityEntrypoint, terminalSubject *core.ObjectAndRelation) (string, error) {
	resourceKey, err := entrypoint.CheckHintForResource(resourceID)
	if err != nil {
		return "", err
	}

	return typesystem.CheckHint(resourceKey, terminalSubject), nil
}

// redispatchOrReport checks if further redispatching is necessary for the found resource
// type. If not, and the found resource type+relation matches the target resource type+relation,
// the resource is reported to the parent stream.
func (crr *CursoredLookupResources2) redispatchOrReport(
	ctx context.Context,
	ci cursorInformation,
	foundResourceType *core.RelationReference,
	foundResources dispatchableResourcesSubjectMap2,
	rg *typesystem.ReachabilityGraph,
	entrypoint typesystem.ReachabilityEntrypoint,
	parentStream dispatch.LookupResources2Stream,
	parentRequest ValidatedLookupResources2Request,
	dispatched *syncONRSet,
) error {
	if foundResources.isEmpty() {
		// Nothing more to do.
		return nil
	}

	// Check for entrypoints for the new found resource type.
	hasResourceEntrypoints, err := rg.HasOptimizedEntrypointsForSubjectToResource(ctx, foundResourceType, parentRequest.ResourceRelation)
	if err != nil {
		return err
	}

	return withSubsetInCursor(ci,
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			if !hasResourceEntrypoints {
				// If the found resource matches the target resource type and relation, potentially yield the resource.
				if foundResourceType.Namespace == parentRequest.ResourceRelation.Namespace && foundResourceType.Relation == parentRequest.ResourceRelation.Relation {
					resources := foundResources.asPossibleResources()
					if len(resources) == 0 {
						return nil
					}

					if currentOffset >= len(resources) {
						return nil
					}

					offsetted := resources[currentOffset:]
					if len(offsetted) == 0 {
						return nil
					}

					filtered := offsetted
					metadata := emptyMetadata

					// If the entrypoint is not a direct result, issue a check to further filter the results on the intersection or exclusion.
					if !entrypoint.IsDirectResult() {
						resourceIDs := make([]string, 0, len(offsetted))
						checkHints := make(map[string]*v1.ResourceCheckResult, len(offsetted))
						for _, resource := range offsetted {
							resourceIDs = append(resourceIDs, resource.ResourceId)
							hintKey, err := hintString(resource.ResourceId, entrypoint, parentRequest.TerminalSubject)
							if err != nil {
								return err
							}

							checkHints[hintKey] = &v1.ResourceCheckResult{
								Membership: v1.ResourceCheckResult_MEMBER,
							}
						}

						resultsByResourceID, checkMetadata, err := computed.ComputeBulkCheck(ctx, crr.dc, computed.CheckParameters{
							ResourceType:  parentRequest.ResourceRelation,
							Subject:       parentRequest.TerminalSubject,
							CaveatContext: parentRequest.Context.AsMap(),
							AtRevision:    parentRequest.Revision,
							MaximumDepth:  parentRequest.Metadata.DepthRemaining - 1,
							DebugOption:   computed.NoDebugging,
							CheckHints:    checkHints,
						}, resourceIDs)
						if err != nil {
							return err
						}

						metadata = addCallToResponseMetadata(checkMetadata)

						filtered = make([]*v1.PossibleResource, 0, len(offsetted))
						for _, resource := range offsetted {
							result, ok := resultsByResourceID[resource.ResourceId]
							if !ok {
								continue
							}

							switch result.Membership {
							case v1.ResourceCheckResult_MEMBER:
								filtered = append(filtered, resource)

							case v1.ResourceCheckResult_CAVEATED_MEMBER:
								missingContextParams := mapz.NewSet(result.MissingExprFields...)
								missingContextParams.Extend(resource.MissingContextParams)

								filtered = append(filtered, &v1.PossibleResource{
									ResourceId:           resource.ResourceId,
									ForSubjectIds:        resource.ForSubjectIds,
									MissingContextParams: missingContextParams.AsSlice(),
								})

							case v1.ResourceCheckResult_NOT_MEMBER:
								// Skip.

							default:
								return spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
							}
						}
					}

					for index, resource := range filtered {
						if !ci.limits.prepareForPublishing() {
							return nil
						}

						err := parentStream.Publish(&v1.DispatchLookupResources2Response{
							Resource:            resource,
							Metadata:            metadata,
							AfterResponseCursor: nextCursorWith(currentOffset + index + 1),
						})
						if err != nil {
							return err
						}

						metadata = emptyMetadata
					}
					return nil
				}
			}
			return nil
		}, func(ci cursorInformation) error {
			if !hasResourceEntrypoints {
				return nil
			}

			// The new subject type for dispatching was the found type of the *resource*.
			newSubjectType := foundResourceType

			// To avoid duplicate work, remove any subjects already dispatched.
			filteredSubjectIDs := foundResources.filterSubjectIDsToDispatch(dispatched, newSubjectType)
			if len(filteredSubjectIDs) == 0 {
				return nil
			}

			// The stream that collects the results of the dispatch will add metadata to the response,
			// map the results found based on the mapping data in the results and, if the entrypoint is not
			// direct, issue a check to further filter the results.
			currentCursor := ci.currentCursor

			// Loop until we've produced enough results to satisfy the limit. This is necessary because
			// the dispatch may return a set of results that, after checking, is less than the limit.
			for {
				stream, completed := lookupResourcesDispatchStreamForEntrypoint(ctx, foundResources, parentStream, entrypoint, ci, parentRequest, crr.dc)

				// NOTE: if the entrypoint is a direct result, then all results returned by the dispatch will, themselves,
				// be direct results. In this case, we can request the full limit of results. If the entrypoint is not a
				// direct result, then we must request more than the limit in the hope that we get enough results to satisfy the
				// limit after filtering.
				var limit uint32 = uint32(datastore.FilterMaximumIDCount)
				if entrypoint.IsDirectResult() {
					limit = parentRequest.OptionalLimit
				}

				// Dispatch the found resources as the subjects for the next call, to continue the
				// resolution.
				err = crr.dl.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
					ResourceRelation: parentRequest.ResourceRelation,
					SubjectRelation:  newSubjectType,
					SubjectIds:       filteredSubjectIDs,
					TerminalSubject:  parentRequest.TerminalSubject,
					Metadata: &v1.ResolverMeta{
						AtRevision:     parentRequest.Revision.String(),
						DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
					},
					OptionalCursor: currentCursor,
					OptionalLimit:  limit, // Request more than the limit to hopefully get enough results.
				}, stream)
				if err != nil {
					// If the dispatch was canceled due to the limit, do not treat it as an error.
					if errors.Is(err, errCanceledBecauseLimitReached) {
						return err
					}
				}

				nextCursor, err := completed()
				if err != nil {
					return err
				}

				if nextCursor == nil || ci.limits.hasExhaustedLimit() {
					break
				}
				currentCursor = nextCursor
			}

			return nil
		})
}

func lookupResourcesDispatchStreamForEntrypoint(
	ctx context.Context,
	foundResources dispatchableResourcesSubjectMap2,
	parentStream dispatch.LookupResources2Stream,
	entrypoint typesystem.ReachabilityEntrypoint,
	ci cursorInformation,
	parentRequest ValidatedLookupResources2Request,
	dc dispatch.Check,
) (dispatch.LookupResources2Stream, func() (*v1.Cursor, error)) {
	// Branch the context so that the dispatch can be canceled without canceling the parent
	// call.
	sctx, cancelDispatch := branchContext(ctx)

	needsCallAddedToMetadata := true
	resultsToCheck := make([]*v1.DispatchLookupResources2Response, 0, int(datastore.FilterMaximumIDCount))
	var nextCursor *v1.Cursor

	publishResultToParentStream := func(
		result *v1.DispatchLookupResources2Response,
		additionalMissingContext []string,
		additionalMetadata *v1.ResponseMeta,
	) error {
		// Map the found resources via the subject+resources used for dispatching, to determine
		// if any need to be made conditional due to caveats.
		mappedResource, err := foundResources.mapPossibleResource(result.Resource)
		if err != nil {
			return err
		}

		if !ci.limits.prepareForPublishing() {
			cancelDispatch(errCanceledBecauseLimitReached)
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

		// Only the first dispatched result gets the call added to it. This is to prevent overcounting
		// of the batched dispatch.
		if needsCallAddedToMetadata {
			metadata = addCallToResponseMetadata(metadata)
			needsCallAddedToMetadata = false
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

	batchCheckAndPublishIfNecessary := func(result *v1.DispatchLookupResources2Response) error {
		// Add the result to the list of results to check. If nil, this is the final call to check+publish.
		if result != nil {
			resultsToCheck = append(resultsToCheck, result)
		}

		// If we have not yet reached the maximum number of results to check and this is not the final
		// call, return early.
		if len(resultsToCheck) < int(datastore.FilterMaximumIDCount) && result != nil {
			return nil
		}

		// Ensure there are items left to check.
		if len(resultsToCheck) == 0 {
			return nil
		}

		// Build the set of resource IDs to check and the hints to short circuit the check on the current entrypoint.
		checkHints := make(map[string]*v1.ResourceCheckResult, len(resultsToCheck))
		resourceIDsToCheck := make([]string, 0, len(resultsToCheck))
		for _, resource := range resultsToCheck {
			hintKey, err := hintString(resource.Resource.ResourceId, entrypoint, parentRequest.TerminalSubject)
			if err != nil {
				return err
			}

			resourceIDsToCheck = append(resourceIDsToCheck, resource.Resource.ResourceId)

			checkHints[hintKey] = &v1.ResourceCheckResult{
				Membership: v1.ResourceCheckResult_MEMBER,
			}
		}

		// Batch check the results to filter to those visible and then publish just the visible resources.
		resultsByResourceID, checkMetadata, err := computed.ComputeBulkCheck(ctx, dc, computed.CheckParameters{
			ResourceType:  parentRequest.ResourceRelation,
			Subject:       parentRequest.TerminalSubject,
			CaveatContext: parentRequest.Context.AsMap(),
			AtRevision:    parentRequest.Revision,
			MaximumDepth:  parentRequest.Metadata.DepthRemaining - 1,
			DebugOption:   computed.NoDebugging,
			CheckHints:    checkHints,
		}, resourceIDsToCheck)
		if err != nil {
			return err
		}

		metadata := checkMetadata
		for _, resource := range resultsToCheck {
			result, ok := resultsByResourceID[resource.Resource.ResourceId]
			if !ok {
				continue
			}

			switch result.Membership {
			case v1.ResourceCheckResult_MEMBER:
				fallthrough

			case v1.ResourceCheckResult_CAVEATED_MEMBER:
				if err := publishResultToParentStream(resource, result.MissingExprFields, metadata); err != nil {
					return err
				}
				metadata = emptyMetadata

			case v1.ResourceCheckResult_NOT_MEMBER:
				// Skip.
				continue

			default:
				return spiceerrors.MustBugf("unexpected result from check: %v", result.Membership)
			}
		}

		resultsToCheck = make([]*v1.DispatchLookupResources2Response, 0, int(datastore.FilterMaximumIDCount))
		return nil
	}

	wrappedStream := dispatch.NewHandlingDispatchStream(sctx, func(result *v1.DispatchLookupResources2Response) error {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		nextCursor = result.AfterResponseCursor

		// If the entrypoint is a direct result, simply publish the found resource.
		if entrypoint.IsDirectResult() {
			return publishResultToParentStream(result, nil, emptyMetadata)
		}

		// Otherwise, queue the result for checking and publishing if the check succeeds.
		return batchCheckAndPublishIfNecessary(result)
	})

	return wrappedStream, func() (*v1.Cursor, error) {
		defer cancelDispatch(nil)
		return nextCursor, batchCheckAndPublishIfNecessary(nil)
	}
}

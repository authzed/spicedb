package graph

import (
	"context"
	"fmt"
	"sort"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewCursoredReachableResources creates an instance of CursoredReachableResources.
func NewCursoredReachableResources(d dispatch.ReachableResources, concurrencyLimit uint16) *CursoredReachableResources {
	return &CursoredReachableResources{d, concurrencyLimit}
}

// CursoredReachableResources exposes a method to perform ReachableResources requests, and
// delegates subproblems to the provided dispatch.ReachableResources instance.
type CursoredReachableResources struct {
	d                dispatch.ReachableResources
	concurrencyLimit uint16
}

// ValidatedReachableResourcesRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedReachableResourcesRequest struct {
	*v1.DispatchReachableResourcesRequest
	Revision datastore.Revision
}

func (crr *CursoredReachableResources) ReachableResources(
	req ValidatedReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
) error {
	if len(req.SubjectIds) == 0 {
		return fmt.Errorf("no subjects ids given to reachable resources dispatch")
	}

	// Sort for stability.
	sort.Strings(req.SubjectIds)

	limits, ctx := newLimitTracker(stream.Context(), req.OptionalLimit)
	ci, err := newCursorInformation(req.OptionalCursor, req.Revision, limits)
	if err != nil {
		return err
	}

	return withSubsetInCursor(ci, "same-type",
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			// If the resource type matches the subject type, yield directly as a one-to-one result
			// for each subjectID.
			if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
				req.SubjectRelation.Relation == req.ResourceRelation.Relation {
				for index, subjectID := range req.SubjectIds {
					if index < currentOffset {
						continue
					}

					okay, done := ci.limits.prepareForPublishing()
					defer done()

					if !okay {
						return nil
					}

					err := stream.Publish(&v1.DispatchReachableResourcesResponse{
						Resource: &v1.ReachableResource{
							ResourceId:    subjectID,
							ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
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

func (crr *CursoredReachableResources) afterSameType(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedReachableResourcesRequest,
	parentStream dispatch.ReachableResourcesStream,
) error {
	dispatched := &syncONRSet{}

	// Load the type system and reachability graph to find the entrypoints for the reachability.
	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(req.Revision)
	_, typeSystem, err := namespace.ReadNamespaceAndTypes(ctx, req.ResourceRelation.Namespace, reader)
	if err != nil {
		return err
	}

	rg := namespace.ReachabilityGraphFor(typeSystem.AsValidated())
	entrypoints, err := rg.OptimizedEntrypointsForSubjectToResource(ctx, &core.RelationReference{
		Namespace: req.SubjectRelation.Namespace,
		Relation:  req.SubjectRelation.Relation,
	}, req.ResourceRelation)
	if err != nil {
		return err
	}

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	return withParallelizedStreamingIterableInCursor(ctx, ci, "entrypoint", entrypoints, parentStream, crr.concurrencyLimit,
		func(ctx context.Context, ci cursorInformation, entrypoint namespace.ReachabilityEntrypoint, stream dispatch.ReachableResourcesStream) error {
			switch entrypoint.EntrypointKind() {
			case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
				return crr.lookupRelationEntrypoint(ctx, ci, entrypoint, rg, reader, req, stream, dispatched)

			case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
				containingRelation := entrypoint.ContainingRelationOrPermission()
				rewrittenSubjectRelation := &core.RelationReference{
					Namespace: containingRelation.Namespace,
					Relation:  containingRelation.Relation,
				}

				rsm := subjectIDsToResourcesMap(rewrittenSubjectRelation, req.SubjectIds)
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

func (crr *CursoredReachableResources) lookupRelationEntrypoint(
	ctx context.Context,
	ci cursorInformation,
	entrypoint namespace.ReachabilityEntrypoint,
	rg *namespace.ReachabilityGraph,
	reader datastore.Reader,
	req ValidatedReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
	dispatched *syncONRSet,
) error {
	relationReference, err := entrypoint.DirectRelation()
	if err != nil {
		return err
	}

	_, relTypeSystem, err := namespace.ReadNamespaceAndTypes(ctx, relationReference.Namespace, reader)
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
	if isDirectAllowed == namespace.DirectRelationValid {
		subjectIds = append(subjectIds, req.SubjectIds...)
	}

	if req.SubjectRelation.Relation == tuple.Ellipsis {
		isWildcardAllowed, err := relTypeSystem.IsAllowedPublicNamespace(relationReference.Relation, req.SubjectRelation.Namespace)
		if err != nil {
			return err
		}

		if isWildcardAllowed == namespace.PublicSubjectAllowed {
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

	return crr.chunkedRedispatch(ctx, ci, reader, subjectsFilter, relationReference,
		func(ctx context.Context, ci cursorInformation, drsm dispatchableResourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, ci, relationReference, drsm, rg, entrypoint, stream, req, dispatched)
		})
}

var queryLimit uint64 = uint64(datastore.FilterMaximumIDCount)

func (crr *CursoredReachableResources) chunkedRedispatch(
	ctx context.Context,
	ci cursorInformation,
	reader datastore.Reader,
	subjectsFilter datastore.SubjectsFilter,
	resourceType *core.RelationReference,
	handler func(ctx context.Context, ci cursorInformation, resources dispatchableResourcesSubjectMap) error,
) error {
	return withDatastoreCursorInCursor(ci, "query-rels",
		func(queryCursor options.Cursor, ci cursorInformation) (options.Cursor, error) {
			it, err := reader.ReverseQueryRelationships(
				ctx,
				subjectsFilter,
				options.WithResRelation(&options.ResourceRelation{
					Namespace: resourceType.Namespace,
					Relation:  resourceType.Relation,
				}),
				options.WithSortForReverse(options.BySubject),
				options.WithAfterForReverse(queryCursor),
				options.WithLimitForReverse(&queryLimit),
			)
			if err != nil {
				return nil, err
			}
			defer it.Close()

			rsm := newResourcesSubjectMap(resourceType)
			var lastTpl options.Cursor
			for tpl := it.Next(); tpl != nil; tpl = it.Next() {
				if it.Err() != nil {
					return nil, it.Err()
				}

				if err := rsm.addRelationship(tpl); err != nil {
					return nil, err
				}

				lastTpl = tpl
			}
			it.Close()

			if rsm.len() == 0 {
				return nil, nil
			}

			return lastTpl, handler(ctx, ci, rsm.asReadOnly())
		})
}

func (crr *CursoredReachableResources) lookupTTUEntrypoint(ctx context.Context,
	ci cursorInformation,
	entrypoint namespace.ReachabilityEntrypoint,
	rg *namespace.ReachabilityGraph,
	reader datastore.Reader,
	req ValidatedReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
	dispatched *syncONRSet,
) error {
	containingRelation := entrypoint.ContainingRelationOrPermission()

	_, ttuTypeSystem, err := namespace.ReadNamespaceAndTypes(ctx, containingRelation.Namespace, reader)
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
	isAllowed, err := ttuTypeSystem.IsAllowedDirectNamespace(tuplesetRelation, req.SubjectRelation.Namespace)
	if err != nil {
		return err
	}

	if isAllowed != namespace.AllowedNamespaceValid {
		return nil
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: req.SubjectIds,
	}

	tuplesetRelationReference := &core.RelationReference{
		Namespace: containingRelation.Namespace,
		Relation:  tuplesetRelation,
	}

	return crr.chunkedRedispatch(ctx, ci, reader, subjectsFilter, tuplesetRelationReference,
		func(ctx context.Context, ci cursorInformation, drsm dispatchableResourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, ci, containingRelation, drsm, rg, entrypoint, stream, req, dispatched)
		})
}

// redispatchOrReport checks if further redispatching is necessary for the found resource
// type. If not, and the found resource type+relation matches the target resource type+relation,
// the resource is reported to the parent stream.
func (crr *CursoredReachableResources) redispatchOrReport(
	ctx context.Context,
	ci cursorInformation,
	foundResourceType *core.RelationReference,
	foundResources dispatchableResourcesSubjectMap,
	rg *namespace.ReachabilityGraph,
	entrypoint namespace.ReachabilityEntrypoint,
	parentStream dispatch.ReachableResourcesStream,
	parentRequest ValidatedReachableResourcesRequest,
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

	return withSubsetInCursor(ci, "matching",
		func(currentOffset int, nextCursorWith afterResponseCursor) error {
			if !hasResourceEntrypoints {
				// If the found resource matches the target resource type and relation, yield the resource.
				if foundResourceType.Namespace == parentRequest.ResourceRelation.Namespace && foundResourceType.Relation == parentRequest.ResourceRelation.Relation {
					resources := foundResources.asReachableResources(entrypoint.IsDirectResult())
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

					for index, resource := range offsetted {
						okay, done := ci.limits.prepareForPublishing()
						defer done()

						if !okay {
							return nil
						}

						err := parentStream.Publish(&v1.DispatchReachableResourcesResponse{
							Resource:            resource,
							Metadata:            emptyMetadata,
							AfterResponseCursor: nextCursorWith(currentOffset + index + 1),
						})
						if err != nil {
							return err
						}
					}
					return nil
				}
			}
			return nil
		}, func(ci cursorInformation) error {
			if !hasResourceEntrypoints {
				return nil
			}

			stream := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
				Stream: parentStream,
				Ctx:    ctx,
				Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, bool, error) {
					// If the context has been closed, nothing more to do.
					select {
					case <-ctx.Done():
						return nil, false, ctx.Err()

					default:
					}

					// If we've exhausted the limit of resources to be returned, nothing more to do.
					if ci.limits.hasExhaustedLimit() {
						return nil, false, nil
					}

					// Map the found resources via the subject+resources used for dispatching, to determine
					// if any need to be made conditional due to caveats.
					mappedResource, err := foundResources.mapFoundResource(result.Resource, entrypoint.IsDirectResult())
					if err != nil {
						return nil, false, err
					}

					okay, done := ci.limits.prepareForPublishing()
					defer done()

					if !okay {
						return nil, false, nil
					}

					// The cursor for the response is that of the parent response + the cursor from the result itself.
					afterResponseCursor, err := combineCursors(
						ci.responsePartialCursor(),
						result.AfterResponseCursor,
					)
					if err != nil {
						return nil, false, err
					}

					resp := &v1.DispatchReachableResourcesResponse{
						Resource:            mappedResource,
						Metadata:            addCallToResponseMetadata(result.Metadata),
						AfterResponseCursor: afterResponseCursor,
					}
					return resp, true, nil
				},
			}

			// The new subject type for dispatching was the found type of the *resource*.
			newSubjectType := foundResourceType

			// To avoid duplicate work, remove any subjects already dispatched.
			filteredSubjectIDs := foundResources.filterSubjectIDsToDispatch(dispatched, newSubjectType)
			if len(filteredSubjectIDs) == 0 {
				return nil
			}

			// Dispatch the found resources as the subjects for the next call, to continue the
			// resolution.
			return crr.d.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: parentRequest.ResourceRelation,
				SubjectRelation:  newSubjectType,
				SubjectIds:       filteredSubjectIDs,
				Metadata: &v1.ResolverMeta{
					AtRevision:     parentRequest.Revision.String(),
					DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
				},
				OptionalCursor: ci.currentCursor,
				OptionalLimit:  ci.limits.currentLimit,
			}, stream)
		})
}

package graph

import (
	"context"
	"fmt"

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

// NewConcurrentReachableResources creates an instance of ConcurrentReachableResources.
func NewConcurrentReachableResources(d dispatch.ReachableResources, concurrencyLimit uint16) *ConcurrentReachableResources {
	return &ConcurrentReachableResources{d, concurrencyLimit}
}

// ConcurrentReachableResources exposes a method to perform ReachableResources requests, and
// delegates subproblems to the provided dispatch.ReachableResources instance.
type ConcurrentReachableResources struct {
	d                dispatch.ReachableResources
	concurrencyLimit uint16
}

// ValidatedReachableResourcesRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedReachableResourcesRequest struct {
	*v1.DispatchReachableResourcesRequest
	Revision datastore.Revision
}

func (crr *ConcurrentReachableResources) ReachableResources(
	req ValidatedReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
) error {
	ctx := stream.Context()
	dispatched := &syncONRSet{}

	if len(req.SubjectIds) == 0 {
		return fmt.Errorf("no subjects ids given to reachable resources dispatch")
	}

	// If the resource type matches the subject type, yield directly as a one-to-one result
	// for each subjectID.
	if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
		req.SubjectRelation.Relation == req.ResourceRelation.Relation {
		resources := make([]*v1.ReachableResource, 0, len(req.SubjectIds))
		for _, subjectID := range req.SubjectIds {
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    subjectID,
				ResultStatus:  v1.ReachableResource_HAS_PERMISSION,
				ForSubjectIds: []string{subjectID},
			})
		}

		err := stream.Publish(&v1.DispatchReachableResourcesResponse{
			Resources: resources,
			Metadata:  emptyMetadata,
		})
		if err != nil {
			return err
		}
	}

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

	t := NewTaskRunner(ctx, crr.concurrencyLimit)

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	for _, entrypoint := range entrypoints {
		switch entrypoint.EntrypointKind() {
		case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
			err := crr.lookupRelationEntrypoint(ctx, t, entrypoint, rg, reader, req, stream, dispatched)
			if err != nil {
				return err
			}

		case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
			containingRelation := entrypoint.ContainingRelationOrPermission()
			rewrittenSubjectRelation := &core.RelationReference{
				Namespace: containingRelation.Namespace,
				Relation:  containingRelation.Relation,
			}

			rsm := subjectIDsToResourcesMap(rewrittenSubjectRelation, req.SubjectIds)
			drsm := rsm.asReadOnly()

			err := crr.redispatchOrReport(
				ctx,
				t,
				rewrittenSubjectRelation,
				drsm,
				rg,
				entrypoint,
				stream,
				req,
				dispatched,
			)
			if err != nil {
				return err
			}

		case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
			err := crr.lookupTTUEntrypoint(ctx, t, entrypoint, rg, reader, req, stream, dispatched)
			if err != nil {
				return err
			}

		default:
			return spiceerrors.MustBugf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind())
		}
	}

	return t.Wait()
}

func (crr *ConcurrentReachableResources) lookupRelationEntrypoint(
	ctx context.Context,
	t *TaskRunner,
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
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: subjectIds,
		RelationFilter: datastore.SubjectRelationFilter{
			NonEllipsisRelation: req.SubjectRelation.Relation,
		},
	}

	crr.scheduleChunkedRedispatch(t, reader, subjectsFilter, relationReference,
		func(ctx context.Context, drsm dispatchableResourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, t, relationReference, drsm, rg, entrypoint, stream, req, dispatched)
		})
	return nil
}

func min(a, b int) int {
	if b < a {
		return b
	}
	return a
}

func (crr *ConcurrentReachableResources) scheduleChunkedRedispatch(
	t *TaskRunner,
	reader datastore.Reader,
	subjectsFilter datastore.SubjectsFilter,
	resourceType *core.RelationReference,
	handler func(ctx context.Context, resources dispatchableResourcesSubjectMap) error,
) {
	t.Schedule(func(ctx context.Context) error {
		toBeHandled := make([]resourcesSubjectMap, 0)
		it, err := reader.ReverseQueryRelationships(
			ctx,
			subjectsFilter,
			options.WithResRelation(&options.ResourceRelation{
				Namespace: resourceType.Namespace,
				Relation:  resourceType.Relation,
			}),
		)
		if err != nil {
			return err
		}
		defer it.Close()

		rsm := newResourcesSubjectMap(resourceType)
		chunkIndex := 0
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			chunkSize := progressiveDispatchChunkSizes[min(chunkIndex, len(progressiveDispatchChunkSizes)-1)]
			if it.Err() != nil {
				return it.Err()
			}

			err := rsm.addRelationship(tpl)
			if err != nil {
				return err
			}

			if rsm.len() == int(chunkSize) {
				chunkIndex++
				toBeHandled = append(toBeHandled, rsm)
				rsm = newResourcesSubjectMap(resourceType)
			}
		}
		it.Close()

		if rsm.len() > 0 {
			if rsm.len() > int(datastore.FilterMaximumIDCount) {
				return fmt.Errorf("found reachableresources chunk in excess of expected max size")
			}

			toBeHandled = append(toBeHandled, rsm)
		}

		for _, rsmToHandle := range toBeHandled {
			err := handler(ctx, rsmToHandle.asReadOnly())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (crr *ConcurrentReachableResources) lookupTTUEntrypoint(ctx context.Context,
	t *TaskRunner,
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

	// Determine the subject relation(s) for which to search. Note that we need to do so
	// for both `...` as well as the subject's defined relation, as either is applicable in
	// the tupleset (the relation is ignored when following the arrow).
	relationFilter := datastore.SubjectRelationFilter{}

	isEllipsisAllowed, err := ttuTypeSystem.IsAllowedDirectRelation(tuplesetRelation, req.SubjectRelation.Namespace, tuple.Ellipsis)
	if err != nil {
		return err
	}
	if isEllipsisAllowed == namespace.DirectRelationValid {
		relationFilter = relationFilter.WithEllipsisRelation()
	}

	isDirectAllowed, err := ttuTypeSystem.IsAllowedDirectRelation(tuplesetRelation, req.SubjectRelation.Namespace, req.SubjectRelation.Relation)
	if err != nil {
		return err
	}
	if isDirectAllowed == namespace.DirectRelationValid {
		relationFilter = relationFilter.WithNonEllipsisRelation(req.SubjectRelation.Relation)
	}

	if relationFilter.IsEmpty() {
		return nil
	}

	// Search for the resolved subjects in the tupleset of the TTU.
	subjectsFilter := datastore.SubjectsFilter{
		SubjectType:        req.SubjectRelation.Namespace,
		OptionalSubjectIds: req.SubjectIds,
		RelationFilter:     relationFilter,
	}

	tuplesetRelationReference := &core.RelationReference{
		Namespace: containingRelation.Namespace,
		Relation:  tuplesetRelation,
	}

	crr.scheduleChunkedRedispatch(t, reader, subjectsFilter, tuplesetRelationReference,
		func(ctx context.Context, drsm dispatchableResourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, t, containingRelation, drsm, rg, entrypoint, stream, req, dispatched)
		})
	return nil
}

// redispatchOrReport checks if further redispatching is necessary for the found resource
// type. If not, and the found resource type+relation matches the target resource type+relation,
// the resource is reported to the parent stream.
func (crr *ConcurrentReachableResources) redispatchOrReport(
	ctx context.Context,
	t *TaskRunner,
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

	// If there are no entrypoints, then no further dispatch is necessary.
	if !hasResourceEntrypoints {
		// If the found resource matches the target resource type and relation, yield the resource.
		if foundResourceType.Namespace == parentRequest.ResourceRelation.Namespace &&
			foundResourceType.Relation == parentRequest.ResourceRelation.Relation {
			return parentStream.Publish(&v1.DispatchReachableResourcesResponse{
				Resources: foundResources.asReachableResources(entrypoint.IsDirectResult()),
				Metadata:  emptyMetadata,
			})
		}

		// Otherwise, we're done.
		return nil
	}

	// Otherwise, redispatch.
	t.Schedule(func(ctx context.Context) error {
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

				// Map the found resources via the subject+resources used for dispatching, to determine
				// if any need to be made conditional due to caveats.
				mapped, err := foundResources.mapFoundResources(result.Resources, entrypoint.IsDirectResult())
				if err != nil {
					return nil, false, err
				}

				return &v1.DispatchReachableResourcesResponse{
					Resources: mapped,
					Metadata:  addCallToResponseMetadata(result.Metadata),
				}, true, nil
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
		}, stream)
	})
	return nil
}

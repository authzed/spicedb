package graph

import (
	"context"
	"fmt"
	"sync"

	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
	Revision decimal.Decimal
}

type syncONRSet struct {
	items sync.Map
}

func (s *syncONRSet) Add(onr *core.ObjectAndRelation) bool {
	key := tuple.StringONR(onr)
	_, existed := s.items.LoadOrStore(key, struct{}{})
	return !existed
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

	// If the resource type matches the subject type, yield directly.
	if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
		req.SubjectRelation.Relation == req.ResourceRelation.Relation {
		err := stream.Publish(&v1.DispatchReachableResourcesResponse{
			Resource: &v1.ReachableResource{
				ResourceIds:  req.SubjectIds,
				ResultStatus: v1.ReachableResource_HAS_PERMISSION,
			},
			Metadata: emptyMetadata,
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

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, subCtx := errgroup.WithContext(cancelCtx)
	g.SetLimit(int(crr.concurrencyLimit))

	// For each entrypoint, load the necessary data and re-dispatch if a subproblem was found.
	for _, entrypoint := range entrypoints {
		switch entrypoint.EntrypointKind() {
		case core.ReachabilityEntrypoint_RELATION_ENTRYPOINT:
			err := crr.lookupRelationEntrypoint(subCtx, entrypoint, rg, g, reader, req, stream, dispatched)
			if err != nil {
				return err
			}

		case core.ReachabilityEntrypoint_COMPUTED_USERSET_ENTRYPOINT:
			containingRelation := entrypoint.ContainingRelationOrPermission()
			rewrittenSubjectRelation := &core.RelationReference{
				Namespace: containingRelation.Namespace,
				Relation:  containingRelation.Relation,
			}

			err := crr.redispatchOrReport(subCtx, rewrittenSubjectRelation, req.SubjectIds, rg, g, entrypoint, stream, req, dispatched)
			if err != nil {
				return err
			}

		case core.ReachabilityEntrypoint_TUPLESET_TO_USERSET_ENTRYPOINT:
			err := crr.lookupTTUEntrypoint(subCtx, entrypoint, rg, g, reader, req, stream, dispatched)
			if err != nil {
				return err
			}

		default:
			panic(fmt.Sprintf("Unknown kind of entrypoint: %v", entrypoint.EntrypointKind()))
		}
	}

	return g.Wait()
}

func (crr *ConcurrentReachableResources) lookupRelationEntrypoint(ctx context.Context,
	entrypoint namespace.ReachabilityEntrypoint,
	rg *namespace.ReachabilityGraph,
	g *errgroup.Group,
	reader datastore.Reader,
	req ValidatedReachableResourcesRequest,
	stream dispatch.ReachableResourcesStream,
	dispatched *syncONRSet,
) error {
	relationReference := entrypoint.DirectRelation()
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
		SubjectType: req.SubjectRelation.Namespace,
		RelationFilter: datastore.SubjectRelationFilter{
			NonEllipsisRelation: req.SubjectRelation.Relation,
		},
		OptionalSubjectIds: subjectIds,
	}

	// Fire off a query lookup in parallel.
	g.Go(func() error {
		it, err := reader.ReverseQueryRelationships(
			ctx,
			subjectsFilter,
			options.WithResRelation(&options.ResourceRelation{
				Namespace: relationReference.Namespace,
				Relation:  relationReference.Relation,
			}),
		)
		if err != nil {
			return err
		}
		defer it.Close()

		return crr.chunkedRedispatch(it, func(resourceIdsFound []string) error {
			return crr.redispatchOrReport(ctx, &core.RelationReference{
				Namespace: relationReference.Namespace,
				Relation:  relationReference.Relation,
			}, resourceIdsFound, rg, g, entrypoint, stream, req, dispatched)
		})
	})

	return nil
}

func min(a, b int) int {
	if b < a {
		return b
	}
	return a
}

func (crr *ConcurrentReachableResources) chunkedRedispatch(it datastore.RelationshipIterator, handler func(resourceIdsFound []string) error) error {
	for chunkIndex := 0; /* until done with all relationships */ true; chunkIndex++ {
		chunkSize := progressiveDispatchChunkSizes[min(chunkIndex, len(progressiveDispatchChunkSizes)-1)]
		resourceIdsFound := make([]string, 0, chunkSize)
		for i := 0; i < chunkSize; i++ {
			tpl := it.Next()
			if it.Err() != nil {
				return it.Err()
			}

			if tpl == nil {
				break
			}

			resourceIdsFound = append(resourceIdsFound, tpl.ResourceAndRelation.ObjectId)
		}

		if len(resourceIdsFound) == 0 {
			return nil
		}

		err := handler(resourceIdsFound)
		if err != nil {
			return err
		}
	}
	return nil
}

func (crr *ConcurrentReachableResources) lookupTTUEntrypoint(ctx context.Context,
	entrypoint namespace.ReachabilityEntrypoint,
	rg *namespace.ReachabilityGraph,
	g *errgroup.Group,
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

	tuplesetRelation := entrypoint.TuplesetRelation()

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
		RelationFilter:     relationFilter,
		OptionalSubjectIds: req.SubjectIds,
	}

	// Fire off a query lookup in parallel.
	g.Go(func() error {
		it, err := reader.ReverseQueryRelationships(
			ctx,
			subjectsFilter,
			options.WithResRelation(&options.ResourceRelation{
				Namespace: containingRelation.Namespace,
				Relation:  tuplesetRelation,
			}),
		)
		if err != nil {
			return err
		}
		defer it.Close()

		return crr.chunkedRedispatch(it, func(resourceIdsFound []string) error {
			return crr.redispatchOrReport(ctx, containingRelation, resourceIdsFound, rg, g, entrypoint, stream, req, dispatched)
		})
	})

	return nil
}

// redispatchOrReport checks if further redispatching is necessary for the found resource
// type. If not, and the found resource type+relation matches the target resource type+relation,
// the resource is reported to the parent stream.
func (crr *ConcurrentReachableResources) redispatchOrReport(
	ctx context.Context,
	foundResourceType *core.RelationReference,
	foundResourceIds []string,
	rg *namespace.ReachabilityGraph,
	g *errgroup.Group,
	entrypoint namespace.ReachabilityEntrypoint,
	parentStream dispatch.ReachableResourcesStream,
	parentRequest ValidatedReachableResourcesRequest,
	dispatched *syncONRSet,
) error {
	toDispatchResourceIds := make([]string, 0, len(foundResourceIds))

	// Skip redispatching or checking for any resources already reported by this
	// pass.
	for _, resourceID := range foundResourceIds {
		if !dispatched.Add(&core.ObjectAndRelation{
			Namespace: foundResourceType.Namespace,
			ObjectId:  resourceID,
			Relation:  foundResourceType.Relation,
		}) {
			continue
		}

		toDispatchResourceIds = append(toDispatchResourceIds, resourceID)
	}

	if len(toDispatchResourceIds) == 0 {
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
			status := v1.ReachableResource_REQUIRES_CHECK
			if entrypoint.IsDirectResult() {
				status = v1.ReachableResource_HAS_PERMISSION
			}

			return parentStream.Publish(&v1.DispatchReachableResourcesResponse{
				Resource: &v1.ReachableResource{
					ResourceIds:  toDispatchResourceIds,
					ResultStatus: status,
				},
				Metadata: emptyMetadata,
			})
		}

		// Otherwise, we're done.
		return nil
	}

	// Otherwise, redispatch.
	g.Go(func() error {
		stream := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
			Stream: parentStream,
			Ctx:    ctx,
			Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, bool, error) {
				// If the entrypoint is not a direct result, then a check is required to determine
				// whether the resource actually has permission.
				status := result.Resource.ResultStatus
				if !entrypoint.IsDirectResult() {
					status = v1.ReachableResource_REQUIRES_CHECK
				}

				return &v1.DispatchReachableResourcesResponse{
					Resource: &v1.ReachableResource{
						ResourceIds:  result.Resource.ResourceIds,
						ResultStatus: status,
					},
					Metadata: addCallToResponseMetadata(result.Metadata),
				}, true, nil
			},
		}

		return crr.d.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
			ResourceRelation: parentRequest.ResourceRelation,
			SubjectRelation:  foundResourceType,
			SubjectIds:       toDispatchResourceIds,
			Metadata: &v1.ResolverMeta{
				AtRevision:     parentRequest.Revision.String(),
				DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
			},
		}, stream)
	})
	return nil
}

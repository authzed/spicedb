package graph

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
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

			err := crr.redispatchOrReport(
				subCtx,
				rewrittenSubjectRelation,
				subjectIDsToResourcesMap(rewrittenSubjectRelation, req.SubjectIds),
				rg,
				g,
				entrypoint,
				stream,
				req,
				dispatched,
			)
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
		return crr.chunkedRedispatch(ctx, reader, subjectsFilter, relationReference, func(rsm resourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, relationReference, rsm, rg, g, entrypoint, stream, req, dispatched)
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

func (crr *ConcurrentReachableResources) chunkedRedispatch(
	ctx context.Context,
	reader datastore.Reader,
	subjectsFilter datastore.SubjectsFilter,
	resourceType *core.RelationReference,
	handler func(resourcesFound resourcesSubjectMap) error,
) error {
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

	toBeHandled := make([]resourcesSubjectMap, 0)
	rsm := newResourcesSubjectMap(resourceType)
	chunkIndex := 0
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		chunkSize := progressiveDispatchChunkSizes[min(chunkIndex, len(progressiveDispatchChunkSizes)-1)]
		if it.Err() != nil {
			return it.Err()
		}

		rsm.addRelationship(tpl)
		if rsm.len() == chunkSize {
			chunkIndex++
			toBeHandled = append(toBeHandled, rsm)
		}
	}
	it.Close()

	if rsm.len() > 0 {
		toBeHandled = append(toBeHandled, rsm)
	}

	for _, rsmToHandle := range toBeHandled {
		err := handler(rsmToHandle)
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
		tuplesetRelationReference := &core.RelationReference{
			Namespace: containingRelation.Namespace,
			Relation:  tuplesetRelation,
		}

		return crr.chunkedRedispatch(ctx, reader, subjectsFilter, tuplesetRelationReference, func(rsm resourcesSubjectMap) error {
			return crr.redispatchOrReport(ctx, containingRelation, rsm, rg, g, entrypoint, stream, req, dispatched)
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
	foundResources resourcesSubjectMap,
	rg *namespace.ReachabilityGraph,
	g *errgroup.Group,
	entrypoint namespace.ReachabilityEntrypoint,
	parentStream dispatch.ReachableResourcesStream,
	parentRequest ValidatedReachableResourcesRequest,
	dispatched *syncONRSet,
) error {
	// Skip redispatching or checking for any resources already reported by this
	// pass.
	foundResources.filterForDispatch(dispatched)
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
	g.Go(func() error {
		stream := &dispatch.WrappedDispatchStream[*v1.DispatchReachableResourcesResponse]{
			Stream: parentStream,
			Ctx:    ctx,
			Processor: func(result *v1.DispatchReachableResourcesResponse) (*v1.DispatchReachableResourcesResponse, bool, error) {
				// Map the found resources via the subject+resources used for dispatching, to determine
				// if any need to be made conditional due to caveats.
				return &v1.DispatchReachableResourcesResponse{
					Resources: foundResources.mapFoundResources(result.Resources, entrypoint.IsDirectResult()),
					Metadata:  addCallToResponseMetadata(result.Metadata),
				}, true, nil
			},
		}

		// Dispatch the found resources as the subjects for the next call, to continue the
		// resolution.
		return crr.d.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
			ResourceRelation: parentRequest.ResourceRelation,
			SubjectRelation:  foundResourceType,
			SubjectIds:       foundResources.resourceIDs(),
			Metadata: &v1.ResolverMeta{
				AtRevision:     parentRequest.Revision.String(),
				DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
			},
		}, stream)
	})
	return nil
}

// resourcesSubjectMap is a multimap which tracks mappings from found resource IDs
// to the subject IDs (may be more than one) for each, as well as whether the mapping
// is conditional due to the use of a caveat on the relationship which formed the mapping.
type resourcesSubjectMap struct {
	resourceType         *core.RelationReference
	resourcesAndSubjects *util.MultiMap[string, subjectInfo]
}

type subjectInfo struct {
	subjectID  string
	isCaveated bool
}

func newResourcesSubjectMap(resourceType *core.RelationReference) resourcesSubjectMap {
	return resourcesSubjectMap{
		resourceType:         resourceType,
		resourcesAndSubjects: util.NewMultiMap[string, subjectInfo](),
	}
}

func subjectIDsToResourcesMap(resourceType *core.RelationReference, subjectIDs []string) resourcesSubjectMap {
	rsm := newResourcesSubjectMap(resourceType)
	for _, subjectID := range subjectIDs {
		rsm.addSubjectIDAsFoundResourceID(subjectID)
	}
	return rsm
}

func (rsm resourcesSubjectMap) addRelationship(rel *core.RelationTuple) {
	if rel.ResourceAndRelation.Namespace != rsm.resourceType.Namespace ||
		rel.ResourceAndRelation.Relation != rsm.resourceType.Relation {
		panic(fmt.Sprintf("invalid relationship for addRelationship. expected: %v, found: %v", rsm.resourceType, rel.ResourceAndRelation))
	}

	rsm.resourcesAndSubjects.Add(rel.ResourceAndRelation.ObjectId, subjectInfo{rel.Subject.ObjectId, rel.Caveat != nil && rel.Caveat.CaveatName != ""})
}

func (rsm resourcesSubjectMap) addSubjectIDAsFoundResourceID(subjectID string) {
	rsm.resourcesAndSubjects.Add(subjectID, subjectInfo{subjectID, false})
}

func (rsm resourcesSubjectMap) filterForDispatch(dispatched *syncONRSet) {
	for _, resourceID := range rsm.resourcesAndSubjects.Keys() {
		if !dispatched.Add(&core.ObjectAndRelation{
			Namespace: rsm.resourceType.Namespace,
			ObjectId:  resourceID,
			Relation:  rsm.resourceType.Relation,
		}) {
			rsm.resourcesAndSubjects.RemoveKey(resourceID)
			continue
		}
	}
}

func (rsm resourcesSubjectMap) isEmpty() bool {
	return rsm.resourcesAndSubjects.IsEmpty()
}

func (rsm resourcesSubjectMap) len() int {
	return rsm.resourcesAndSubjects.Len()
}

func (rsm resourcesSubjectMap) resourceIDs() []string {
	return rsm.resourcesAndSubjects.Keys()
}

func (rsm resourcesSubjectMap) asReachableResources(isDirectEntrypoint bool) []*v1.ReachableResource {
	resources := make([]*v1.ReachableResource, 0, rsm.resourcesAndSubjects.Len())
	for _, resourceID := range rsm.resourcesAndSubjects.Keys() {
		status := v1.ReachableResource_REQUIRES_CHECK
		if isDirectEntrypoint {
			status = v1.ReachableResource_HAS_PERMISSION
		}

		subjectInfos, _ := rsm.resourcesAndSubjects.Get(resourceID)
		subjectIDs := make([]string, 0, len(subjectInfos))
		allCaveated := true
		nonCaveatedSubjectIDs := make([]string, 0, len(subjectInfos))

		for _, info := range subjectInfos {
			subjectIDs = append(subjectIDs, info.subjectID)
			if !info.isCaveated {
				allCaveated = false
				nonCaveatedSubjectIDs = append(nonCaveatedSubjectIDs, info.subjectID)
			}
		}

		// If all the incoming edges are caveated, then the entire status has to be marked as a check
		// is required. Otherwise, if there is at least *one* non-caveated incoming edge, then we can
		// return the existing status as a short-circuit for those non-caveated found subjects.
		if allCaveated {
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    resourceID,
				ForSubjectIds: subjectIDs,
				ResultStatus:  v1.ReachableResource_REQUIRES_CHECK,
			})
		} else {
			resources = append(resources, &v1.ReachableResource{
				ResourceId:    resourceID,
				ForSubjectIds: nonCaveatedSubjectIDs,
				ResultStatus:  status,
			})
		}
	}
	return resources
}

func (rsm resourcesSubjectMap) mapFoundResources(foundResources []*v1.ReachableResource, isDirectEntrypoint bool) []*v1.ReachableResource {
	// For each found resource, lookup the associated entry(s) for the "ForSubjectIDs" and
	// check if *all* are conditional. If all are conditional, then mark the found resource
	// as conditional. Otherwise, mark it as non-conditional.
	resources := make([]*v1.ReachableResource, 0, len(foundResources))
	for _, foundResource := range foundResources {
		// If the found resource already requires a check, nothing more to do.
		if foundResource.ResultStatus == v1.ReachableResource_REQUIRES_CHECK {
			resources = append(resources, foundResource)
			continue
		}

		// Otherwise, see if a check is required either due to the parent entrypoint being indirect
		// *or* if the resource was reached via a caveated subject.
		status := v1.ReachableResource_REQUIRES_CHECK
		forSubjectIDs := foundResource.ForSubjectIds

		// If a direct entrypoint, then we need to do a caveat check.
		if isDirectEntrypoint {
			forSubjectIDs = make([]string, 0, len(foundResource.ForSubjectIds))

		outer:
			for _, forSubjectID := range foundResource.ForSubjectIds {
				infos, ok := rsm.resourcesAndSubjects.Get(forSubjectID)
				if !ok {
					panic("missing for subject ID")
				}

				for _, info := range infos {
					if !info.isCaveated {
						status = v1.ReachableResource_HAS_PERMISSION
						forSubjectIDs = []string{info.subjectID}
						break outer
					}
				}

				forSubjectIDs = append(forSubjectIDs, forSubjectID)
			}
		}

		resources = append(resources, &v1.ReachableResource{
			ResourceId:    foundResource.ResourceId,
			ForSubjectIds: forSubjectIDs,
			ResultStatus:  status,
		})
	}

	return resources
}

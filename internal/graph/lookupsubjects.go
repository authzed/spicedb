package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datasets"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidatedLookupSubjectsRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupSubjectsRequest struct {
	*v1.DispatchLookupSubjectsRequest
	Revision datastore.Revision
}

// NewConcurrentLookupSubjects creates an instance of ConcurrentLookupSubjects.
func NewConcurrentLookupSubjects(d dispatch.LookupSubjects, concurrencyLimit uint16, dispatchChunkSize uint16) *ConcurrentLookupSubjects {
	return &ConcurrentLookupSubjects{d, concurrencyLimit, dispatchChunkSize}
}

type ConcurrentLookupSubjects struct {
	d                 dispatch.LookupSubjects
	concurrencyLimit  uint16
	dispatchChunkSize uint16
}

func (cl *ConcurrentLookupSubjects) LookupSubjects(
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	ctx := stream.Context()

	if len(req.ResourceIds) == 0 {
		return fmt.Errorf("no resources ids given to lookupsubjects dispatch")
	}

	// If the resource type matches the subject type, yield directly.
	if req.SubjectRelation.Namespace == req.ResourceRelation.Namespace &&
		req.SubjectRelation.Relation == req.ResourceRelation.Relation {
		if err := stream.Publish(&v1.DispatchLookupSubjectsResponse{
			FoundSubjectsByResourceId: subjectsForConcreteIds(req.ResourceIds),
			Metadata:                  emptyMetadata,
		}); err != nil {
			return err
		}
	}

	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(req.Revision)
	_, relation, err := namespace.ReadNamespaceAndRelation(
		ctx,
		req.ResourceRelation.Namespace,
		req.ResourceRelation.Relation,
		reader)
	if err != nil {
		return err
	}

	if relation.UsersetRewrite == nil {
		// Direct lookup of subjects.
		return cl.lookupDirectSubjects(ctx, req, stream, relation, reader)
	}

	return cl.lookupViaRewrite(ctx, req, stream, relation.UsersetRewrite)
}

func subjectsForConcreteIds(subjectIds []string) map[string]*v1.FoundSubjects {
	foundSubjects := make(map[string]*v1.FoundSubjects, len(subjectIds))
	for _, subjectID := range subjectIds {
		foundSubjects[subjectID] = &v1.FoundSubjects{
			FoundSubjects: []*v1.FoundSubject{
				{
					SubjectId:        subjectID,
					CaveatExpression: nil, // Explicitly nil since this is a concrete found subject.
				},
			},
		}
	}
	return foundSubjects
}

func (cl *ConcurrentLookupSubjects) lookupDirectSubjects(
	ctx context.Context,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	_ *core.Relation,
	reader datastore.Reader,
) error {
	// TODO(jschorr): use type information to skip subject relations that cannot reach the subject type.

	toDispatchByType := datasets.NewSubjectByTypeSet()
	foundSubjectsByResourceID := datasets.NewSubjectSetByResourceID()
	relationshipsBySubjectONR := mapz.NewMultiMap[tuple.ObjectAndRelation, tuple.Relationship]()

	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     req.ResourceRelation.Namespace,
		OptionalResourceRelation: req.ResourceRelation.Relation,
		OptionalResourceIds:      req.ResourceIds,
	}, dsoptions.WithIncludeObjectData(req.IncludeObjectData))
	if err != nil {
		return err
	}

	for rel, err := range it {
		if err != nil {
			return err
		}

		if rel.Subject.ObjectType == req.SubjectRelation.Namespace &&
			rel.Subject.Relation == req.SubjectRelation.Relation {
			if err := foundSubjectsByResourceID.AddFromRelationship(rel); err != nil {
				return fmt.Errorf("failed to call AddFromRelationship in lookupDirectSubjects: %w", err)
			}
		}

		if rel.Subject.Relation != tuple.Ellipsis {
			err := toDispatchByType.AddSubjectOf(rel)
			if err != nil {
				return err
			}

			relationshipsBySubjectONR.Add(rel.Subject, rel)
		}
	}

	if !foundSubjectsByResourceID.IsEmpty() {
		if err := stream.Publish(&v1.DispatchLookupSubjectsResponse{
			FoundSubjectsByResourceId: foundSubjectsByResourceID.AsMap(),
			Metadata:                  emptyMetadata,
		}); err != nil {
			return err
		}
	}

	return cl.dispatchTo(ctx, req, toDispatchByType, relationshipsBySubjectONR, stream)
}

func (cl *ConcurrentLookupSubjects) lookupViaComputed(
	ctx context.Context,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	cu *core.ComputedUserset,
) error {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	if err := namespace.CheckNamespaceAndRelation(ctx, parentRequest.ResourceRelation.Namespace, cu.Relation, true, ds); err != nil {
		if errors.As(err, &namespace.RelationNotFoundError{}) {
			return nil
		}

		return err
	}

	stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
		Stream: parentStream,
		Ctx:    ctx,
		Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
			return &v1.DispatchLookupSubjectsResponse{
				FoundSubjectsByResourceId: result.FoundSubjectsByResourceId,
				Metadata:                  addCallToResponseMetadata(result.Metadata),
			}, true, nil
		},
	}

	return cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: &core.RelationReference{
			Namespace: parentRequest.ResourceRelation.Namespace,
			Relation:  cu.Relation,
		},
		ResourceIds:     parentRequest.ResourceIds,
		SubjectRelation: parentRequest.SubjectRelation,
		Metadata: &v1.ResolverMeta{
			AtRevision:     parentRequest.Revision.String(),
			DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
		}, IncludeObjectData: parentRequest.IncludeObjectData,
	}, stream)
}

type resourceDispatchTracker struct {
	ctx            context.Context
	cancelDispatch context.CancelFunc
	resourceID     string

	subjectsSet datasets.SubjectSet
	metadata    *v1.ResponseMeta

	isFirstUpdate bool
	wasCanceled   bool

	lock sync.Mutex
}

func lookupViaIntersectionTupleToUserset(
	ctx context.Context,
	cl *ConcurrentLookupSubjects,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	ttu *core.FunctionedTupleToUserset,
) error {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     parentRequest.ResourceRelation.Namespace,
		OptionalResourceRelation: ttu.GetTupleset().GetRelation(),
		OptionalResourceIds:      parentRequest.ResourceIds,
	}, dsoptions.WithIncludeObjectData(parentRequest.IncludeObjectData))
	if err != nil {
		return err
	}

	// TODO(jschorr): Find a means of doing this without dispatching per subject, per resource. Perhaps
	// there is a way we can still dispatch to all the subjects at once, and then intersect the results
	// afterwards.
	resourceDispatchTrackerByResourceID := make(map[string]*resourceDispatchTracker)

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	// For each found tuple, dispatch a lookup subjects request and collect its results.
	// We need to intersect between *all* the found subjects for each resource ID.
	var ttuCaveat *core.CaveatExpression
	taskrunner := taskrunner.NewPreloadedTaskRunner(cancelCtx, cl.concurrencyLimit, 1)
	for rel, err := range it {
		if err != nil {
			return err
		}

		// If the relationship has a caveat, add it to the overall TTU caveat. Since this is an intersection
		// of *all* branches, the caveat will be applied to all found subjects, so this is a safe approach.
		if rel.OptionalCaveat != nil {
			ttuCaveat = caveatAnd(ttuCaveat, wrapCaveat(rel.OptionalCaveat))
		}

		if err := namespace.CheckNamespaceAndRelation(ctx, rel.Subject.ObjectType, ttu.GetComputedUserset().Relation, false, ds); err != nil {
			if !errors.As(err, &namespace.RelationNotFoundError{}) {
				return err
			}

			continue
		}

		// Create a data structure to track the intersection of subjects for the particular resource. If the resource's subject set
		// ends up empty anywhere along the way, the dispatches for *that resource* will be canceled early.
		resourceID := rel.Resource.ObjectID
		dispatchInfoForResource, ok := resourceDispatchTrackerByResourceID[resourceID]
		if !ok {
			dispatchCtx, cancelDispatch := context.WithCancel(cancelCtx)
			dispatchInfoForResource = &resourceDispatchTracker{
				ctx:            dispatchCtx,
				cancelDispatch: cancelDispatch,
				resourceID:     resourceID,
				subjectsSet:    datasets.NewSubjectSet(),
				metadata:       emptyMetadata,
				isFirstUpdate:  true,
				lock:           sync.Mutex{},
			}
			resourceDispatchTrackerByResourceID[resourceID] = dispatchInfoForResource
		}

		rel := rel
		taskrunner.Add(func(ctx context.Context) error {
			// Collect all results for this branch of the resource ID.
			// TODO(jschorr): once LS has cursoring (and thus, ordering), we can move to not collecting everything up before intersecting
			// for this branch of the resource ID.
			collectingStream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](dispatchInfoForResource.ctx)
			err := cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: &core.RelationReference{
					Namespace: rel.Subject.ObjectType,
					Relation:  ttu.GetComputedUserset().Relation,
				},
				ResourceIds:     []string{rel.Subject.ObjectID},
				SubjectRelation: parentRequest.SubjectRelation,
				Metadata: &v1.ResolverMeta{
					AtRevision:     parentRequest.Revision.String(),
					DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
				}, IncludeObjectData: parentRequest.IncludeObjectData,
			}, collectingStream)
			if err != nil {
				// Check if the dispatches for the resource were canceled, and if so, return nil to stop the task.
				dispatchInfoForResource.lock.Lock()
				wasCanceled := dispatchInfoForResource.wasCanceled
				dispatchInfoForResource.lock.Unlock()

				if wasCanceled {
					if errors.Is(err, context.Canceled) {
						return nil
					}

					errStatus, ok := status.FromError(err)
					if ok && errStatus.Code() == codes.Canceled {
						return nil
					}
				}

				return err
			}

			// Collect the results into a subject set.
			results := datasets.NewSubjectSet()
			collectedMetadata := emptyMetadata
			for _, result := range collectingStream.Results() {
				collectedMetadata = combineResponseMetadata(ctx, collectedMetadata, result.Metadata)
				for _, foundSubjects := range result.FoundSubjectsByResourceId {
					if err := results.UnionWith(foundSubjects.FoundSubjects); err != nil {
						return fmt.Errorf("failed to UnionWith under lookupSubjectsIntersection: %w", err)
					}
				}
			}

			dispatchInfoForResource.lock.Lock()
			defer dispatchInfoForResource.lock.Unlock()

			dispatchInfoForResource.metadata = combineResponseMetadata(ctx, dispatchInfoForResource.metadata, collectedMetadata)

			// If the first update for the resource, set the subjects set to the results.
			if dispatchInfoForResource.isFirstUpdate {
				dispatchInfoForResource.isFirstUpdate = false
				dispatchInfoForResource.subjectsSet = results
			} else {
				// Otherwise, intersect the results with the existing subjects set.
				err := dispatchInfoForResource.subjectsSet.IntersectionDifference(results)
				if err != nil {
					return err
				}
			}

			// If the subjects set is empty, cancel the dispatch for any further results for this resource ID.
			if dispatchInfoForResource.subjectsSet.IsEmpty() {
				dispatchInfoForResource.wasCanceled = true
				dispatchInfoForResource.cancelDispatch()
			}

			return nil
		})
	}

	// Wait for all dispatched operations to complete.
	if err := taskrunner.StartAndWait(); err != nil {
		return err
	}

	// For each resource ID, intersect the found subjects from each stream.
	metadata := emptyMetadata
	currentSubjectsByResourceID := map[string]*v1.FoundSubjects{}

	for incomingResourceID, tracker := range resourceDispatchTrackerByResourceID {
		currentSubjects := tracker.subjectsSet
		currentSubjects = currentSubjects.WithParentCaveatExpression(ttuCaveat)
		currentSubjectsByResourceID[incomingResourceID] = currentSubjects.AsFoundSubjects()

		metadata = combineResponseMetadata(ctx, metadata, tracker.metadata)
	}

	return parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: currentSubjectsByResourceID,
		Metadata:                  metadata,
	})
}

func lookupViaTupleToUserset[T relation](
	ctx context.Context,
	cl *ConcurrentLookupSubjects,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	ttu ttu[T],
) error {
	toDispatchByTuplesetType := datasets.NewSubjectByTypeSet()
	relationshipsBySubjectONR := mapz.NewMultiMap[tuple.ObjectAndRelation, tuple.Relationship]()

	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     parentRequest.ResourceRelation.Namespace,
		OptionalResourceRelation: ttu.GetTupleset().GetRelation(),
		OptionalResourceIds:      parentRequest.ResourceIds,
	}, dsoptions.WithIncludeObjectData(parentRequest.IncludeObjectData))
	if err != nil {
		return err
	}

	for rel, err := range it {
		if err != nil {
			return err
		}

		// Add the subject to be dispatched.
		err := toDispatchByTuplesetType.AddSubjectOf(rel)
		if err != nil {
			return err
		}

		// Add the *rewritten* subject to the relationships multimap for mapping back to the associated
		// relationship, as we will be mapping from the computed relation, not the tupleset relation.
		relationshipsBySubjectONR.Add(tuple.ONR(rel.Subject.ObjectType, rel.Subject.ObjectID, ttu.GetComputedUserset().Relation), rel)
	}

	// Map the found subject types by the computed userset relation, so that we dispatch to it.
	toDispatchByComputedRelationType, err := toDispatchByTuplesetType.Map(func(resourceType *core.RelationReference) (*core.RelationReference, error) {
		if err := namespace.CheckNamespaceAndRelation(ctx, resourceType.Namespace, ttu.GetComputedUserset().Relation, false, ds); err != nil {
			if errors.As(err, &namespace.RelationNotFoundError{}) {
				return nil, nil
			}

			return nil, err
		}

		return &core.RelationReference{
			Namespace: resourceType.Namespace,
			Relation:  ttu.GetComputedUserset().Relation,
		}, nil
	})
	if err != nil {
		return err
	}

	return cl.dispatchTo(ctx, parentRequest, toDispatchByComputedRelationType, relationshipsBySubjectONR, parentStream)
}

func (cl *ConcurrentLookupSubjects) lookupViaRewrite(
	ctx context.Context,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	usr *core.UsersetRewrite,
) error {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		log.Ctx(ctx).Trace().Msg("union")
		return cl.lookupSetOperation(ctx, req, rw.Union, newLookupSubjectsUnion(stream))
	case *core.UsersetRewrite_Intersection:
		log.Ctx(ctx).Trace().Msg("intersection")
		return cl.lookupSetOperation(ctx, req, rw.Intersection, newLookupSubjectsIntersection(stream))
	case *core.UsersetRewrite_Exclusion:
		log.Ctx(ctx).Trace().Msg("exclusion")
		return cl.lookupSetOperation(ctx, req, rw.Exclusion, newLookupSubjectsExclusion(stream))
	default:
		return fmt.Errorf("unknown kind of rewrite in lookup subjects")
	}
}

func (cl *ConcurrentLookupSubjects) lookupSetOperation(
	ctx context.Context,
	req ValidatedLookupSubjectsRequest,
	so *core.SetOperation,
	reducer lookupSubjectsReducer,
) error {
	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, subCtx := errgroup.WithContext(cancelCtx)
	g.SetLimit(int(cl.concurrencyLimit))

	for index, childOneof := range so.Child {
		stream := reducer.ForIndex(subCtx, index)

		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return errors.New("use of _this is unsupported; please rewrite your schema")

		case *core.SetOperation_Child_ComputedUserset:
			g.Go(func() error {
				return cl.lookupViaComputed(subCtx, req, stream, child.ComputedUserset)
			})

		case *core.SetOperation_Child_UsersetRewrite:
			g.Go(func() error {
				return cl.lookupViaRewrite(subCtx, req, stream, child.UsersetRewrite)
			})

		case *core.SetOperation_Child_TupleToUserset:
			g.Go(func() error {
				return lookupViaTupleToUserset(subCtx, cl, req, stream, child.TupleToUserset)
			})

		case *core.SetOperation_Child_FunctionedTupleToUserset:
			switch child.FunctionedTupleToUserset.Function {
			case core.FunctionedTupleToUserset_FUNCTION_ANY:
				g.Go(func() error {
					return lookupViaTupleToUserset(subCtx, cl, req, stream, child.FunctionedTupleToUserset)
				})

			case core.FunctionedTupleToUserset_FUNCTION_ALL:
				g.Go(func() error {
					return lookupViaIntersectionTupleToUserset(subCtx, cl, req, stream, child.FunctionedTupleToUserset)
				})

			default:
				return spiceerrors.MustBugf("unknown function in lookup subjects: %v", child.FunctionedTupleToUserset.Function)
			}

		case *core.SetOperation_Child_XNil:
			// Purposely do nothing.
			continue

		default:
			return spiceerrors.MustBugf("unknown set operation child `%T` in lookup subjects", child)
		}
	}

	// Wait for all dispatched operations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	return reducer.CompletedChildOperations(ctx)
}

func (cl *ConcurrentLookupSubjects) dispatchTo(
	ctx context.Context,
	parentRequest ValidatedLookupSubjectsRequest,
	toDispatchByType *datasets.SubjectByTypeSet,
	relationshipsBySubjectONR *mapz.MultiMap[tuple.ObjectAndRelation, tuple.Relationship],
	parentStream dispatch.LookupSubjectsStream,
) error {
	if toDispatchByType.IsEmpty() {
		return nil
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, subCtx := errgroup.WithContext(cancelCtx)
	g.SetLimit(int(cl.concurrencyLimit))

	toDispatchByType.ForEachType(func(resourceType *core.RelationReference, foundSubjects datasets.SubjectSet) {
		slice := foundSubjects.AsSlice()
		resourceIds := make([]string, 0, len(slice))
		for _, foundSubject := range slice {
			resourceIds = append(resourceIds, foundSubject.SubjectId)
		}

		stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
			Stream: parentStream,
			Ctx:    subCtx,
			Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
				// For any found subjects, map them through their associated starting resources, to apply any caveats that were
				// only those resources' relationships.
				//
				// For example, given relationships which formed the dispatch:
				//   - document:firstdoc#viewer@group:group1#member
				//   - document:firstdoc#viewer@group:group2#member[somecaveat]
				//
				//	And results:
				//	 - group1 => {user:tom, user:sarah}
				//   - group2 => {user:tom, user:fred}
				//
				//	This will produce:
				//	 - firstdoc => {user:tom, user:sarah, user:fred[somecaveat]}
				//
				mappedFoundSubjects := make(map[string]*v1.FoundSubjects)
				for childResourceID, foundSubjects := range result.FoundSubjectsByResourceId {
					subjectKey := tuple.ONR(resourceType.Namespace, childResourceID, resourceType.Relation)
					relationships, _ := relationshipsBySubjectONR.Get(subjectKey)
					if len(relationships) == 0 {
						return nil, false, fmt.Errorf("missing relationships for subject key %v; please report this error", subjectKey)
					}

					for _, relationship := range relationships {
						existing := mappedFoundSubjects[relationship.Resource.ObjectID]

						// If the relationship has no caveat, simply map the resource ID.
						if relationship.OptionalCaveat == nil {
							combined, err := combineFoundSubjects(existing, foundSubjects)
							if err != nil {
								return nil, false, fmt.Errorf("could not combine caveat-less subjects: %w", err)
							}
							mappedFoundSubjects[relationship.Resource.ObjectID] = combined
							continue
						}

						// Otherwise, apply the caveat to all found subjects for that resource and map to the resource ID.
						foundSubjectSet := datasets.NewSubjectSet()
						err := foundSubjectSet.UnionWith(foundSubjects.FoundSubjects)
						if err != nil {
							return nil, false, fmt.Errorf("could not combine subject sets: %w", err)
						}

						combined, err := combineFoundSubjects(
							existing,
							foundSubjectSet.WithParentCaveatExpression(wrapCaveat(relationship.OptionalCaveat)).AsFoundSubjects(),
						)
						if err != nil {
							return nil, false, fmt.Errorf("could not combine caveated subjects: %w", err)
						}

						mappedFoundSubjects[relationship.Resource.ObjectID] = combined
					}
				}

				return &v1.DispatchLookupSubjectsResponse{
					FoundSubjectsByResourceId: mappedFoundSubjects,
					Metadata:                  addCallToResponseMetadata(result.Metadata),
				}, true, nil
			},
		}

		// Dispatch the found subjects as the resources of the next step.
		slicez.ForEachChunk(resourceIds, cl.dispatchChunkSize, func(resourceIdChunk []string) {
			g.Go(func() error {
				return cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
					ResourceRelation: resourceType,
					ResourceIds:      resourceIdChunk,
					SubjectRelation:  parentRequest.SubjectRelation,
					Metadata: &v1.ResolverMeta{
						AtRevision:     parentRequest.Revision.String(),
						DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
					}, IncludeObjectData: parentRequest.IncludeObjectData,
				}, stream)
			})
		})
	})

	return g.Wait()
}

func combineFoundSubjects(existing *v1.FoundSubjects, toAdd *v1.FoundSubjects) (*v1.FoundSubjects, error) {
	if existing == nil {
		return toAdd, nil
	}

	if toAdd == nil {
		return nil, fmt.Errorf("toAdd FoundSubject cannot be nil")
	}

	return &v1.FoundSubjects{
		FoundSubjects: append(existing.FoundSubjects, toAdd.FoundSubjects...),
	}, nil
}

type lookupSubjectsReducer interface {
	ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream
	CompletedChildOperations(ctx context.Context) error
}

// Union
type lookupSubjectsUnion struct {
	parentStream dispatch.LookupSubjectsStream
	collectors   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]
}

func newLookupSubjectsUnion(parentStream dispatch.LookupSubjectsStream) *lookupSubjectsUnion {
	return &lookupSubjectsUnion{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
	}
}

func (lsu *lookupSubjectsUnion) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	lsu.collectors[setOperationIndex] = collector
	return collector
}

func (lsu *lookupSubjectsUnion) CompletedChildOperations(ctx context.Context) error {
	foundSubjects := datasets.NewSubjectSetByResourceID()
	metadata := emptyMetadata

	for index := 0; index < len(lsu.collectors); index++ {
		collector, ok := lsu.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(ctx, metadata, result.Metadata)
			if err := foundSubjects.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return fmt.Errorf("failed to UnionWith under lookupSubjectsUnion: %w", err)
			}
		}
	}

	if foundSubjects.IsEmpty() {
		return nil
	}

	return lsu.parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: foundSubjects.AsMap(),
		Metadata:                  metadata,
	})
}

// Intersection
type lookupSubjectsIntersection struct {
	parentStream dispatch.LookupSubjectsStream
	collectors   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]
}

func newLookupSubjectsIntersection(parentStream dispatch.LookupSubjectsStream) *lookupSubjectsIntersection {
	return &lookupSubjectsIntersection{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
	}
}

func (lsi *lookupSubjectsIntersection) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	lsi.collectors[setOperationIndex] = collector
	return collector
}

func (lsi *lookupSubjectsIntersection) CompletedChildOperations(ctx context.Context) error {
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(lsi.collectors); index++ {
		collector, ok := lsi.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(ctx, metadata, result.Metadata)
			if err := results.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return fmt.Errorf("failed to UnionWith under lookupSubjectsIntersection: %w", err)
			}
		}

		if index == 0 {
			foundSubjects = results
		} else {
			err := foundSubjects.IntersectionDifference(results)
			if err != nil {
				return err
			}

			if foundSubjects.IsEmpty() {
				return nil
			}
		}
	}

	return lsi.parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: foundSubjects.AsMap(),
		Metadata:                  metadata,
	})
}

// Exclusion
type lookupSubjectsExclusion struct {
	parentStream dispatch.LookupSubjectsStream
	collectors   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]
}

func newLookupSubjectsExclusion(parentStream dispatch.LookupSubjectsStream) *lookupSubjectsExclusion {
	return &lookupSubjectsExclusion{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
	}
}

func (lse *lookupSubjectsExclusion) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	lse.collectors[setOperationIndex] = collector
	return collector
}

func (lse *lookupSubjectsExclusion) CompletedChildOperations(ctx context.Context) error {
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(lse.collectors); index++ {
		collector := lse.collectors[index]
		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(ctx, metadata, result.Metadata)
			if err := results.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return fmt.Errorf("failed to UnionWith under lookupSubjectsExclusion: %w", err)
			}
		}

		if index == 0 {
			foundSubjects = results
		} else {
			foundSubjects.SubtractAll(results)
			if foundSubjects.IsEmpty() {
				return nil
			}
		}
	}

	return lse.parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: foundSubjects.AsMap(),
		Metadata:                  metadata,
	})
}

package graph

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datasets"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidatedLookupSubjectsRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupSubjectsRequest struct {
	*v1.DispatchLookupSubjectsRequest
	Revision datastore.Revision
}

// NewConcurrentLookupSubjects creates an instance of ConcurrentLookupSubjects.
func NewConcurrentLookupSubjects(d dispatch.LookupSubjects, concurrencyLimit uint16) *ConcurrentLookupSubjects {
	return &ConcurrentLookupSubjects{d, concurrencyLimit}
}

type ConcurrentLookupSubjects struct {
	d                dispatch.LookupSubjects
	concurrencyLimit uint16
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
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             req.ResourceRelation.Namespace,
		OptionalResourceRelation: req.ResourceRelation.Relation,
		OptionalResourceIds:      req.ResourceIds,
	})
	if err != nil {
		return err
	}
	defer it.Close()

	toDispatchByType := datasets.NewSubjectByTypeSet()
	foundSubjectsByResourceID := datasets.NewSubjectSetByResourceID()
	relationshipsBySubjectONR := mapz.NewMultiMap[string, *core.RelationTuple]()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return it.Err()
		}

		if tpl.Subject.Namespace == req.SubjectRelation.Namespace &&
			tpl.Subject.Relation == req.SubjectRelation.Relation {
			if err := foundSubjectsByResourceID.AddFromRelationship(tpl); err != nil {
				return fmt.Errorf("failed to call AddFromRelationship in lookupDirectSubjects: %w", err)
			}
		}

		if tpl.Subject.Relation != tuple.Ellipsis {
			err := toDispatchByType.AddSubjectOf(tpl)
			if err != nil {
				return err
			}

			relationshipsBySubjectONR.Add(tuple.StringONR(tpl.Subject), tpl)
		}
	}
	it.Close()

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
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
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
		},
	}, stream)
}

func (cl *ConcurrentLookupSubjects) lookupViaTupleToUserset(
	ctx context.Context,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	ttu *core.TupleToUserset,
) error {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             parentRequest.ResourceRelation.Namespace,
		OptionalResourceRelation: ttu.Tupleset.Relation,
		OptionalResourceIds:      parentRequest.ResourceIds,
	})
	if err != nil {
		return err
	}
	defer it.Close()

	toDispatchByTuplesetType := datasets.NewSubjectByTypeSet()
	relationshipsBySubjectONR := mapz.NewMultiMap[string, *core.RelationTuple]()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return it.Err()
		}

		// Add the subject to be dispatched.
		err := toDispatchByTuplesetType.AddSubjectOf(tpl)
		if err != nil {
			return err
		}

		// Add the *rewritten* subject to the relationships multimap for mapping back to the associated
		// relationship, as we will be mapping from the computed relation, not the tupleset relation.
		relationshipsBySubjectONR.Add(tuple.StringONR(&core.ObjectAndRelation{
			Namespace: tpl.Subject.Namespace,
			ObjectId:  tpl.Subject.ObjectId,
			Relation:  ttu.ComputedUserset.Relation,
		}), tpl)
	}
	it.Close()

	// Map the found subject types by the computed userset relation, so that we dispatch to it.
	toDispatchByComputedRelationType, err := toDispatchByTuplesetType.Map(func(resourceType *core.RelationReference) (*core.RelationReference, error) {
		if err := namespace.CheckNamespaceAndRelation(ctx, resourceType.Namespace, ttu.ComputedUserset.Relation, false, ds); err != nil {
			if errors.As(err, &namespace.ErrRelationNotFound{}) {
				return nil, nil
			}

			return nil, err
		}

		return &core.RelationReference{
			Namespace: resourceType.Namespace,
			Relation:  ttu.ComputedUserset.Relation,
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
				return cl.lookupViaTupleToUserset(subCtx, req, stream, child.TupleToUserset)
			})

		case *core.SetOperation_Child_XNil:
			// Purposely do nothing.
			continue

		default:
			return fmt.Errorf("unknown set operation child `%T` in expand", child)
		}
	}

	// Wait for all dispatched operations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	return reducer.CompletedChildOperations()
}

func (cl *ConcurrentLookupSubjects) dispatchTo(
	ctx context.Context,
	parentRequest ValidatedLookupSubjectsRequest,
	toDispatchByType *datasets.SubjectByTypeSet,
	relationshipsBySubjectONR *mapz.MultiMap[string, *core.RelationTuple],
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
					subjectKey := tuple.StringONR(&core.ObjectAndRelation{
						Namespace: resourceType.Namespace,
						ObjectId:  childResourceID,
						Relation:  resourceType.Relation,
					})

					relationships, _ := relationshipsBySubjectONR.Get(subjectKey)
					if len(relationships) == 0 {
						return nil, false, fmt.Errorf("missing relationships for subject key %v; please report this error", subjectKey)
					}

					for _, relationship := range relationships {
						existing := mappedFoundSubjects[relationship.ResourceAndRelation.ObjectId]

						// If the relationship has no caveat, simply map the resource ID.
						if relationship.GetCaveat() == nil {
							combined, err := combineFoundSubjects(existing, foundSubjects)
							if err != nil {
								return nil, false, fmt.Errorf("could not combine caveat-less subjects: %w", err)
							}
							mappedFoundSubjects[relationship.ResourceAndRelation.ObjectId] = combined
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
							foundSubjectSet.WithParentCaveatExpression(wrapCaveat(relationship.Caveat)).AsFoundSubjects(),
						)
						if err != nil {
							return nil, false, fmt.Errorf("could not combine caveated subjects: %w", err)
						}

						mappedFoundSubjects[relationship.ResourceAndRelation.ObjectId] = combined
					}
				}

				return &v1.DispatchLookupSubjectsResponse{
					FoundSubjectsByResourceId: mappedFoundSubjects,
					Metadata:                  addCallToResponseMetadata(result.Metadata),
				}, true, nil
			},
		}

		// Dispatch the found subjects as the resources of the next step.
		slicez.ForEachChunk(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) {
			g.Go(func() error {
				return cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
					ResourceRelation: resourceType,
					ResourceIds:      resourceIdChunk,
					SubjectRelation:  parentRequest.SubjectRelation,
					Metadata: &v1.ResolverMeta{
						AtRevision:     parentRequest.Revision.String(),
						DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
					},
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
	CompletedChildOperations() error
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

func (lsu *lookupSubjectsUnion) CompletedChildOperations() error {
	foundSubjects := datasets.NewSubjectSetByResourceID()
	metadata := emptyMetadata

	for index := 0; index < len(lsu.collectors); index++ {
		collector, ok := lsu.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
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

func (lsi *lookupSubjectsIntersection) CompletedChildOperations() error {
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(lsi.collectors); index++ {
		collector, ok := lsi.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
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

func (lse *lookupSubjectsExclusion) CompletedChildOperations() error {
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(lse.collectors); index++ {
		collector := lse.collectors[index]
		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
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

package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/util"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidatedLookupSubjectsRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupSubjectsRequest struct {
	*v1.DispatchLookupSubjectsRequest
	Revision decimal.Decimal
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
		err := stream.Publish(&v1.DispatchLookupSubjectsResponse{
			FoundSubjects: subjectsForIds(req.ResourceIds),
			Metadata:      emptyMetadata,
		})
		if err != nil {
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

func subjectsForIds(subjectIds []string) []*v1.FoundSubject {
	foundSubjects := make([]*v1.FoundSubject, 0, len(subjectIds))
	for _, subjectID := range subjectIds {
		foundSubjects = append(foundSubjects, &v1.FoundSubject{
			SubjectId: subjectID,
		})
	}
	return foundSubjects
}

func (cl *ConcurrentLookupSubjects) lookupDirectSubjects(
	ctx context.Context,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	relation *core.Relation,
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

	toDispatchByType := tuple.NewONRByTypeSet()
	var foundSubjectIds []string
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return it.Err()
		}

		if tpl.Subject.Namespace == req.SubjectRelation.Namespace &&
			tpl.Subject.Relation == req.SubjectRelation.Relation {
			foundSubjectIds = append(foundSubjectIds, tpl.Subject.ObjectId)
		}

		if tpl.Subject.Relation != tuple.Ellipsis {
			toDispatchByType.Add(tpl.Subject)
		}
	}

	if len(foundSubjectIds) > 0 {
		err := stream.Publish(&v1.DispatchLookupSubjectsResponse{
			FoundSubjects: subjectsForIds(foundSubjectIds),
			Metadata:      emptyMetadata,
		})
		if err != nil {
			return err
		}
	}

	return cl.dispatchTo(ctx, req, toDispatchByType, stream)
}

func (cl *ConcurrentLookupSubjects) lookupViaComputed(
	ctx context.Context,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	cu *core.ComputedUserset,
) error {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, parentRequest.ResourceRelation.Namespace, cu.Relation, true, ds)
	if err != nil {
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
				FoundSubjects: result.FoundSubjects,
				Metadata:      addCallToResponseMetadata(result.Metadata),
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

	toDispatchByTuplesetType := tuple.NewONRByTypeSet()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return it.Err()
		}

		toDispatchByTuplesetType.Add(tpl.Subject)
	}

	// Map the found subject types by the computed userset relation, so that we dispatch to it.
	toDispatchByComputedRelationType, err := toDispatchByTuplesetType.Map(func(resourceType *core.RelationReference) (*core.RelationReference, error) {
		err := namespace.CheckNamespaceAndRelation(ctx, resourceType.Namespace, ttu.ComputedUserset.Relation, false, ds)
		if err != nil {
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

	return cl.dispatchTo(ctx, parentRequest, toDispatchByComputedRelationType, parentStream)
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
	toDispatchByType *tuple.ONRByTypeSet,
	parentStream dispatch.LookupSubjectsStream,
) error {
	if toDispatchByType.IsEmpty() {
		return nil
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	g, subCtx := errgroup.WithContext(cancelCtx)
	g.SetLimit(int(cl.concurrencyLimit))

	stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
		Stream: parentStream,
		Ctx:    subCtx,
		Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
			return &v1.DispatchLookupSubjectsResponse{
				FoundSubjects: result.FoundSubjects,
				Metadata:      addCallToResponseMetadata(result.Metadata),
			}, true, nil
		},
	}

	toDispatchByType.ForEachType(func(resourceType *core.RelationReference, resourceIds []string) {
		util.ForEachChunk(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) {
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
	foundSubjects := util.NewSubjectSet()
	metadata := emptyMetadata

	for index := 0; index < len(lsu.collectors); index++ {
		collector, ok := lsu.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			foundSubjects.UnionWith(result.FoundSubjects)
		}
	}

	if foundSubjects.IsEmpty() {
		return nil
	}

	return lsu.parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjects: foundSubjects.AsSlice(),
		Metadata:      metadata,
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
	var foundSubjects util.SubjectSet
	metadata := emptyMetadata

	for index := 0; index < len(lsi.collectors); index++ {
		collector, ok := lsi.collectors[index]
		if !ok {
			return fmt.Errorf("missing collector for index %d", index)
		}

		results := util.NewSubjectSet()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			results.UnionWith(result.FoundSubjects)
		}

		if index == 0 {
			foundSubjects = results
		} else {
			foundSubjects.IntersectionDifference(results)
			if foundSubjects.IsEmpty() {
				return nil
			}
		}
	}

	return lsi.parentStream.Publish(&v1.DispatchLookupSubjectsResponse{
		FoundSubjects: foundSubjects.AsSlice(),
		Metadata:      metadata,
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
	var foundSubjects util.SubjectSet
	metadata := emptyMetadata

	for index := 0; index < len(lse.collectors); index++ {
		collector := lse.collectors[index]
		results := util.NewSubjectSet()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			results.UnionWith(result.FoundSubjects)
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
		FoundSubjects: foundSubjects.AsSlice(),
		Metadata:      metadata,
	})
}

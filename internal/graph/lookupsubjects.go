package graph

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datasets"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/typesystem"
)

// lsDispatchVersion defines the "version" of this dispatcher. Must be incremented
// anytime an incompatible change is made to the dispatcher itself or its cursor
// production.
const lsDispatchVersion = 1

// CursorForFoundSubjectID returns an updated version of the afterResponseCursor (which must have been created
// by this dispatcher), but with the specified subjectID as the starting point.
func CursorForFoundSubjectID(subjectID string, afterResponseCursor *v1.Cursor) (*v1.Cursor, error) {
	if afterResponseCursor == nil {
		return &v1.Cursor{
			DispatchVersion: lsDispatchVersion,
			Sections:        []string{subjectID},
		}, nil
	}

	if len(afterResponseCursor.Sections) != 1 {
		return nil, spiceerrors.MustBugf("given an invalid afterResponseCursor (wrong number of sections)")
	}

	return &v1.Cursor{
		DispatchVersion: lsDispatchVersion,
		Sections:        []string{subjectID},
	}, nil
}

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

// ConcurrentLookupSubjects performs the concurrent lookup subjects operation.
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

	limits := newLimitTracker(req.OptionalLimit)
	ci, err := newCursorInformation(req.OptionalCursor, limits, lsDispatchVersion)
	if err != nil {
		return err
	}

	// Run both "branches" in parallel and union together to respect the cursors and limits.
	return runInParallel(ctx, ci, stream, cl.concurrencyLimit,
		unionOperation{
			callback: func(ctx context.Context, cstream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error {
				return cl.yieldMatchingResources(ctx, ci.withClonedLimits(), req, cstream)
			},
			runIf: req.SubjectRelation.Namespace == req.ResourceRelation.Namespace && req.SubjectRelation.Relation == req.ResourceRelation.Relation,
		},
		unionOperation{
			callback: func(ctx context.Context, cstream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error {
				return cl.yieldRelationSubjects(ctx, ci.withClonedLimits(), req, cstream, concurrencyLimit)
			},
			runIf: true,
		},
	)
}

// yieldMatchingResources yields the current resource IDs iff the resource matches the target
// subject.
func (cl *ConcurrentLookupSubjects) yieldMatchingResources(
	_ context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
) error {
	if req.SubjectRelation.Namespace != req.ResourceRelation.Namespace ||
		req.SubjectRelation.Relation != req.ResourceRelation.Relation {
		return nil
	}

	subjectsMap, err := subjectsForConcreteIds(req.ResourceIds, ci)
	if err != nil {
		return err
	}

	return publishSubjects(stream, ci, subjectsMap, emptyMetadata)
}

// yieldRelationSubjects walks the relation, performing lookup subjects on the relation's data or
// computed rewrite.
func (cl *ConcurrentLookupSubjects) yieldRelationSubjects(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	concurrencyLimit uint16,
) error {
	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(req.Revision)

	_, validatedTS, err := typesystem.ReadNamespaceAndTypes(ctx, req.ResourceRelation.Namespace, reader)
	if err != nil {
		return err
	}

	relation, err := validatedTS.GetRelationOrError(req.ResourceRelation.Relation)
	if err != nil {
		return err
	}

	if relation.UsersetRewrite == nil {
		// As there is no rewrite here, perform direct lookup of subjects on the relation.
		return cl.lookupDirectSubjects(ctx, ci, req, stream, validatedTS, reader, concurrencyLimit)
	}

	return cl.lookupViaRewrite(ctx, ci, req, stream, relation.UsersetRewrite, concurrencyLimit)
}

// subjectsForConcreteIds returns a FoundSubjects map for the given *concrete* subject IDs, filtered by the cursor (if applicable).
func subjectsForConcreteIds(subjectIDs []string, ci cursorInformation) (map[string]*v1.FoundSubjects, error) {
	// If the after subject ID is the wildcard, then no concrete subjects should be returned.
	afterSubjectID, _ := ci.headSectionValue()
	if afterSubjectID == tuple.PublicWildcard {
		return nil, nil
	}

	foundSubjects := make(map[string]*v1.FoundSubjects, len(subjectIDs))
	for _, subjectID := range subjectIDs {
		if afterSubjectID != "" && subjectID <= afterSubjectID {
			continue
		}

		foundSubjects[subjectID] = &v1.FoundSubjects{
			FoundSubjects: []*v1.FoundSubject{
				{
					SubjectId:        subjectID,
					CaveatExpression: nil, // Explicitly nil since this is a concrete found subject.
				},
			},
		}
	}
	return foundSubjects, nil
}

// lookupDirectSubjects performs lookup of subjects directly on a relation.
func (cl *ConcurrentLookupSubjects) lookupDirectSubjects(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	validatedTS *typesystem.ValidatedNamespaceTypeSystem,
	reader datastore.Reader,
	concurrencyLimit uint16,
) error {
	// Check if the direct subject can be found on this relation and, if so, query for then.
	directAllowed, err := validatedTS.IsAllowedDirectRelation(req.ResourceRelation.Relation, req.SubjectRelation.Namespace, req.SubjectRelation.Relation)
	if err != nil {
		return err
	}

	hasIndirectSubjects, err := validatedTS.HasIndirectSubjects(req.ResourceRelation.Relation)
	if err != nil {
		return err
	}

	wildcardAllowed, err := validatedTS.IsAllowedPublicNamespace(req.ResourceRelation.Relation, req.SubjectRelation.Namespace)
	if err != nil {
		return err
	}

	return runInParallel(ctx, ci, stream, concurrencyLimit,
		// Direct subjects found on the relation.
		unionOperation{
			callback: func(ctx context.Context, cstream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error {
				return cl.lookupDirectSubjectsForRelation(ctx, ci.withClonedLimits(), req, cstream, validatedTS, reader)
			},
			runIf: directAllowed == typesystem.DirectRelationValid,
		},

		// Wildcard on the relation.
		unionOperation{
			callback: func(ctx context.Context, cstream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error {
				return cl.lookupWildcardSubjectForRelation(ctx, ci.withClonedLimits(), req, cstream, validatedTS, reader)
			},

			// Wildcards are only applicable on ellipsis subjects
			runIf: req.SubjectRelation.Relation == tuple.Ellipsis && wildcardAllowed == typesystem.PublicSubjectAllowed,
		},

		// Dispatching over indirect subjects on the relation.
		unionOperation{
			callback: func(ctx context.Context, cstream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error {
				return cl.dispatchIndirectSubjectsForRelation(ctx, ci.withClonedLimits(), req, cstream, reader)
			},
			runIf: hasIndirectSubjects,
		},
	)
}

// lookupDirectSubjectsForRelation finds all directly matching subjects on the request's relation, if applicable.
func (cl *ConcurrentLookupSubjects) lookupDirectSubjectsForRelation(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	validatedTS *typesystem.ValidatedNamespaceTypeSystem,
	reader datastore.Reader,
) error {
	// Check if the direct subject can be found on this relation and, if so, query for then.
	directAllowed, err := validatedTS.IsAllowedDirectRelation(req.ResourceRelation.Relation, req.SubjectRelation.Namespace, req.SubjectRelation.Relation)
	if err != nil {
		return err
	}

	if directAllowed == typesystem.DirectRelationNotValid {
		return nil
	}

	var afterCursor options.Cursor
	afterSubjectID, _ := ci.headSectionValue()

	// If the cursor specifies the wildcard, then skip all further non-wildcard results.
	if afterSubjectID == tuple.PublicWildcard {
		return nil
	}

	if afterSubjectID != "" {
		afterCursor = &core.RelationTuple{
			// NOTE: since we fully specify the resource below, the resource should be ignored in this cursor.
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: "",
				ObjectId:  "",
				Relation:  "",
			},
			Subject: &core.ObjectAndRelation{
				Namespace: req.SubjectRelation.Namespace,
				ObjectId:  afterSubjectID,
				Relation:  req.SubjectRelation.Relation,
			},
		}
	}

	limit := ci.limits.currentLimit + 1 // +1 because there might be a matching wildcard too.
	if !ci.limits.hasLimit {
		limit = 0
	}

	foundSubjectsByResourceID := datasets.NewSubjectSetByResourceID()
	if err := queryForDirectSubjects(ctx, req, datastore.SubjectsSelector{
		OptionalSubjectType: req.SubjectRelation.Namespace,
		RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation(req.SubjectRelation.Relation),
	}, afterCursor, foundSubjectsByResourceID, reader, limit); err != nil {
		return err
	}

	// Send the results to the stream.
	if foundSubjectsByResourceID.IsEmpty() {
		return nil
	}
	return publishSubjects(stream, ci, foundSubjectsByResourceID.AsMap(), addCallToResponseMetadata(emptyMetadata))
}

// lookupWildcardSubjectForRelation finds the wildcard subject on the request's relation, if applicable.
func (cl *ConcurrentLookupSubjects) lookupWildcardSubjectForRelation(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	validatedTS *typesystem.ValidatedNamespaceTypeSystem,
	reader datastore.Reader,
) error {
	// Check if a wildcard is possible and, if so, query directly for it without any cursoring. This is necessary because wildcards
	// must *always* be returned, regardless of the cursor.
	if req.SubjectRelation.Relation != tuple.Ellipsis {
		return nil
	}

	wildcardAllowed, err := validatedTS.IsAllowedPublicNamespace(req.ResourceRelation.Relation, req.SubjectRelation.Namespace)
	if err != nil {
		return err
	}
	if wildcardAllowed == typesystem.PublicSubjectNotAllowed {
		return nil
	}

	// NOTE: the cursor here is `nil` regardless of that passed in, to ensure wildcards are always returned.
	foundSubjectsByResourceID := datasets.NewSubjectSetByResourceID()
	if err := queryForDirectSubjects(ctx, req, datastore.SubjectsSelector{
		OptionalSubjectType: req.SubjectRelation.Namespace,
		OptionalSubjectIds:  []string{tuple.PublicWildcard},
		RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
	}, nil, foundSubjectsByResourceID, reader, 1); err != nil {
		return err
	}

	// Send the results to the stream.
	if foundSubjectsByResourceID.IsEmpty() {
		return nil
	}

	return publishSubjects(stream, ci, foundSubjectsByResourceID.AsMap(), addCallToResponseMetadata(emptyMetadata))
}

// dispatchIndirectSubjectsForRelation looks up all non-ellipsis subjects on the relation and redispatches the LookupSubjects
// operation over them.
func (cl *ConcurrentLookupSubjects) dispatchIndirectSubjectsForRelation(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	reader datastore.Reader,
) error {
	// TODO(jschorr): use reachability type information to skip subject relations that cannot reach the subject type.
	// TODO(jschorr): Store the range of subjects found as a result of this call and store in the cursor to further optimize.

	// Lookup indirect subjects for redispatching.
	// TODO: limit to only the necessary columns. See: https://github.com/authzed/spicedb/issues/1527
	it, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     req.ResourceRelation.Namespace,
		OptionalResourceRelation: req.ResourceRelation.Relation,
		OptionalResourceIds:      req.ResourceIds,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
		}},
	})
	if err != nil {
		return err
	}
	defer it.Close()

	toDispatchByType := datasets.NewSubjectByTypeSet()
	relationshipsBySubjectONR := mapz.NewMultiMap[string, *core.RelationTuple]()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return it.Err()
		}

		err := toDispatchByType.AddSubjectOf(tpl)
		if err != nil {
			return err
		}

		relationshipsBySubjectONR.Add(tuple.StringONR(tpl.Subject), tpl)
	}
	it.Close()

	return cl.dispatchTo(ctx, ci, req, toDispatchByType, relationshipsBySubjectONR, stream)
}

// queryForDirectSubjects performs querying for direct subjects on the request's relation, with the specified
// subjects selector. The found subjects (if any) are added to the foundSubjectsByResourceID dataset.
func queryForDirectSubjects(
	ctx context.Context,
	req ValidatedLookupSubjectsRequest,
	subjectsSelector datastore.SubjectsSelector,
	afterCursor options.Cursor,
	foundSubjectsByResourceID datasets.SubjectSetByResourceID,
	reader datastore.Reader,
	limit uint32,
) error {
	queryOptions := []options.QueryOptionsOption{options.WithSort(options.BySubject), options.WithAfter(afterCursor)}
	if limit > 0 {
		limit64 := uint64(limit)
		queryOptions = append(queryOptions, options.WithLimit(&limit64))
	}

	sit, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     req.ResourceRelation.Namespace,
		OptionalResourceRelation: req.ResourceRelation.Relation,
		OptionalResourceIds:      req.ResourceIds,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			subjectsSelector,
		},
	}, queryOptions...)
	if err != nil {
		return err
	}
	defer sit.Close()

	for tpl := sit.Next(); tpl != nil; tpl = sit.Next() {
		if sit.Err() != nil {
			return sit.Err()
		}
		if err := foundSubjectsByResourceID.AddFromRelationship(tpl); err != nil {
			return fmt.Errorf("failed to call AddFromRelationship in lookupDirectSubjects: %w", err)
		}
	}
	if sit.Err() != nil {
		return sit.Err()
	}
	sit.Close()
	return nil
}

// lookupViaComputed redispatches LookupSubjects over a computed relation.
func (cl *ConcurrentLookupSubjects) lookupViaComputed(
	ctx context.Context,
	ci cursorInformation,
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

	metadata := addCallToResponseMetadata(emptyMetadata)

	stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
		Stream: parentStream,
		Ctx:    ctx,
		Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
			resp := &v1.DispatchLookupSubjectsResponse{
				FoundSubjectsByResourceId: result.FoundSubjectsByResourceId,
				Metadata:                  combineResponseMetadata(result.Metadata, metadata),
				AfterResponseCursor:       result.AfterResponseCursor,
			}
			metadata = emptyMetadata
			return resp, true, nil
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
		OptionalCursor: ci.currentCursor,
		OptionalLimit:  ci.limits.currentLimit,
	}, stream)
}

// lookupViaTupleToUserset redispatches LookupSubjects over those objects found from an arrow (TTU).
func (cl *ConcurrentLookupSubjects) lookupViaTupleToUserset(
	ctx context.Context,
	ci cursorInformation,
	parentRequest ValidatedLookupSubjectsRequest,
	parentStream dispatch.LookupSubjectsStream,
	ttu *core.TupleToUserset,
) error {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentRequest.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     parentRequest.ResourceRelation.Namespace,
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

	return cl.dispatchTo(ctx, ci, parentRequest, toDispatchByComputedRelationType, relationshipsBySubjectONR, parentStream)
}

// lookupViaRewrite performs LookupSubjects over a rewrite operation (union, intersection, exclusion).
func (cl *ConcurrentLookupSubjects) lookupViaRewrite(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	usr *core.UsersetRewrite,
	concurrencyLimit uint16,
) error {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		log.Ctx(ctx).Trace().Msg("union")
		return cl.lookupSetOperationForUnion(ctx, ci, req, stream, rw.Union, concurrencyLimit)
	case *core.UsersetRewrite_Intersection:
		log.Ctx(ctx).Trace().Msg("intersection")
		return cl.lookupSetOperationInSequence(ctx, ci, req, rw.Intersection, newLookupSubjectsIntersection(stream, ci), concurrencyLimit)
	case *core.UsersetRewrite_Exclusion:
		log.Ctx(ctx).Trace().Msg("exclusion")
		return cl.lookupSetOperationInSequence(ctx, ci, req, rw.Exclusion, newLookupSubjectsExclusion(stream, ci), concurrencyLimit)
	default:
		return fmt.Errorf("unknown kind of rewrite in lookup subjects")
	}
}

func (cl *ConcurrentLookupSubjects) lookupSetOperationForUnion(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	stream dispatch.LookupSubjectsStream,
	so *core.SetOperation,
	concurrencyLimit uint16,
) error {
	runChild := func(cctx context.Context, cstream dispatch.LookupSubjectsStream, childOneof *core.SetOperation_Child) error {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return errors.New("use of _this is unsupported; please rewrite your schema")

		case *core.SetOperation_Child_ComputedUserset:
			return cl.lookupViaComputed(cctx, ci, req, cstream, child.ComputedUserset)

		case *core.SetOperation_Child_UsersetRewrite:
			return cl.lookupViaRewrite(cctx, ci, req, cstream, child.UsersetRewrite, adjustConcurrencyLimit(concurrencyLimit, len(so.Child)))

		case *core.SetOperation_Child_TupleToUserset:
			return cl.lookupViaTupleToUserset(cctx, ci, req, cstream, child.TupleToUserset)

		case *core.SetOperation_Child_XNil:
			// Purposely do nothing.
			return nil

		default:
			return fmt.Errorf("unknown set operation child `%T` in expand", child)
		}
	}

	return lsUnion(ctx, ci, stream, concurrencyLimit, so.Child, runChild)
}

func lsUnion[T any](
	ctx context.Context,
	ci cursorInformation,
	stream dispatch.LookupSubjectsStream,
	concurrencyLimit uint16,
	children []T,
	runChild func(ctx context.Context, stream dispatch.LookupSubjectsStream, child T) error,
) error {
	// NOTE: unlike intersection or exclusion, union can run all of its branches in parallel, with the starting cursor
	// and limit, as the results will be merged at completion of the operation and any "extra" results will be tossed.
	reducer := newLookupSubjectsUnion(stream, ci)

	// Skip the goroutines when there is a single child, such as a direct aliasing of a permission (permission foo = bar)
	if len(children) == 1 {
		if err := runChild(ctx, reducer.ForIndex(ctx, 0), children[0]); err != nil {
			return err
		}
	} else {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, subCtx := errgroup.WithContext(cancelCtx)
		g.SetLimit(int(concurrencyLimit))

		for index, child := range children {
			stream := reducer.ForIndex(subCtx, index)
			child := child
			g.Go(func() error {
				return runChild(subCtx, stream, child)
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}
	}

	return reducer.CompletedChildOperations()
}

func (cl *ConcurrentLookupSubjects) lookupSetOperationInSequence(
	ctx context.Context,
	ci cursorInformation,
	req ValidatedLookupSubjectsRequest,
	so *core.SetOperation,
	reducer *dependentBranchReducer,
	concurrencyLimit uint16,
) error {
	// Run the intersection/exclusion until the limit is reached (if applicable) or until results are exhausted.
	for {
		if ci.limits.hasExhaustedLimit() {
			return nil
		}

		// In order to run a cursored/limited intersection or exclusion, we need to ensure that the later branches represent
		// the entire span of results from the first branch. Therefore, we run the first branch, gets its results, then run
		// the later branches, looping until the entire span is computed. The span looping occurs within RunUntilSpanned based
		// on the passed in `index`.
		for index, childOneof := range so.Child {
			stream := reducer.ForIndex(ctx, index)
			err := reducer.RunUntilSpanned(ctx, index, func(ctx context.Context, current branchRunInformation) error {
				switch child := childOneof.ChildType.(type) {
				case *core.SetOperation_Child_XThis:
					return errors.New("use of _this is unsupported; please rewrite your schema")

				case *core.SetOperation_Child_ComputedUserset:
					return cl.lookupViaComputed(ctx, current.ci, req, stream, child.ComputedUserset)

				case *core.SetOperation_Child_UsersetRewrite:
					return cl.lookupViaRewrite(ctx, current.ci, req, stream, child.UsersetRewrite, concurrencyLimit)

				case *core.SetOperation_Child_TupleToUserset:
					return cl.lookupViaTupleToUserset(ctx, current.ci, req, stream, child.TupleToUserset)

				case *core.SetOperation_Child_XNil:
					// Purposely do nothing.
					return nil

				default:
					return fmt.Errorf("unknown set operation child `%T` in expand", child)
				}
			})
			if err != nil {
				return err
			}
		}

		firstBranchConcreteCount, err := reducer.CompletedDependentChildOperations()
		if err != nil {
			return err
		}

		// If the first branch has no additional results, then we're done.
		if firstBranchConcreteCount == 0 {
			return nil
		}
	}
}

func (cl *ConcurrentLookupSubjects) dispatchTo(
	ctx context.Context,
	ci cursorInformation,
	parentRequest ValidatedLookupSubjectsRequest,
	toDispatchByType *datasets.SubjectByTypeSet,
	relationshipsBySubjectONR *mapz.MultiMap[string, *core.RelationTuple],
	parentStream dispatch.LookupSubjectsStream,
) error {
	if toDispatchByType.IsEmpty() {
		return nil
	}

	return toDispatchByType.ForEachTypeUntil(func(resourceType *core.RelationReference, foundSubjects datasets.SubjectSet) (bool, error) {
		metadata := addCallToResponseMetadata(emptyMetadata)

		if ci.limits.hasExhaustedLimit() {
			return false, nil
		}

		slice := foundSubjects.AsSlice()
		resourceIds := make([]string, 0, len(slice))
		for _, foundSubject := range slice {
			resourceIds = append(resourceIds, foundSubject.SubjectId)
		}

		var publishLock sync.Mutex

		stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
			Stream: parentStream,
			Ctx:    ctx,
			Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
				publishLock.Lock()
				defer publishLock.Unlock()

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
				mappedFoundSubjects := make(map[string]*v1.FoundSubjects, len(result.FoundSubjectsByResourceId))
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

				// NOTE: this response does not need to be limited or filtered because the child dispatch has already done so.
				resp := &v1.DispatchLookupSubjectsResponse{
					FoundSubjectsByResourceId: mappedFoundSubjects,
					Metadata:                  combineResponseMetadata(metadata, result.Metadata),
					AfterResponseCursor:       result.AfterResponseCursor,
				}

				metadata = emptyMetadata
				return resp, true, nil
			},
		}

		// Dispatch the found subjects as the resources of the next step.
		chunks := [][]string{}
		_, _ = slicez.ForEachChunkUntil(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) (bool, error) {
			chunks = append(chunks, resourceIdChunk)
			return true, nil
		})

		err := lsUnion(ctx, ci, stream, cl.concurrencyLimit, chunks, func(ctx context.Context, cstream dispatch.LookupSubjectsStream, child []string) error {
			err := cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: resourceType,
				ResourceIds:      child,
				SubjectRelation:  parentRequest.SubjectRelation,
				Metadata: &v1.ResolverMeta{
					AtRevision:     parentRequest.Revision.String(),
					DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
				},
				OptionalCursor: ci.currentCursor,
				OptionalLimit:  ci.limits.currentLimit,
			}, stream)
			if err != nil {
				return err
			}

			return nil
		})
		return true, err
	})
}

type unionOperation struct {
	callback func(ctx context.Context, stream dispatch.LookupSubjectsStream, concurrencyLimit uint16) error
	runIf    bool
}

// runInParallel runs the given operations in parallel, union-ing together the results from the operations.
func runInParallel(ctx context.Context, ci cursorInformation, stream dispatch.LookupSubjectsStream, concurrencyLimit uint16, operations ...unionOperation) error {
	filteredOperations := make([]unionOperation, 0, len(operations))
	for _, op := range operations {
		if op.runIf {
			filteredOperations = append(filteredOperations, op)
		}
	}

	// If there is no work to be done, return.
	if len(filteredOperations) == 0 {
		return nil
	}

	// If there is only a single operation to run, just invoke it directly to avoid creating unnecessary goroutines and
	// additional work.
	if len(filteredOperations) == 1 {
		return filteredOperations[0].callback(ctx, stream, concurrencyLimit)
	}

	// Otherwise, run each operation in parallel and union together the results via a reducer.
	reducer := newLookupSubjectsUnion(stream, ci)

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, subCtx := errgroup.WithContext(cancelCtx)
	g.SetLimit(int(concurrencyLimit))

	adjustedLimit := adjustConcurrencyLimit(concurrencyLimit, 1)
	for index, fop := range filteredOperations {
		opStream := reducer.ForIndex(subCtx, index)
		fop := fop
		adjustedLimit = adjustedLimit - 1
		currentLimit := max(adjustedLimit, 1)
		g.Go(func() error {
			return fop.callback(subCtx, opStream, currentLimit)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return reducer.CompletedChildOperations()
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

// finalSubjectIDForResults returns the ID of the last subject (sorted) in the results, if any.
// Returns empty string if none.
func finalSubjectIDForResults(ci cursorInformation, results []*v1.DispatchLookupSubjectsResponse) (string, error) {
	endingSubjectIDs := mapz.NewSet[string]()
	for _, result := range results {
		frc, err := newCursorInformation(result.AfterResponseCursor, ci.limits, lsDispatchVersion)
		if err != nil {
			return "", err
		}

		lastSubjectID, _ := frc.headSectionValue()
		if lastSubjectID == "" {
			return "", spiceerrors.MustBugf("got invalid cursor")
		}

		endingSubjectIDs.Add(lastSubjectID)
	}

	sortedSubjectIDs := endingSubjectIDs.AsSlice()
	sort.Strings(sortedSubjectIDs)

	if len(sortedSubjectIDs) == 0 {
		return "", nil
	}

	return sortedSubjectIDs[len(sortedSubjectIDs)-1], nil
}

// createFilteredAndLimitedResponse creates a filtered and limited (as is necessary via the cursor and limits)
// version of the subjects, returning a DispatchLookupSubjectsResponse ready for publishing with just that
// subset of results.
func createFilteredAndLimitedResponse(
	ci cursorInformation,
	subjects map[string]*v1.FoundSubjects,
	metadata *v1.ResponseMeta,
) (*v1.DispatchLookupSubjectsResponse, func(), error) {
	if subjects == nil {
		return nil, func() {}, spiceerrors.MustBugf("nil subjects given to createFilteredAndLimitedResponse")
	}

	afterSubjectID, _ := ci.headSectionValue()

	// Filter down the subjects found by the cursor (if applicable) and then apply a limit.
	filteredSubjectIDs := mapz.NewSet[string]()
	for _, foundSubjects := range subjects {
		for _, foundSubject := range foundSubjects.FoundSubjects {
			// NOTE: wildcard is always returned, because it is needed by all branches, at all times.
			if foundSubject.SubjectId == tuple.PublicWildcard || (afterSubjectID == "" || foundSubject.SubjectId > afterSubjectID) {
				filteredSubjectIDs.Add(foundSubject.SubjectId)
			}
		}
	}

	sortedSubjectIDs := filteredSubjectIDs.AsSlice()
	sort.Strings(sortedSubjectIDs)

	subjectIDsToPublish := mapz.NewSet[string]()
	lastSubjectIDToPublishWithoutWildcard := ""

	done := func() {}
	for _, subjectID := range sortedSubjectIDs {
		// Wildcards are always published, regardless of the limit.
		if subjectID == tuple.PublicWildcard {
			subjectIDsToPublish.Add(subjectID)
			continue
		}

		ok := ci.limits.prepareForPublishing()
		if !ok {
			break
		}

		subjectIDsToPublish.Add(subjectID)
		lastSubjectIDToPublishWithoutWildcard = subjectID
	}

	if subjectIDsToPublish.IsEmpty() {
		return nil, done, nil
	}

	// Determine the subject ID for the cursor. If there are any concrete subject IDs, then the last
	// one is used. Otherwise, the wildcard itself is published as a specialized cursor to indicate that
	// all concrete subjects have been consumed.
	cursorSubjectID := "*"
	if len(lastSubjectIDToPublishWithoutWildcard) > 0 {
		cursorSubjectID = lastSubjectIDToPublishWithoutWildcard
	}

	updatedCI, err := ci.withOutgoingSection(cursorSubjectID)
	if err != nil {
		return nil, done, err
	}

	// Filter the subjects down to only those that are to be published.
	foundSubjectsByResourceID := make(map[string]*v1.FoundSubjects, len(subjects))
	for key, subjects := range subjects {
		filtered := make([]*v1.FoundSubject, 0, len(subjects.FoundSubjects))
		for _, subject := range subjects.FoundSubjects {
			if !subjectIDsToPublish.Has(subject.SubjectId) {
				continue
			}

			filtered = append(filtered, subject)
		}

		sort.Sort(bySubjectID(filtered))
		if len(filtered) > 0 {
			foundSubjectsByResourceID[key] = &v1.FoundSubjects{FoundSubjects: filtered}
		}
	}

	return &v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: foundSubjectsByResourceID,
		Metadata:                  metadata,
		AfterResponseCursor:       updatedCI.responsePartialCursor(),
	}, done, nil
}

// publishSubjects publishes the given subjects to the stream, after applying filtering and limiting.
func publishSubjects(stream dispatch.LookupSubjectsStream, ci cursorInformation, subjects map[string]*v1.FoundSubjects, metadata *v1.ResponseMeta) error {
	response, done, err := createFilteredAndLimitedResponse(ci, subjects, metadata)
	defer done()
	if err != nil {
		return err
	}

	if response == nil {
		return nil
	}

	return stream.Publish(response)
}

func adjustConcurrencyLimit(concurrencyLimit uint16, count int) uint16 {
	if int(concurrencyLimit)-count <= 0 {
		return 1
	}

	return concurrencyLimit - uint16(count)
}

type bySubjectID []*v1.FoundSubject

func (u bySubjectID) Len() int {
	return len(u)
}

func (u bySubjectID) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

func (u bySubjectID) Less(i, j int) bool {
	return u[i].SubjectId < u[j].SubjectId
}

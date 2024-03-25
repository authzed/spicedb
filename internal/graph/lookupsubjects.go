package graph

import (
	"context"
	"errors"
	"fmt"
	"sort"

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

	return publishSubjects(stream, ci, subjectsMap)
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
	return publishSubjects(stream, ci, foundSubjectsByResourceID.AsMap())
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

	return publishSubjects(stream, ci, foundSubjectsByResourceID.AsMap())
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

	stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
		Stream: parentStream,
		Ctx:    ctx,
		Processor: func(result *v1.DispatchLookupSubjectsResponse) (*v1.DispatchLookupSubjectsResponse, bool, error) {
			return &v1.DispatchLookupSubjectsResponse{
				FoundSubjectsByResourceId: result.FoundSubjectsByResourceId,
				Metadata:                  addCallToResponseMetadata(result.Metadata),
				AfterResponseCursor:       result.AfterResponseCursor,
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
	// NOTE: unlike intersection or exclusion, union can run all of its branches in parallel, with the starting cursor
	// and limit, as the results will be merged at completion of the operation and any "extra" results will be tossed.
	reducer := newLookupSubjectsUnion(stream, ci)

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

	// Skip the goroutines when there is a single child, such as a direct aliasing of a permission (permission foo = bar)
	if len(so.Child) == 1 {
		if err := runChild(ctx, reducer.ForIndex(ctx, 0), so.Child[0]); err != nil {
			return err
		}
	} else {
		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		g, subCtx := errgroup.WithContext(cancelCtx)
		g.SetLimit(int(concurrencyLimit))

		for index, childOneof := range so.Child {
			stream := reducer.ForIndex(subCtx, index)
			childOneof := childOneof
			g.Go(func() error {
				return runChild(subCtx, stream, childOneof)
			})
		}

		// Wait for all dispatched operations to complete.
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
		if ci.limits.hasExhaustedLimit() {
			return false, nil
		}

		slice := foundSubjects.AsSlice()
		resourceIds := make([]string, 0, len(slice))
		for _, foundSubject := range slice {
			resourceIds = append(resourceIds, foundSubject.SubjectId)
		}

		stream := &dispatch.WrappedDispatchStream[*v1.DispatchLookupSubjectsResponse]{
			Stream: parentStream,
			Ctx:    ctx,
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
				return &v1.DispatchLookupSubjectsResponse{
					FoundSubjectsByResourceId: mappedFoundSubjects,
					Metadata:                  addCallToResponseMetadata(result.Metadata),
					AfterResponseCursor:       result.AfterResponseCursor,
				}, true, nil
			},
		}

		// Dispatch the found subjects as the resources of the next step.
		return slicez.ForEachChunkUntil(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) (bool, error) {
			err := cl.d.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: resourceType,
				ResourceIds:      resourceIdChunk,
				SubjectRelation:  parentRequest.SubjectRelation,
				Metadata: &v1.ResolverMeta{
					AtRevision:     parentRequest.Revision.String(),
					DepthRemaining: parentRequest.Metadata.DepthRemaining - 1,
				},
				OptionalCursor: ci.currentCursor,
				OptionalLimit:  ci.limits.currentLimit,
			}, stream)
			if err != nil {
				return false, err
			}

			return true, nil
		})
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
		currentLimit := adjustedLimit
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

// lookupSubjectsUnion defines a reducer for union operations, where all the results from each stream
// for each branch are unioned together, filtered, limited and then published.
type lookupSubjectsUnion struct {
	parentStream dispatch.LookupSubjectsStream
	collectors   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]
	ci           cursorInformation
}

func newLookupSubjectsUnion(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *lookupSubjectsUnion {
	return &lookupSubjectsUnion{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		ci:           ci,
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

	// Since we've collected results from multiple branches, some which may be past the end of the overall limit,
	// do a cursor-based filtering here to enure we only return the limit.
	resp, done, err := createFilteredAndLimitedResponse(lsu.ci, foundSubjects.AsMap(), metadata)
	defer done()
	if err != nil {
		return err
	}

	if resp == nil {
		return nil
	}

	return lsu.parentStream.Publish(resp)
}

// branchRunInformation is information passed to a RunUntilSpanned handler.
type branchRunInformation struct {
	ci cursorInformation
}

// dependentBranchReducerReloopLimit is the limit of results for each iteration of the dependent branch LookupSubject redispatches.
const dependentBranchReducerReloopLimit = 1000

// dependentBranchReducer is the implementation reducer for any rewrite operations whose branches depend upon one another
// (intersection and exclusion).
type dependentBranchReducer struct {
	// parentStream is the stream to which results will be published, after reduction.
	parentStream dispatch.LookupSubjectsStream

	// collectors are a map from branch index to the associated collector of stream results.
	collectors map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]

	// parentCi is the cursor information from the parent call.
	parentCi cursorInformation

	// combinationHandler is the function invoked to "combine" the results from different branches, such as performing
	// intersection or exclusion.
	combinationHandler func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error

	// firstBranchCi is the *current* cursor for the first branch; this value is updated during iteration as the reducer is
	// re-run.
	firstBranchCi cursorInformation
}

// ForIndex returns the stream to which results should be published for the branch with the given index. Must not be called
// in parallel.
func (dbr *dependentBranchReducer) ForIndex(ctx context.Context, setOperationIndex int) dispatch.LookupSubjectsStream {
	collector := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	dbr.collectors[setOperationIndex] = collector
	return collector
}

// RunUntilSpanned runs the branch (with the given index) until all necessary results have been collected. For the first branch,
// this is just a direct invocation. For all other branches, the handler will be reinvoked until all results have been collected
// *or* the last subject ID found is >= the last subject ID found by the first branch, ensuring that all other branches have
// "spanned" the subjects of the first branch. This is necessary because an intersection or exclusion must operate over the same
// set of subject IDs.
func (dbr *dependentBranchReducer) RunUntilSpanned(ctx context.Context, index int, handler func(ctx context.Context, current branchRunInformation) error) error {
	// If invoking the run for the first branch, use the current first branch cursor.
	if index == 0 {
		return handler(ctx, branchRunInformation{ci: dbr.firstBranchCi.withClonedLimits()})
	}

	// Otherwise, run the branch until it has either exhausted all results OR the last result returned matches the last result previously
	// returned by the first branch. This is to ensure that the other branches encompass the entire "span" of results from the first branch,
	// which is necessary for intersection or exclusion (e.g. dependent branches).
	firstBranchTerminalSubjectID, err := finalSubjectIDForResults(dbr.firstBranchCi, dbr.collectors[0].Results())
	if err != nil {
		return err
	}

	// If there are no concrete subject IDs found, then simply invoke the handler with the first branch's cursor/limit to
	// return the wildcard; all other results will be superflouous.
	if firstBranchTerminalSubjectID == "" {
		return handler(ctx, branchRunInformation{ci: dbr.firstBranchCi})
	}

	// Otherwise, run the handler until its returned results is empty OR its cursor is >= the terminal subject ID.
	startingCursor := dbr.firstBranchCi.currentCursor
	previousResultCount := 0
	for {
		limits := newLimitTracker(dependentBranchReducerReloopLimit)
		ci, err := newCursorInformation(startingCursor, limits, lsDispatchVersion)
		if err != nil {
			return err
		}

		// Invoke the handler with a modified limits and a cursor starting at the previous call.
		if err := handler(ctx, branchRunInformation{
			ci: ci,
		}); err != nil {
			return err
		}

		// Check for any new results found. If none, then we're done.
		updatedResults := dbr.collectors[index].Results()
		if len(updatedResults) == previousResultCount {
			return nil
		}

		// Otherwise, grab the terminal subject ID to create the next cursor.
		previousResultCount = len(updatedResults)
		terminalSubjectID, err := finalSubjectIDForResults(dbr.parentCi, updatedResults)
		if err != nil {
			return nil
		}

		// If the cursor is now the wildcard, then we know that all concrete results have been consumed.
		if terminalSubjectID == tuple.PublicWildcard {
			return nil
		}

		// If the terminal subject in the results collector is now at or beyond that of the first branch, then
		// we've spanned the entire results set necessary to perform the intersection or exclusion.
		if firstBranchTerminalSubjectID != tuple.PublicWildcard && terminalSubjectID >= firstBranchTerminalSubjectID {
			return nil
		}

		startingCursor = updatedResults[len(updatedResults)-1].AfterResponseCursor
	}
}

// CompletedDependentChildOperations is invoked once all branches have been run to perform combination and publish any
// valid subject IDs. This also moves the first branch's cursor forward.
//
// Returns the number of results from the first branch, and/or any error. The number of results is used to determine whether
// the first branch has been exhausted.
func (dbr *dependentBranchReducer) CompletedDependentChildOperations() (int, error) {
	firstBranchCount := -1

	// Update the first branch cursor for moving forward. This ensures that each iteration of the first branch for
	// RunUntilSpanned is moving forward.
	firstBranchTerminalSubjectID, err := finalSubjectIDForResults(dbr.parentCi, dbr.collectors[0].Results())
	if err != nil {
		return firstBranchCount, err
	}

	existingFirstBranchCI := dbr.firstBranchCi
	if firstBranchTerminalSubjectID != "" {
		updatedCI, err := dbr.firstBranchCi.withOutgoingSection(firstBranchTerminalSubjectID)
		if err != nil {
			return -1, err
		}

		updatedCursor := updatedCI.responsePartialCursor()
		fbci, err := newCursorInformation(updatedCursor, dbr.firstBranchCi.limits, lsDispatchVersion)
		if err != nil {
			return firstBranchCount, err
		}

		dbr.firstBranchCi = fbci
	}

	// Run the combiner over the results.
	var foundSubjects datasets.SubjectSetByResourceID
	metadata := emptyMetadata

	for index := 0; index < len(dbr.collectors); index++ {
		collector, ok := dbr.collectors[index]
		if !ok {
			return firstBranchCount, fmt.Errorf("missing collector for index %d", index)
		}

		results := datasets.NewSubjectSetByResourceID()
		for _, result := range collector.Results() {
			metadata = combineResponseMetadata(metadata, result.Metadata)
			if err := results.UnionWith(result.FoundSubjectsByResourceId); err != nil {
				return firstBranchCount, fmt.Errorf("failed to UnionWith: %w", err)
			}
		}

		if index == 0 {
			foundSubjects = results
			firstBranchCount = results.ConcreteSubjectCount()
		} else {
			err := dbr.combinationHandler(foundSubjects, results)
			if err != nil {
				return firstBranchCount, err
			}

			if foundSubjects.IsEmpty() {
				return firstBranchCount, nil
			}
		}
	}

	// Apply the limits to the found results.
	resp, done, err := createFilteredAndLimitedResponse(existingFirstBranchCI, foundSubjects.AsMap(), metadata)
	defer done()
	if err != nil {
		return firstBranchCount, err
	}

	if resp == nil {
		return firstBranchCount, nil
	}

	return firstBranchCount, dbr.parentStream.Publish(resp)
}

func newLookupSubjectsIntersection(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *dependentBranchReducer {
	return &dependentBranchReducer{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		parentCi:     ci,
		combinationHandler: func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error {
			return fs.IntersectionDifference(other)
		},
		firstBranchCi: ci,
	}
}

func newLookupSubjectsExclusion(parentStream dispatch.LookupSubjectsStream, ci cursorInformation) *dependentBranchReducer {
	return &dependentBranchReducer{
		parentStream: parentStream,
		collectors:   map[int]*dispatch.CollectingDispatchStream[*v1.DispatchLookupSubjectsResponse]{},
		parentCi:     ci,
		combinationHandler: func(fs datasets.SubjectSetByResourceID, other datasets.SubjectSetByResourceID) error {
			fs.SubtractAll(other)
			return nil
		},
		firstBranchCi: ci,
	}
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

	subjectIDsToPublish := make([]string, 0, len(sortedSubjectIDs))
	lastSubjectIDToPublishWithoutWildcard := ""

	done := func() {}
	for _, subjectID := range sortedSubjectIDs {
		// Wildcards are always published, regardless of the limit.
		if subjectID == tuple.PublicWildcard {
			subjectIDsToPublish = append(subjectIDsToPublish, subjectID)
			continue
		}

		ok := ci.limits.prepareForPublishing()
		if !ok {
			break
		}

		subjectIDsToPublish = append(subjectIDsToPublish, subjectID)
		lastSubjectIDToPublishWithoutWildcard = subjectID
	}

	if len(subjectIDsToPublish) == 0 {
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

	return &v1.DispatchLookupSubjectsResponse{
		FoundSubjectsByResourceId: filterSubjectsMap(subjects, subjectIDsToPublish),
		Metadata:                  metadata,
		AfterResponseCursor:       updatedCI.responsePartialCursor(),
	}, done, nil
}

// publishSubjects publishes the given subjects to the stream, after appying filtering and limiting.
func publishSubjects(stream dispatch.LookupSubjectsStream, ci cursorInformation, subjects map[string]*v1.FoundSubjects) error {
	response, done, err := createFilteredAndLimitedResponse(ci, subjects, emptyMetadata)
	defer done()
	if err != nil {
		return err
	}

	if response == nil {
		return nil
	}

	return stream.Publish(response)
}

// filterSubjectsMap filters the subjects found in the subjects map to only those allowed, returning an updated map.
func filterSubjectsMap(subjects map[string]*v1.FoundSubjects, allowedSubjectIds []string) map[string]*v1.FoundSubjects {
	updated := make(map[string]*v1.FoundSubjects, len(subjects))
	allowed := mapz.NewSet[string](allowedSubjectIds...)

	for key, subjects := range subjects {
		filtered := make([]*v1.FoundSubject, 0, len(subjects.FoundSubjects))

		for _, subject := range subjects.FoundSubjects {
			if !allowed.Has(subject.SubjectId) {
				continue
			}

			filtered = append(filtered, subject)
		}

		sort.Sort(bySubjectID(filtered))
		if len(filtered) > 0 {
			updated[key] = &v1.FoundSubjects{FoundSubjects: filtered}
		}
	}

	return updated
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

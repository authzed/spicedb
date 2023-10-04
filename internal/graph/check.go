package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/taskrunner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

var dispatchChunkCountHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "spicedb_check_dispatch_chunk_count",
	Help:    "number of chunks when dispatching in check",
	Buckets: []float64{1, 2, 3, 5, 10, 25, 100, 250},
})

var directDispatchQueryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "spicedb_check_direct_dispatch_query_count",
	Help:    "number of queries made per direct dispatch",
	Buckets: []float64{1, 2},
})

func init() {
	prometheus.MustRegister(directDispatchQueryHistogram)
	prometheus.MustRegister(dispatchChunkCountHistogram)
}

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check, concurrencyLimit uint16) *ConcurrentChecker {
	return &ConcurrentChecker{d, concurrencyLimit}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d                dispatch.Check
	concurrencyLimit uint16
}

// ValidatedCheckRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedCheckRequest struct {
	*v1.DispatchCheckRequest
	Revision datastore.Revision
}

// currentRequestContext holds context information for the current request being
// processed.
type currentRequestContext struct {
	// parentReq is the parent request being processed.
	parentReq ValidatedCheckRequest

	// filteredResourceIDs are those resource IDs to be checked after filtering for
	// any resource IDs found directly matching the incoming subject.
	//
	// For example, a check of resources `user:{tom,sarah,fred}` and subject `user:sarah` will
	// result in this slice containing `tom` and `fred`, but not `sarah`, as she was found as a
	// match.
	//
	// This check and filter occurs via the filterForFoundMemberResource function in the
	// checkInternal function before the rest of the checking logic is run. This slice should never
	// be empty.
	filteredResourceIDs []string

	// resultsSetting is the results setting to use for this request and all subsequent
	// requests.
	resultsSetting v1.DispatchCheckRequest_ResultsSetting

	// maxDispatchCount is the maximum number of resource IDs that can be specified in each dispatch.
	maxDispatchCount uint16
}

// Check performs a check request with the provided request and context
func (cc *ConcurrentChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) (*v1.DispatchCheckResponse, error) {
	var startTime *time.Time
	if req.Debug != v1.DispatchCheckRequest_NO_DEBUG {
		now := time.Now()
		startTime = &now
	}

	resolved := cc.checkInternal(ctx, req, relation)
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	if req.Debug == v1.DispatchCheckRequest_NO_DEBUG {
		return resolved.Resp, resolved.Err
	}

	// Add debug information if requested.
	debugInfo := resolved.Resp.Metadata.DebugInfo
	if debugInfo == nil {
		debugInfo = &v1.DebugInformation{
			Check: &v1.CheckDebugTrace{},
		}
	}

	debugInfo.Check.Request = req.DispatchCheckRequest
	debugInfo.Check.Duration = durationpb.New(time.Since(*startTime))

	if nspkg.GetRelationKind(relation) == iv1.RelationMetadata_PERMISSION {
		debugInfo.Check.ResourceRelationType = v1.CheckDebugTrace_PERMISSION
	} else if nspkg.GetRelationKind(relation) == iv1.RelationMetadata_RELATION {
		debugInfo.Check.ResourceRelationType = v1.CheckDebugTrace_RELATION
	}

	// Build the results for the debug trace.
	results := make(map[string]*v1.ResourceCheckResult, len(req.DispatchCheckRequest.ResourceIds))
	for _, resourceID := range req.DispatchCheckRequest.ResourceIds {
		if found, ok := resolved.Resp.ResultsByResourceId[resourceID]; ok {
			results[resourceID] = found
		}
	}

	debugInfo.Check.Results = results
	resolved.Resp.Metadata.DebugInfo = debugInfo
	return resolved.Resp, resolved.Err
}

func (cc *ConcurrentChecker) checkInternal(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) CheckResult {
	// Ensure that we have proper type information for running the check. This is now required as of the deprecation and removal
	// of the v0 API.
	if relation.GetTypeInformation() == nil && relation.GetUsersetRewrite() == nil {
		return checkResultError(
			fmt.Errorf("found relation `%s` without type information; to fix, please re-write your schema", relation.Name),
			emptyMetadata,
		)
	}

	// Ensure that we have at least one resource ID for which to execute the check.
	if len(req.ResourceIds) == 0 {
		return checkResultError(
			fmt.Errorf("empty resource IDs given to dispatched check"),
			emptyMetadata,
		)
	}

	// Ensure that we are not performing a check for a wildcard as the subject.
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return checkResultError(NewErrInvalidArgument(errors.New("cannot perform check on wildcard")), emptyMetadata)
	}

	// Deduplicate any incoming resource IDs.
	resourceIds := slicez.Unique(req.ResourceIds)

	// Filter the incoming resource IDs for any which match the subject directly. For example, if we receive
	// a check for resource `user:{tom, fred, sarah}#...` and a subject of `user:sarah#...`, then we know
	// that `user:sarah#...` is a valid "member" of the resource, as it matches exactly.
	//
	// If the filtering results in no further resource IDs to check, or a result is found and a single
	// result is allowed, we terminate early.
	membershipSet, filteredResourcesIds := filterForFoundMemberResource(req.ResourceRelation, resourceIds, req.Subject)
	if membershipSet.HasDeterminedMember() && req.DispatchCheckRequest.ResultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
		return checkResultsForMembership(membershipSet, emptyMetadata)
	}

	if len(filteredResourcesIds) == 0 {
		return noMembers()
	}

	// NOTE: We can always allow a single result if we're only trying to find the results for a
	// single resource ID. This "reset" allows for short circuiting of downstream dispatched calls.
	resultsSetting := req.ResultsSetting
	if len(filteredResourcesIds) == 1 {
		resultsSetting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT
	}

	crc := currentRequestContext{
		parentReq:           req,
		filteredResourceIDs: filteredResourcesIds,
		resultsSetting:      resultsSetting,
		maxDispatchCount:    maxDispatchChunkSize,
	}

	if req.Debug == v1.DispatchCheckRequest_ENABLE_TRACE_DEBUGGING {
		crc.maxDispatchCount = 1
	}

	if relation.UsersetRewrite == nil {
		return combineResultWithFoundResources(cc.checkDirect(ctx, crc, relation), membershipSet)
	}

	return combineResultWithFoundResources(cc.checkUsersetRewrite(ctx, crc, relation.UsersetRewrite), membershipSet)
}

type directDispatch struct {
	resourceType *core.RelationReference
	resourceIds  []string
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, crc currentRequestContext, relation *core.Relation) CheckResult {
	log.Ctx(ctx).Trace().Object("direct", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)

	// Build a filter for finding the direct relationships for the check. There are three
	// classes of relationships to be found:
	// 1) the target subject itself, if allowed on this relation
	// 2) the wildcard form of the target subject, if a wildcard is allowed on this relation
	// 3) Otherwise, any non-terminal (non-`...`) subjects, if allowed on this relation, to be
	//    redispatched outward
	hasNonTerminals := false
	hasDirectSubject := false
	hasWildcardSubject := false

	for _, allowedDirectRelation := range relation.GetTypeInformation().GetAllowedDirectRelations() {
		// If the namespace of the allowed direct relation matches the subject type, there are two
		// cases to optimize:
		// 1) Finding the target subject itself, as a direct lookup
		// 2) Finding a wildcard for the subject type+relation
		if allowedDirectRelation.GetNamespace() == crc.parentReq.Subject.Namespace {
			if allowedDirectRelation.GetPublicWildcard() != nil {
				hasWildcardSubject = true
			} else if allowedDirectRelation.GetRelation() == crc.parentReq.Subject.Relation {
				hasDirectSubject = true
			}
		}

		// If the relation found is not an ellipsis, then this is a nested relation that
		// might need to be followed, so indicate that such relationships should be returned
		//
		// TODO(jschorr): Use type information to *further* optimize this query around which nested
		// relations can reach the target subject type.
		if allowedDirectRelation.GetRelation() != tuple.Ellipsis {
			hasNonTerminals = true
		}
	}

	foundResources := NewMembershipSet()

	// If the direct subject or a wildcard form can be found, issue a query for just that
	// subject.
	var queryCount float64
	defer func() {
		directDispatchQueryHistogram.Observe(queryCount)
	}()

	if hasDirectSubject || hasWildcardSubject {
		subjectSelectors := []datastore.SubjectsSelector{}

		if hasDirectSubject {
			subjectSelectors = append(subjectSelectors, datastore.SubjectsSelector{
				OptionalSubjectType: crc.parentReq.Subject.Namespace,
				OptionalSubjectIds:  []string{crc.parentReq.Subject.ObjectId},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithRelation(crc.parentReq.Subject.Relation),
			})
		}

		if hasWildcardSubject {
			subjectSelectors = append(subjectSelectors, datastore.SubjectsSelector{
				OptionalSubjectType: crc.parentReq.Subject.Namespace,
				OptionalSubjectIds:  []string{tuple.PublicWildcard},
				RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
			})
		}

		filter := datastore.RelationshipsFilter{
			ResourceType:              crc.parentReq.ResourceRelation.Namespace,
			OptionalResourceIds:       crc.filteredResourceIDs,
			OptionalResourceRelation:  crc.parentReq.ResourceRelation.Relation,
			OptionalSubjectsSelectors: subjectSelectors,
		}

		it, err := ds.QueryRelationships(ctx, filter)
		if err != nil {
			return checkResultError(NewCheckFailureErr(err), emptyMetadata)
		}
		defer it.Close()
		queryCount += 1.0

		// Find the matching subject(s).
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if it.Err() != nil {
				return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
			}

			// If the subject of the relationship matches the target subject, then we've found
			// a result.
			if !tuple.OnrEqualOrWildcard(tpl.Subject, crc.parentReq.Subject) {
				tplString, err := tuple.String(tpl)
				if err != nil {
					return checkResultError(err, emptyMetadata)
				}

				return checkResultError(
					NewCheckFailureErr(
						fmt.Errorf("somehow got invalid ONR for direct check matching: %s vs %s", tuple.StringONR(crc.parentReq.Subject), tplString),
					),
					emptyMetadata,
				)
			}

			foundResources.AddDirectMember(tpl.ResourceAndRelation.ObjectId, tpl.Caveat)
			if crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT && foundResources.HasDeterminedMember() {
				return checkResultsForMembership(foundResources, emptyMetadata)
			}
		}
		it.Close()
	}

	// Filter down the resource IDs for further dispatch based on whether they exist as found
	// subjects in the existing membership set.
	furtherFilteredResourceIDs := make([]string, 0, len(crc.filteredResourceIDs)-foundResources.Size())
	for _, resourceID := range crc.filteredResourceIDs {
		if foundResources.HasConcreteResourceID(resourceID) {
			continue
		}

		furtherFilteredResourceIDs = append(furtherFilteredResourceIDs, resourceID)
	}

	// If there are no possible non-terminals, then the check is completed.
	if !hasNonTerminals || len(furtherFilteredResourceIDs) == 0 {
		return checkResultsForMembership(foundResources, emptyMetadata)
	}

	// Otherwise, for any remaining resource IDs, query for redispatch.
	filter := datastore.RelationshipsFilter{
		ResourceType:             crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      furtherFilteredResourceIDs,
		OptionalResourceRelation: crc.parentReq.ResourceRelation.Relation,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				RelationFilter: datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
			},
		},
	}

	it, err := ds.QueryRelationships(ctx, filter)
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()
	queryCount += 1.0

	// Find the subjects over which to dispatch.
	subjectsToDispatch := tuple.NewONRByTypeSet()
	relationshipsBySubjectONR := mapz.NewMultiMap[string, *core.RelationTuple]()

	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		// Add the subject as an object over which to dispatch.
		if tpl.Subject.Relation == Ellipsis {
			return checkResultError(NewCheckFailureErr(fmt.Errorf("got a terminal for a non-terminal query")), emptyMetadata)
		}

		subjectsToDispatch.Add(tpl.Subject)
		relationshipsBySubjectONR.Add(tuple.StringONR(tpl.Subject), tpl)
	}
	it.Close()

	// Convert the subjects into batched requests.
	toDispatch := make([]directDispatch, 0, subjectsToDispatch.Len())
	subjectsToDispatch.ForEachType(func(rr *core.RelationReference, resourceIds []string) {
		chunkCount := 0.0
		slicez.ForEachChunk(resourceIds, crc.maxDispatchCount, func(resourceIdChunk []string) {
			chunkCount++
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
		dispatchChunkCountHistogram.Observe(chunkCount)
	})

	// Dispatch and map to the associated resource ID(s).
	result := union(ctx, crc, toDispatch, func(ctx context.Context, crc currentRequestContext, dd directDispatch) CheckResult {
		childResult := cc.dispatch(ctx, crc, ValidatedCheckRequest{
			&v1.DispatchCheckRequest{
				ResourceRelation: dd.resourceType,
				ResourceIds:      dd.resourceIds,
				Subject:          crc.parentReq.Subject,
				ResultsSetting:   crc.resultsSetting,

				Metadata: decrementDepth(crc.parentReq.Metadata),
				Debug:    crc.parentReq.Debug,
			},
			crc.parentReq.Revision,
		})
		if childResult.Err != nil {
			return childResult
		}

		return mapFoundResources(childResult, dd.resourceType, relationshipsBySubjectONR)
	}, cc.concurrencyLimit)

	return combineResultWithFoundResources(result, foundResources)
}

func mapFoundResources(result CheckResult, resourceType *core.RelationReference, relationshipsBySubjectONR *mapz.MultiMap[string, *core.RelationTuple]) CheckResult {
	// Map any resources found to the parent resource IDs.
	membershipSet := NewMembershipSet()
	for foundResourceID, result := range result.Resp.ResultsByResourceId {
		subjectKey := tuple.StringONR(&core.ObjectAndRelation{
			Namespace: resourceType.Namespace,
			ObjectId:  foundResourceID,
			Relation:  resourceType.Relation,
		})

		tuples, _ := relationshipsBySubjectONR.Get(subjectKey)
		for _, relationTuple := range tuples {
			membershipSet.AddMemberViaRelationship(relationTuple.ResourceAndRelation.ObjectId, result.Expression, relationTuple)
		}
	}

	if membershipSet.IsEmpty() {
		return noMembersWithMetadata(result.Resp.Metadata)
	}

	return checkResultsForMembership(membershipSet, result.Resp.Metadata)
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, crc currentRequestContext, rewrite *core.UsersetRewrite) CheckResult {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return union(ctx, crc, rw.Union.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Intersection:
		return all(ctx, crc, rw.Intersection.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Exclusion:
		return difference(ctx, crc, rw.Exclusion.Child, cc.runSetOperation, cc.concurrencyLimit)
	default:
		return checkResultError(fmt.Errorf("unknown userset rewrite operator"), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) dispatch(ctx context.Context, _ currentRequestContext, req ValidatedCheckRequest) CheckResult {
	log.Ctx(ctx).Trace().Object("dispatch", req).Send()
	result, err := cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
	return CheckResult{result, err}
}

func (cc *ConcurrentChecker) runSetOperation(ctx context.Context, crc currentRequestContext, childOneof *core.SetOperation_Child) CheckResult {
	switch child := childOneof.ChildType.(type) {
	case *core.SetOperation_Child_XThis:
		return checkResultError(errors.New("use of _this is unsupported; please rewrite your schema"), emptyMetadata)
	case *core.SetOperation_Child_ComputedUserset:
		return cc.checkComputedUserset(ctx, crc, child.ComputedUserset, nil, nil)
	case *core.SetOperation_Child_UsersetRewrite:
		return cc.checkUsersetRewrite(ctx, crc, child.UsersetRewrite)
	case *core.SetOperation_Child_TupleToUserset:
		return cc.checkTupleToUserset(ctx, crc, child.TupleToUserset)
	case *core.SetOperation_Child_XNil:
		return noMembers()
	default:
		return checkResultError(fmt.Errorf("unknown set operation child `%T` in check", child), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, crc currentRequestContext, cu *core.ComputedUserset, rr *core.RelationReference, resourceIds []string) CheckResult {
	var startNamespace string
	var targetResourceIds []string
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if rr == nil || len(resourceIds) == 0 {
			return checkResultError(spiceerrors.MustBugf("computed userset for tupleset without tuples"), emptyMetadata)
		}

		startNamespace = rr.Namespace
		targetResourceIds = resourceIds
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if rr != nil {
			return checkResultError(spiceerrors.MustBugf("computed userset for tupleset with wrong object type"), emptyMetadata)
		}

		startNamespace = crc.parentReq.ResourceRelation.Namespace
		targetResourceIds = crc.filteredResourceIDs
	}

	targetRR := &core.RelationReference{
		Namespace: startNamespace,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	membershipSet, updatedTargetResourceIds := filterForFoundMemberResource(targetRR, targetResourceIds, crc.parentReq.Subject)
	if (membershipSet.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT) || len(updatedTargetResourceIds) == 0 {
		return checkResultsForMembership(membershipSet, emptyMetadata)
	}

	// Check if the target relation exists. If not, return nothing. This is only necessary
	// for TTU-based computed usersets, as directly computed ones reference relations within
	// the same namespace as the caller, and thus must be fully typed checked.
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
		err := namespace.CheckNamespaceAndRelation(ctx, targetRR.Namespace, targetRR.Relation, true, ds)
		if err != nil {
			if errors.As(err, &namespace.ErrRelationNotFound{}) {
				return noMembers()
			}

			return checkResultError(err, emptyMetadata)
		}
	}

	result := cc.dispatch(ctx, crc, ValidatedCheckRequest{
		&v1.DispatchCheckRequest{
			ResourceRelation: targetRR,
			ResourceIds:      updatedTargetResourceIds,
			Subject:          crc.parentReq.Subject,
			ResultsSetting:   crc.resultsSetting,
			Metadata:         decrementDepth(crc.parentReq.Metadata),
			Debug:            crc.parentReq.Debug,
		},
		crc.parentReq.Revision,
	})
	return combineResultWithFoundResources(result, membershipSet)
}

func filterForFoundMemberResource(resourceRelation *core.RelationReference, resourceIds []string, subject *core.ObjectAndRelation) (*MembershipSet, []string) {
	if resourceRelation.Namespace != subject.Namespace || resourceRelation.Relation != subject.Relation {
		return nil, resourceIds
	}

	for index, resourceID := range resourceIds {
		if subject.ObjectId == resourceID {
			membershipSet := NewMembershipSet()
			membershipSet.AddDirectMember(resourceID, nil)
			return membershipSet, removeIndexFromSlice(resourceIds, index)
		}
	}

	return nil, resourceIds
}

func removeIndexFromSlice[T any](s []T, index int) []T {
	cpy := make([]T, 0, len(s)-1)
	cpy = append(cpy, s[:index]...)
	return append(cpy, s[index+1:]...)
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, crc currentRequestContext, ttu *core.TupleToUserset) CheckResult {
	log.Ctx(ctx).Trace().Object("ttu", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      crc.filteredResourceIDs,
		OptionalResourceRelation: ttu.Tupleset.Relation,
	})
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()

	subjectsToDispatch := tuple.NewONRByTypeSet()
	relationshipsBySubjectONR := mapz.NewMultiMap[string, *core.RelationTuple]()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		subjectsToDispatch.Add(tpl.Subject)
		relationshipsBySubjectONR.Add(tuple.StringONR(tpl.Subject), tpl)
	}
	it.Close()

	// Convert the subjects into batched requests.
	toDispatch := make([]directDispatch, 0, subjectsToDispatch.Len())
	subjectsToDispatch.ForEachType(func(rr *core.RelationReference, resourceIds []string) {
		chunkCount := 0.0
		slicez.ForEachChunk(resourceIds, crc.maxDispatchCount, func(resourceIdChunk []string) {
			chunkCount++
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
		dispatchChunkCountHistogram.Observe(chunkCount)
	})

	return union(
		ctx,
		crc,
		toDispatch,
		func(ctx context.Context, crc currentRequestContext, dd directDispatch) CheckResult {
			childResult := cc.checkComputedUserset(ctx, crc, ttu.ComputedUserset, dd.resourceType, dd.resourceIds)
			if childResult.Err != nil {
				return childResult
			}

			return mapFoundResources(childResult, dd.resourceType, relationshipsBySubjectONR)
		},
		cc.concurrencyLimit,
	)
}

func withDistinctMetadata(result CheckResult) CheckResult {
	// NOTE: This is necessary to ensure unique debug information on the request and that debug
	// information from the child metadata is *not* copied over.
	clonedResp := result.Resp.CloneVT()
	clonedResp.Metadata = combineResponseMetadata(emptyMetadata, clonedResp.Metadata)
	return CheckResult{
		Resp: clonedResp,
		Err:  result.Err,
	}
}

// union returns whether any one of the lazy checks pass, and is used for union.
func union[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return withDistinctMetadata(handler(ctx, crc, children[0]))
	}

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)
	dispatchAllAsync(childCtx, crc, children, handler, resultChan, concurrencyLimit)
	defer cancelFn()

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			membershipSet.UnionWith(result.Resp.ResultsByResourceId)
			if membershipSet.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
				return checkResultsForMembership(membershipSet, responseMetadata)
			}

		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return withDistinctMetadata(handler(ctx, crc, children[0]))
	}

	responseMetadata := emptyMetadata

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)
	dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIDs: crc.filteredResourceIDs,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		maxDispatchCount:    crc.maxDispatchCount,
	}, children, handler, resultChan, concurrencyLimit)
	defer cancelFn()

	var membershipSet *MembershipSet
	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			if membershipSet == nil {
				membershipSet = NewMembershipSet()
				membershipSet.UnionWith(result.Resp.ResultsByResourceId)
			} else {
				membershipSet.IntersectWith(result.Resp.ResultsByResourceId)
			}

			if membershipSet.IsEmpty() {
				return noMembersWithMetadata(responseMetadata)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// difference returns whether the first lazy check passes and none of the supsequent checks pass.
func difference[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return checkResultError(fmt.Errorf("difference requires more than a single child"), emptyMetadata)
	}

	childCtx, cancelFn := context.WithCancel(ctx)
	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(children)-1)

	go func() {
		result := handler(childCtx, crc, children[0])
		baseChan <- result
	}()

	dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIDs: crc.filteredResourceIDs,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
		maxDispatchCount:    crc.maxDispatchCount,
	}, children[1:], handler, othersChan, concurrencyLimit-1)
	defer cancelFn()

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	// Wait for the base set to return.
	select {
	case base := <-baseChan:
		responseMetadata = combineResponseMetadata(responseMetadata, base.Resp.Metadata)

		if base.Err != nil {
			return checkResultError(base.Err, responseMetadata)
		}

		membershipSet.UnionWith(base.Resp.ResultsByResourceId)
		if membershipSet.IsEmpty() {
			return noMembersWithMetadata(responseMetadata)
		}

	case <-ctx.Done():
		return checkResultError(NewRequestCanceledErr(), responseMetadata)
	}

	// Subtract the remaining sets.
	for i := 1; i < len(children); i++ {
		select {
		case sub := <-othersChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Resp.Metadata)

			if sub.Err != nil {
				return checkResultError(sub.Err, responseMetadata)
			}

			membershipSet.Subtract(sub.Resp.ResultsByResourceId)
			if membershipSet.IsEmpty() {
				return noMembersWithMetadata(responseMetadata)
			}

		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

func dispatchAllAsync[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	resultChan chan<- CheckResult,
	concurrencyLimit uint16,
) {
	tr := taskrunner.NewPreloadedTaskRunner(ctx, concurrencyLimit, len(children))
	for _, currentChild := range children {
		currentChild := currentChild
		tr.Add(func(ctx context.Context) error {
			result := handler(ctx, crc, currentChild)
			resultChan <- result
			return result.Err
		})
	}

	tr.Start()
}

func noMembers() CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: emptyMetadata,
		},
		nil,
	}
}

func noMembersWithMetadata(metadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: metadata,
		},
		nil,
	}
}

func checkResultsForMembership(foundMembership *MembershipSet, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:            ensureMetadata(subProblemMetadata),
			ResultsByResourceId: foundMembership.AsCheckResultsMap(),
		},
		nil,
	}
}

func checkResultError(err error, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}

func combineResultWithFoundResources(result CheckResult, foundResources *MembershipSet) CheckResult {
	if result.Err != nil {
		return result
	}

	if foundResources.IsEmpty() {
		return result
	}

	foundResources.UnionWith(result.Resp.ResultsByResourceId)
	return CheckResult{
		Resp: &v1.DispatchCheckResponse{
			ResultsByResourceId: foundResources.AsCheckResultsMap(),
			Metadata:            result.Resp.Metadata,
		},
		Err: result.Err,
	}
}

func combineResponseMetadata(existing *v1.ResponseMeta, responseMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	combined := &v1.ResponseMeta{
		DispatchCount:       existing.DispatchCount + responseMetadata.DispatchCount,
		DepthRequired:       max(existing.DepthRequired, responseMetadata.DepthRequired),
		CachedDispatchCount: existing.CachedDispatchCount + responseMetadata.CachedDispatchCount,
	}

	if existing.DebugInfo == nil && responseMetadata.DebugInfo == nil {
		return combined
	}

	debugInfo := &v1.DebugInformation{
		Check: &v1.CheckDebugTrace{},
	}

	if existing.DebugInfo != nil {
		if existing.DebugInfo.Check.Request != nil {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, existing.DebugInfo.Check)
		} else {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, existing.DebugInfo.Check.SubProblems...)
		}
	}

	if responseMetadata.DebugInfo != nil {
		if responseMetadata.DebugInfo.Check.Request != nil {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check)
		} else {
			debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check.SubProblems...)
		}
	}

	combined.DebugInfo = debugInfo
	return combined
}

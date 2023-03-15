package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
)

// NewSerialChecker creates an instance of SerialChecker.
func NewSerialChecker(d dispatch.Check) *SerialChecker {
	return &SerialChecker{d}
}

// SerialChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type SerialChecker struct {
	d dispatch.Check
}

// Check performs a check request with the provided request and context
func (cc *SerialChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) (*v1.DispatchCheckResponse, error) {
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

func (cc *SerialChecker) checkInternal(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) CheckResult {
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

	// Filter the incoming resource IDs for any which match the subject directly. For example, if we receive
	// a check for resource `user:{tom, fred, sarah}#...` and a subject of `user:sarah#...`, then we know
	// that `user:sarah#...` is a valid "member" of the resource, as it matches exactly.
	//
	// If the filtering results in no further resource IDs to check, or a result is found and a single
	// result is allowed, we terminate early.
	membershipSet, filteredResourcesIds := filterForFoundMemberResource(req.ResourceRelation, req.ResourceIds, req.Subject)
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

func (cc *SerialChecker) checkDirect(ctx context.Context, crc currentRequestContext, relation *core.Relation) CheckResult {
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
			if !onrEqualOrWildcard(tpl.Subject, crc.parentReq.Subject) {
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
	relationshipsBySubjectONR := util.NewMultiMap[string, *core.RelationTuple]()

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
		util.ForEachChunk(resourceIds, crc.maxDispatchCount, func(resourceIdChunk []string) {
			chunkCount++
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
		dispatchChunkCountHistogram.Observe(chunkCount)
	})

	// Dispatch and map to the associated resource ID(s).
	result := serialUnion(ctx, crc, toDispatch, func(ctx context.Context, crc currentRequestContext, dd directDispatch) CheckResult {
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
	})

	return combineResultWithFoundResources(result, foundResources)
}

func (cc *SerialChecker) checkUsersetRewrite(ctx context.Context, crc currentRequestContext, rewrite *core.UsersetRewrite) CheckResult {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return serialUnion(ctx, crc, rw.Union.Child, cc.runSetOperation)
	case *core.UsersetRewrite_Intersection:
		return serialAll(ctx, crc, rw.Intersection.Child, cc.runSetOperation)
	case *core.UsersetRewrite_Exclusion:
		return serialDifference(ctx, crc, rw.Exclusion.Child, cc.runSetOperation)
	default:
		return checkResultError(fmt.Errorf("unknown userset rewrite operator"), emptyMetadata)
	}
}

func (cc *SerialChecker) dispatch(ctx context.Context, crc currentRequestContext, req ValidatedCheckRequest) CheckResult {
	log.Ctx(ctx).Trace().Object("dispatch", req).Send()
	result, err := cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
	return CheckResult{result, err}
}

func (cc *SerialChecker) runSetOperation(ctx context.Context, crc currentRequestContext, childOneof *core.SetOperation_Child) CheckResult {
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

func (cc *SerialChecker) checkComputedUserset(ctx context.Context, crc currentRequestContext, cu *core.ComputedUserset, rr *core.RelationReference, resourceIds []string) CheckResult {
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

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, targetRR.Namespace, targetRR.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return noMembers()
		}

		return checkResultError(err, emptyMetadata)
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

func (cc *SerialChecker) checkTupleToUserset(ctx context.Context, crc currentRequestContext, ttu *core.TupleToUserset) CheckResult {
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
	relationshipsBySubjectONR := util.NewMultiMap[string, *core.RelationTuple]()
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
		util.ForEachChunk(resourceIds, crc.maxDispatchCount, func(resourceIdChunk []string) {
			chunkCount++
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
		dispatchChunkCountHistogram.Observe(chunkCount)
	})

	return serialUnion(
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
	)
}

// serialUnion returns whether any one of the lazy checks pass, and is used for serialUnion.
func serialUnion[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	for i := 0; i < len(children); i++ {
		result := handler(ctx, crc, children[i])

		log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
		responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
		if result.Err != nil {
			return checkResultError(result.Err, responseMetadata)
		}

		membershipSet.UnionWith(result.Resp.ResultsByResourceId)
		if membershipSet.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
			return checkResultsForMembership(membershipSet, responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// serialALl returns whether all of the lazy checks pass, and is used for intersection.
func serialAll[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	responseMetadata := emptyMetadata
	var membershipSet *MembershipSet
	for i := 0; i < len(children); i++ {
		result := handler(ctx, crc, children[i])
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
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

// serialDifference returns whether the first lazy check passes and none of the subsequent checks pass.
func serialDifference[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
) CheckResult {
	if len(children) == 0 {
		return noMembers()
	}

	if len(children) == 1 {
		return checkResultError(fmt.Errorf("difference requires more than a single child"), emptyMetadata)
	}

	responseMetadata := emptyMetadata
	membershipSet := NewMembershipSet()

	base := handler(ctx, crc, children[0])
	responseMetadata = combineResponseMetadata(responseMetadata, base.Resp.Metadata)

	if base.Err != nil {
		return checkResultError(base.Err, responseMetadata)
	}

	membershipSet.UnionWith(base.Resp.ResultsByResourceId)
	if membershipSet.IsEmpty() {
		return noMembersWithMetadata(responseMetadata)
	}

	// Subtract the remaining sets.
	for i := 1; i < len(children); i++ {
		sub := handler(ctx, crc, children[i])
		responseMetadata = combineResponseMetadata(responseMetadata, sub.Resp.Metadata)

		if sub.Err != nil {
			return checkResultError(sub.Err, responseMetadata)
		}

		membershipSet.Subtract(sub.Resp.ResultsByResourceId)
		if membershipSet.IsEmpty() {
			return noMembersWithMetadata(responseMetadata)
		}
	}

	return checkResultsForMembership(membershipSet, responseMetadata)
}

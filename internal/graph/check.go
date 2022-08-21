package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/util"
	"github.com/authzed/spicedb/pkg/datastore"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

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
	Revision decimal.Decimal
}

// currentRequestContext holds context information for the current request being
// processed.
type currentRequestContext struct {
	// parentReq is the parent request being processed.
	parentReq ValidatedCheckRequest

	// filteredResourceIds are those resource IDs to be checked after filtering for
	// found resources.
	filteredResourceIds []string

	// resultsSetting is the results setting to use for this request and all subsequent
	// requests.
	resultsSetting v1.DispatchCheckRequest_ResultsSetting
}

// Check performs a check request with the provided request and context
func (cc *ConcurrentChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) (*v1.DispatchCheckResponse, error) {
	resolved := cc.checkInternal(ctx, req, relation)
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	if req.Debug != v1.DispatchCheckRequest_ENABLE_DEBUGGING {
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
	results := make(map[string]*v1.CheckDebugTrace_ResourceCheckResult, len(req.DispatchCheckRequest.ResourceIds))
	for _, resourceId := range req.DispatchCheckRequest.ResourceIds {
		hasPermission := false
		if found, ok := resolved.Resp.ResultsByResourceId[resourceId]; ok && found.Membership == v1.DispatchCheckResponse_MEMBER {
			hasPermission = true
		}

		results[resourceId] = &v1.CheckDebugTrace_ResourceCheckResult{
			HasPermission: hasPermission,
		}
	}

	debugInfo.Check.Results = results
	resolved.Resp.Metadata.DebugInfo = debugInfo
	return resolved.Resp, resolved.Err
}

func (cc *ConcurrentChecker) checkInternal(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) CheckResult {
	if relation.GetTypeInformation() == nil && relation.GetUsersetRewrite() == nil {
		return checkResultError(
			fmt.Errorf("found relation `%s` without type information; to fix, please re-write your schema", relation.Name),
			emptyMetadata,
		)
	}

	if len(req.ResourceIds) == 0 {
		return checkResultError(
			fmt.Errorf("empty resource IDs given to dispatched check"),
			emptyMetadata,
		)
	}

	if req.Subject.ObjectId == tuple.PublicWildcard {
		return checkResultError(NewErrInvalidArgument(errors.New("cannot perform check on wildcard")), emptyMetadata)
	}

	foundResourceIds, filteredResourcesIds := filterForFoundMemberResource(req.ResourceRelation, req.ResourceIds, req.Subject)
	if len(foundResourceIds) > 0 && req.DispatchCheckRequest.ResultsSetting == v1.DispatchCheckRequest_SHORT_CIRCUIT {
		return checkResultsForResourceIds(foundResourceIds, emptyMetadata)
	}

	if len(filteredResourcesIds) == 0 {
		return noMembers()
	}

	// NOTE: We can always short circuit if only trying to find the results for a single resource ID.
	resultsSetting := req.ResultsSetting
	if len(filteredResourcesIds) == 1 {
		resultsSetting = v1.DispatchCheckRequest_SHORT_CIRCUIT
	}

	crc := currentRequestContext{
		parentReq:           req,
		filteredResourceIds: filteredResourcesIds,
		resultsSetting:      resultsSetting,
	}

	if relation.UsersetRewrite == nil {
		return combineResultWithFoundResourceIds(cc.checkDirect(ctx, crc), foundResourceIds)
	}

	return combineResultWithFoundResourceIds(cc.checkUsersetRewrite(ctx, crc, relation.UsersetRewrite), foundResourceIds)
}

func onrEqual(lhs, rhs *core.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

func onrEqualOrWildcard(tpl, target *core.ObjectAndRelation) bool {
	return onrEqual(tpl, target) || (tpl.Namespace == target.Namespace && tpl.ObjectId == tuple.PublicWildcard)
}

type directDispatch struct {
	resourceType *core.RelationReference
	resourceIds  []string
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, crc currentRequestContext) CheckResult {
	log.Ctx(ctx).Trace().Object("direct", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)

	// TODO(jschorr): Use type information to further optimize this query.
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      crc.filteredResourceIds,
		OptionalResourceRelation: crc.parentReq.ResourceRelation.Relation,
	})
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()

	// Find the subjects over which to dispatch.
	foundResourceIds := []string{}
	subjectsToDispatch := tuple.NewONRByTypeSet()
	resourceIdsBySubjectId := util.NewMultiMap[string, string]()

	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		if onrEqualOrWildcard(tpl.Subject, crc.parentReq.Subject) {
			foundResourceIds = append(foundResourceIds, tpl.ResourceAndRelation.ObjectId)
			if crc.resultsSetting == v1.DispatchCheckRequest_SHORT_CIRCUIT {
				return checkResultsForResourceIds(foundResourceIds, emptyMetadata)
			}
			continue
		}

		if tpl.Subject.Relation != Ellipsis {
			subjectsToDispatch.Add(tpl.Subject)
			resourceIdsBySubjectId.Add(tuple.StringONR(tpl.Subject), tpl.ResourceAndRelation.ObjectId)
		}
	}

	// Convert the subjects into batched requests.
	toDispatch := make([]directDispatch, 0, subjectsToDispatch.Len())
	subjectsToDispatch.ForEachType(func(rr *core.RelationReference, resourceIds []string) {
		util.ForEachChunk(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) {
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
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

		return mapResourceIds(childResult, dd.resourceType, resourceIdsBySubjectId)
	}, cc.concurrencyLimit)

	return combineResultWithFoundResourceIds(result, foundResourceIds)
}

func mapResourceIds(result CheckResult, resourceType *core.RelationReference, resourceIdsBySubjectId *util.MultiMap[string, string]) CheckResult {
	// Map any resources found to the parent resource IDs.
	mappedResourceIds := []string{}
	for foundResourceId, result := range result.Resp.ResultsByResourceId {
		if result.Membership != v1.DispatchCheckResponse_MEMBER {
			continue
		}

		mappedResourceIds = append(mappedResourceIds,
			resourceIdsBySubjectId.Get(tuple.StringONR(&core.ObjectAndRelation{
				Namespace: resourceType.Namespace,
				ObjectId:  foundResourceId,
				Relation:  resourceType.Relation,
			}))...)
	}

	if len(mappedResourceIds) == 0 {
		return noMembers()
	}

	return checkResultsForResourceIds(mappedResourceIds, result.Resp.Metadata)
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

func (cc *ConcurrentChecker) dispatch(ctx context.Context, crc currentRequestContext, req ValidatedCheckRequest) CheckResult {
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
			panic("computed userset for tupleset without tuples")
		}

		startNamespace = rr.Namespace
		targetResourceIds = resourceIds
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if rr != nil {
			panic("computed userset for tupleset with wrong object type")
		}

		startNamespace = crc.parentReq.ResourceRelation.Namespace
		targetResourceIds = crc.filteredResourceIds
	}

	targetRR := &core.RelationReference{
		Namespace: startNamespace,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	foundResourceIds, updatedTargetResourceIds := filterForFoundMemberResource(targetRR, targetResourceIds, crc.parentReq.Subject)
	if (len(foundResourceIds) > 0 && crc.resultsSetting == v1.DispatchCheckRequest_SHORT_CIRCUIT) || len(updatedTargetResourceIds) == 0 {
		return checkResultsForResourceIds(foundResourceIds, emptyMetadata)
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
	return combineResultWithFoundResourceIds(result, foundResourceIds)
}

func filterForFoundMemberResource(resourceRelation *core.RelationReference, resourceIds []string, subject *core.ObjectAndRelation) ([]string, []string) {
	if resourceRelation.Namespace != subject.Namespace || resourceRelation.Relation != subject.Relation {
		return nil, resourceIds
	}

	for index, resourceId := range resourceIds {
		if subject.ObjectId == resourceId {
			return []string{resourceId}, append(resourceIds[:index], resourceIds[index+1:]...)
		}
	}

	return nil, resourceIds
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, crc currentRequestContext, ttu *core.TupleToUserset) CheckResult {
	log.Ctx(ctx).Trace().Object("ttu", crc.parentReq).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(crc.parentReq.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             crc.parentReq.ResourceRelation.Namespace,
		OptionalResourceIds:      crc.filteredResourceIds,
		OptionalResourceRelation: ttu.Tupleset.Relation,
	})
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()

	subjectsToDispatch := tuple.NewONRByTypeSet()
	resourceIdsBySubjectId := util.NewMultiMap[string, string]()
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		subjectsToDispatch.Add(tpl.Subject)
		resourceIdsBySubjectId.Add(tuple.StringONR(tpl.Subject), tpl.ResourceAndRelation.ObjectId)
	}

	// Convert the subjects into batched requests.
	toDispatch := make([]directDispatch, 0, subjectsToDispatch.Len())
	subjectsToDispatch.ForEachType(func(rr *core.RelationReference, resourceIds []string) {
		util.ForEachChunk(resourceIds, maxDispatchChunkSize, func(resourceIdChunk []string) {
			toDispatch = append(toDispatch, directDispatch{
				resourceType: rr,
				resourceIds:  resourceIdChunk,
			})
		})
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

			if childResult.Err != nil {
				return childResult
			}

			return mapResourceIds(childResult, dd.resourceType, resourceIdsBySubjectId)
		},
		cc.concurrencyLimit,
	)
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

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)

	dispatcherCleanup := dispatchAllAsync(childCtx, crc, children, handler, resultChan, concurrencyLimit)

	defer func() {
		cancelFn()
		dispatcherCleanup()
		close(resultChan)
	}()

	responseMetadata := emptyMetadata
	responseResults := make(map[string]*v1.DispatchCheckResponse_ResourceCheckResult, len(crc.filteredResourceIds))

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			for resourceId, result := range result.Resp.ResultsByResourceId {
				responseResults[resourceId] = result
				if crc.resultsSetting == v1.DispatchCheckRequest_SHORT_CIRCUIT && result.Membership == v1.DispatchCheckResponse_MEMBER {
					return checkResults(responseResults, responseMetadata)
				}
			}

		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResults(responseResults, responseMetadata)
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

	responseMetadata := emptyMetadata
	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)

	cleanupFunc := dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIds: crc.filteredResourceIds,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
	}, children, handler, resultChan, concurrencyLimit)

	defer func() {
		cancelFn()
		cleanupFunc()
		close(resultChan)
	}()

	var foundForResourceIds *util.Set[string]

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			resourceIdsWithMembership := util.NewSet[string]()
			resourceIdsWithMembership.Extend(filterToResourceIdsWithMembership(result.Resp.ResultsByResourceId))

			if foundForResourceIds == nil {
				foundForResourceIds = resourceIdsWithMembership
			} else {
				foundForResourceIds.IntersectionDifference(resourceIdsWithMembership)
				if foundForResourceIds.IsEmpty() {
					return noMembers()
				}
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResultsForResourceIds(foundForResourceIds.AsSlice(), responseMetadata)
}

// difference returns whether the first lazy check passes and none of the supsequent checks pass.
func difference[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	childCtx, cancelFn := context.WithCancel(ctx)

	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(children)-1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		result := handler(childCtx, crc, children[0])
		baseChan <- result
		wg.Done()
	}()

	cleanupFunc := dispatchAllAsync(childCtx, currentRequestContext{
		parentReq:           crc.parentReq,
		filteredResourceIds: crc.filteredResourceIds,
		resultsSetting:      v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
	}, children[1:], handler, othersChan, concurrencyLimit-1)

	defer func() {
		cancelFn()
		cleanupFunc()
		close(othersChan)
		wg.Wait()
		close(baseChan)
	}()

	responseMetadata := emptyMetadata
	foundResourceIds := util.NewSet[string]()

	// Wait for the base set to return.
	select {
	case base := <-baseChan:
		responseMetadata = combineResponseMetadata(responseMetadata, base.Resp.Metadata)

		if base.Err != nil {
			return checkResultError(base.Err, responseMetadata)
		}

		foundResourceIds.Extend(filterToResourceIdsWithMembership(base.Resp.ResultsByResourceId))
		if foundResourceIds.IsEmpty() {
			return noMembers()
		}

	case <-ctx.Done():
		return checkResultError(NewRequestCanceledErr(), responseMetadata)
	}

	// For the remaining sets to return.
	for i := 1; i < len(children); i++ {
		select {
		case sub := <-othersChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Resp.Metadata)

			if sub.Err != nil {
				return checkResultError(sub.Err, responseMetadata)
			}

			resourceIdsWithMembership := util.NewSet[string]()
			resourceIdsWithMembership.Extend(filterToResourceIdsWithMembership(sub.Resp.ResultsByResourceId))

			foundResourceIds.RemoveAll(resourceIdsWithMembership)
			if foundResourceIds.IsEmpty() {
				return noMembers()
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResultsForResourceIds(foundResourceIds.AsSlice(), responseMetadata)
}

func dispatchAllAsync[T any](
	ctx context.Context,
	crc currentRequestContext,
	children []T,
	handler func(ctx context.Context, crc currentRequestContext, child T) CheckResult,
	resultChan chan<- CheckResult,
	concurrencyLimit uint16,
) func() {
	sem := make(chan struct{}, concurrencyLimit)
	var wg sync.WaitGroup

	runHandler := func(child T) {
		result := handler(ctx, crc, child)
		resultChan <- result
		<-sem
		wg.Done()
	}

	wg.Add(1)
	go func() {
	dispatcher:
		for _, currentChild := range children {
			currentChild := currentChild
			select {
			case sem <- struct{}{}:
				wg.Add(1)
				go runHandler(currentChild)
			case <-ctx.Done():
				break dispatcher
			}
		}
		wg.Done()
	}()

	return func() {
		wg.Wait()
		close(sem)
	}
}

func noMembers() CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: emptyMetadata,
		},
		nil,
	}
}

func checkResultsForResourceIds(resourceIds []string, subProblemMetadata *v1.ResponseMeta) CheckResult {
	results := make(map[string]*v1.DispatchCheckResponse_ResourceCheckResult, len(resourceIds))
	for _, resourceId := range resourceIds {
		results[resourceId] = &v1.DispatchCheckResponse_ResourceCheckResult{
			Membership: v1.DispatchCheckResponse_MEMBER,
		}
	}
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:            ensureMetadata(subProblemMetadata),
			ResultsByResourceId: results,
		},
		nil,
	}
}

func checkResults(results map[string]*v1.DispatchCheckResponse_ResourceCheckResult, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:            ensureMetadata(subProblemMetadata),
			ResultsByResourceId: results,
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

func filterToResourceIdsWithMembership(results map[string]*v1.DispatchCheckResponse_ResourceCheckResult) []string {
	members := make([]string, 0, len(results))
	for resourceId, result := range results {
		if result.Membership == v1.DispatchCheckResponse_MEMBER {
			members = append(members, resourceId)
		}
	}
	return members
}

func combineResultWithFoundResourceIds(result CheckResult, foundResourceIds []string) CheckResult {
	if result.Err != nil {
		return result
	}

	if len(foundResourceIds) == 0 {
		return result
	}

	for _, resourceId := range foundResourceIds {
		if len(resourceId) == 0 {
			panic("given empty resource id")
		}

		if result.Resp.ResultsByResourceId == nil {
			result.Resp.ResultsByResourceId = map[string]*v1.DispatchCheckResponse_ResourceCheckResult{}
		}

		result.Resp.ResultsByResourceId[resourceId] = &v1.DispatchCheckResponse_ResourceCheckResult{
			Membership: v1.DispatchCheckResponse_MEMBER,
		}
	}

	return result
}

func combineResponseMetadata(existing *v1.ResponseMeta, responseMetadata *v1.ResponseMeta) *v1.ResponseMeta {
	combined := &v1.ResponseMeta{
		DispatchCount:       existing.DispatchCount + responseMetadata.DispatchCount,
		DepthRequired:       max(existing.DepthRequired, responseMetadata.DepthRequired),
		CachedDispatchCount: existing.CachedDispatchCount + responseMetadata.CachedDispatchCount,
	}

	if responseMetadata.DebugInfo == nil {
		return combined
	}

	debugInfo := existing.DebugInfo
	if debugInfo == nil {
		debugInfo = &v1.DebugInformation{
			Check: &v1.CheckDebugTrace{},
		}
	}

	if responseMetadata.DebugInfo.Check.Request != nil {
		debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check)
	} else {
		debugInfo.Check.SubProblems = append(debugInfo.Check.SubProblems, responseMetadata.DebugInfo.Check.SubProblems...)
	}

	combined.DebugInfo = debugInfo
	return combined
}

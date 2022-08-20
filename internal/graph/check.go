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

func onrEqual(lhs, rhs *core.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

// ValidatedCheckRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedCheckRequest struct {
	*v1.DispatchCheckRequest
	Revision decimal.Decimal
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

	debugInfo.Check.HasPermission = resolved.Resp.Membership == v1.DispatchCheckResponse_MEMBER
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

	if req.Subject.ObjectId == tuple.PublicWildcard {
		return checkResultError(NewErrInvalidArgument(errors.New("cannot perform check on wildcard")), emptyMetadata)
	}

	if onrEqual(req.Subject, req.ResourceAndRelation) {
		// If we have found the goal's ONR, then we know that the ONR is a member.
		return checkResult(v1.DispatchCheckResponse_MEMBER, emptyMetadata)
	}

	if relation.UsersetRewrite == nil {
		return cc.checkDirect(ctx, req)
	}

	return cc.checkUsersetRewrite(ctx, req, relation.UsersetRewrite)
}

func onrEqualOrWildcard(tpl, target *core.ObjectAndRelation) bool {
	return onrEqual(tpl, target) || (tpl.Namespace == target.Namespace && tpl.ObjectId == tuple.PublicWildcard)
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req ValidatedCheckRequest) CheckResult {
	log.Ctx(ctx).Trace().Object("direct", req).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)

	// TODO(jschorr): Use type information to further optimize this query.
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             req.ResourceAndRelation.Namespace,
		OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
		OptionalResourceRelation: req.ResourceAndRelation.Relation,
	})
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()

	var requestsToDispatch []ValidatedCheckRequest
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		if onrEqualOrWildcard(tpl.Subject, req.Subject) {
			return checkResult(v1.DispatchCheckResponse_MEMBER, emptyMetadata)
		}

		if tpl.Subject.Relation != Ellipsis {
			// We need to recursively call check here, potentially changing namespaces
			requestsToDispatch = append(requestsToDispatch, ValidatedCheckRequest{
				&v1.DispatchCheckRequest{
					ResourceAndRelation: tpl.Subject,
					Subject:             req.Subject,

					Metadata: decrementDepth(req.Metadata),
					Debug:    req.Debug,
				},
				req.Revision,
			})
		}
	}
	return union(ctx, req, requestsToDispatch, cc.dispatch, cc.concurrencyLimit)
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, req ValidatedCheckRequest, rewrite *core.UsersetRewrite) CheckResult {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return union(ctx, req, rw.Union.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Intersection:
		return all(ctx, req, rw.Intersection.Child, cc.runSetOperation, cc.concurrencyLimit)
	case *core.UsersetRewrite_Exclusion:
		return difference(ctx, req, rw.Exclusion.Child, cc.runSetOperation, cc.concurrencyLimit)
	default:
		return checkResultError(fmt.Errorf("unknown userset rewrite operator"), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) dispatch(ctx context.Context, parentReq ValidatedCheckRequest, req ValidatedCheckRequest) CheckResult {
	log.Ctx(ctx).Trace().Object("dispatch", req).Send()
	result, err := cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
	return CheckResult{result, err}
}

func (cc *ConcurrentChecker) runSetOperation(ctx context.Context, parentReq ValidatedCheckRequest, childOneof *core.SetOperation_Child) CheckResult {
	switch child := childOneof.ChildType.(type) {
	case *core.SetOperation_Child_XThis:
		return checkResultError(errors.New("use of _this is unsupported; please rewrite your schema"), emptyMetadata)
	case *core.SetOperation_Child_ComputedUserset:
		return cc.checkComputedUserset(ctx, parentReq, child.ComputedUserset, nil)
	case *core.SetOperation_Child_UsersetRewrite:
		return cc.checkUsersetRewrite(ctx, parentReq, child.UsersetRewrite)
	case *core.SetOperation_Child_TupleToUserset:
		return cc.checkTupleToUserset(ctx, parentReq, child.TupleToUserset)
	case *core.SetOperation_Child_XNil:
		return notMember()
	default:
		return checkResultError(fmt.Errorf("unknown set operation child `%T` in check", child), emptyMetadata)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, parentReq ValidatedCheckRequest, cu *core.ComputedUserset, tpl *core.RelationTuple) CheckResult {
	var start *core.ObjectAndRelation
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.Subject
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ResourceAndRelation
		} else {
			start = parentReq.ResourceAndRelation
		}
	}

	targetOnr := &core.ObjectAndRelation{
		Namespace: start.Namespace,
		ObjectId:  start.ObjectId,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	if onrEqual(parentReq.Subject, targetOnr) {
		return alwaysMember()
	}

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(parentReq.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return notMember()
		}

		return checkResultError(err, emptyMetadata)
	}

	return cc.dispatch(ctx, parentReq, ValidatedCheckRequest{
		&v1.DispatchCheckRequest{
			ResourceAndRelation: targetOnr,
			Subject:             parentReq.Subject,
			Metadata:            decrementDepth(parentReq.Metadata),
			Debug:               parentReq.Debug,
		},
		parentReq.Revision,
	})
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, req ValidatedCheckRequest, ttu *core.TupleToUserset) CheckResult {
	log.Ctx(ctx).Trace().Object("ttu", req).Send()
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
		ResourceType:             req.ResourceAndRelation.Namespace,
		OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
		OptionalResourceRelation: ttu.Tupleset.Relation,
	})
	if err != nil {
		return checkResultError(NewCheckFailureErr(err), emptyMetadata)
	}
	defer it.Close()

	var tuplesToDispatch []*core.RelationTuple
	for tpl := it.Next(); tpl != nil; tpl = it.Next() {
		if it.Err() != nil {
			return checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
		}

		tuplesToDispatch = append(tuplesToDispatch, tpl)
	}

	return union(
		ctx,
		req,
		tuplesToDispatch,
		func(ctx context.Context, parentReq ValidatedCheckRequest, tpl *core.RelationTuple) CheckResult {
			return cc.checkComputedUserset(ctx, parentReq, ttu.ComputedUserset, tpl)
		},
		cc.concurrencyLimit,
	)
}

// union returns whether any one of the lazy checks pass, and is used for union.
func union[T any](
	ctx context.Context,
	parentReq ValidatedCheckRequest,
	children []T,
	handler func(ctx context.Context, parentReq ValidatedCheckRequest, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
	}

	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)

	dispatcherCleanup := dispatchAllAsync(childCtx, parentReq, children, handler, resultChan, concurrencyLimit)

	defer func() {
		cancelFn()
		dispatcherCleanup()
		close(resultChan)
	}()

	responseMetadata := emptyMetadata

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)

			if result.Err == nil && result.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_MEMBER, responseMetadata)
			}

			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}
		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, responseMetadata)
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all[T any](
	ctx context.Context,
	parentReq ValidatedCheckRequest,
	children []T,
	handler func(ctx context.Context, parentReq ValidatedCheckRequest, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	if len(children) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
	}

	responseMetadata := emptyMetadata
	resultChan := make(chan CheckResult, len(children))
	childCtx, cancelFn := context.WithCancel(ctx)

	cleanupFunc := dispatchAllAsync(childCtx, parentReq, children, handler, resultChan, concurrencyLimit)

	defer func() {
		cancelFn()
		cleanupFunc()
		close(resultChan)
	}()

	for i := 0; i < len(children); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return checkResultError(result.Err, responseMetadata)
			}

			if result.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, responseMetadata)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, responseMetadata)
}

// difference returns whether the first lazy check passes and none of the supsequent checks pass.
func difference[T any](
	ctx context.Context,
	parentReq ValidatedCheckRequest,
	children []T,
	handler func(ctx context.Context, parentReq ValidatedCheckRequest, child T) CheckResult,
	concurrencyLimit uint16,
) CheckResult {
	childCtx, cancelFn := context.WithCancel(ctx)

	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(children)-1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		result := handler(childCtx, parentReq, children[0])
		baseChan <- result
		wg.Done()
	}()

	cleanupFunc := dispatchAllAsync(childCtx, parentReq, children[1:], handler, othersChan, concurrencyLimit-1)

	defer func() {
		cancelFn()
		cleanupFunc()
		close(othersChan)
		wg.Wait()
		close(baseChan)
	}()

	responseMetadata := emptyMetadata

	for i := 0; i < len(children); i++ {
		select {
		case base := <-baseChan:
			responseMetadata = combineResponseMetadata(responseMetadata, base.Resp.Metadata)

			if base.Err != nil {
				return checkResultError(base.Err, responseMetadata)
			}

			if base.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, responseMetadata)
			}
		case sub := <-othersChan:
			responseMetadata = combineResponseMetadata(responseMetadata, sub.Resp.Metadata)

			if sub.Err != nil {
				return checkResultError(sub.Err, responseMetadata)
			}

			if sub.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, responseMetadata)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, responseMetadata)
}

func dispatchAllAsync[T any](
	ctx context.Context,
	parentReq ValidatedCheckRequest,
	children []T,
	handler func(ctx context.Context, parentReq ValidatedCheckRequest, child T) CheckResult,
	resultChan chan<- CheckResult,
	concurrencyLimit uint16,
) func() {
	sem := make(chan struct{}, concurrencyLimit)
	var wg sync.WaitGroup

	runHandler := func(child T) {
		result := handler(ctx, parentReq, child)
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

func alwaysMember() CheckResult {
	return checkResult(v1.DispatchCheckResponse_MEMBER, emptyMetadata)
}

func notMember() CheckResult {
	return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
}

func checkResult(membership v1.DispatchCheckResponse_Membership, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:   ensureMetadata(subProblemMetadata),
			Membership: membership,
		},
		nil,
	}
}

func checkResultError(err error, subProblemMetadata *v1.ResponseMeta) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata:   ensureMetadata(subProblemMetadata),
			Membership: v1.DispatchCheckResponse_UNKNOWN,
		},
		err,
	}
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

package graph

import (
	"context"
	"errors"
	"fmt"

	v1_proto "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check) *ConcurrentChecker {
	return &ConcurrentChecker{d: d}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d dispatch.Check
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
	var directFunc ReduceableCheckFunc

	// TODO(jschorr): Turn into an error once v0 API has been removed.
	if relation.GetTypeInformation() == nil && relation.GetUsersetRewrite() == nil {
		log.Ctx(ctx).Warn().Str("relation", relation.Name).Msg("Found relation without type information. Please switch to using schema. This will be an error in the future!")
	}

	if req.Subject.ObjectId == tuple.PublicWildcard {
		directFunc = checkError(NewErrInvalidArgument(errors.New("cannot perform check on wildcard")))
	} else if onrEqual(req.Subject, req.ObjectAndRelation) {
		// If we have found the goal's ONR, then we know that the ONR is a member.
		directFunc = alwaysMember()
	} else if relation.UsersetRewrite == nil {
		directFunc = cc.checkDirect(ctx, req)
	} else {
		directFunc = cc.checkUsersetRewrite(ctx, req, relation.UsersetRewrite)
	}

	resolved := union(ctx, []ReduceableCheckFunc{directFunc})
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	return resolved.Resp, resolved.Err
}

func (cc *ConcurrentChecker) dispatch(req ValidatedCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("dispatch", req).Send()
		result, err := cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
		resultChan <- CheckResult{result, err}
	}
}

func onrEqualOrWildcard(tpl, target *core.ObjectAndRelation) bool {
	return onrEqual(tpl, target) || (tpl.Namespace == target.Namespace && tpl.ObjectId == tuple.PublicWildcard)
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req ValidatedCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("direct", req).Send()
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)

		// TODO(jschorr): Use type information to further optimize this query.
		it, err := ds.QueryRelationships(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ObjectAndRelation.Namespace,
			OptionalResourceId: req.ObjectAndRelation.ObjectId,
			OptionalRelation:   req.ObjectAndRelation.Relation,
		})
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), emptyMetadata)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			tplUserset := tpl.User.GetUserset()
			if onrEqualOrWildcard(tplUserset, req.Subject) {
				resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, emptyMetadata)
				return
			}
			if tplUserset.Relation != Ellipsis {
				// We need to recursively call check here, potentially changing namespaces
				requestsToDispatch = append(requestsToDispatch, cc.dispatch(ValidatedCheckRequest{
					&v1.DispatchCheckRequest{
						ObjectAndRelation: tplUserset,
						Subject:           req.Subject,

						Metadata: decrementDepth(req.Metadata),
					},
					req.Revision,
				}))
			}
		}
		if it.Err() != nil {
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
			return
		}
		resultChan <- union(ctx, requestsToDispatch)
	}
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, req ValidatedCheckRequest, usr *core.UsersetRewrite) ReduceableCheckFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return cc.checkSetOperation(ctx, req, rw.Union, union)
	case *core.UsersetRewrite_Intersection:
		return cc.checkSetOperation(ctx, req, rw.Intersection, all)
	case *core.UsersetRewrite_Exclusion:
		return cc.checkSetOperation(ctx, req, rw.Exclusion, difference)
	default:
		return AlwaysFail
	}
}

func (cc *ConcurrentChecker) checkSetOperation(ctx context.Context, req ValidatedCheckRequest, so *core.SetOperation, reducer Reducer) ReduceableCheckFunc {
	var requests []ReduceableCheckFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			// TODO(jschorr): Turn into an error once v0 API has been removed.
			log.Ctx(ctx).Warn().Stringer("operation", so).Msg("Use of _this is deprecated and will soon be an error! Please switch to using schema!")
			requests = append(requests, cc.checkDirect(ctx, req))
		case *core.SetOperation_Child_ComputedUserset:
			requests = append(requests, cc.checkComputedUserset(ctx, req, child.ComputedUserset, nil))
		case *core.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cc.checkUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *core.SetOperation_Child_TupleToUserset:
			requests = append(requests, cc.checkTupleToUserset(ctx, req, child.TupleToUserset))
		case *core.SetOperation_Child_XNil:
			requests = append(requests, notMember())
		default:
			return checkError(fmt.Errorf("unknown set operation child `%T` in check", child))
		}
	}
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("setOperation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, requests)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, req ValidatedCheckRequest, cu *core.ComputedUserset, tpl *core.RelationTuple) ReduceableCheckFunc {
	var start *core.ObjectAndRelation
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.User.GetUserset()
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ObjectAndRelation
		} else {
			start = req.ObjectAndRelation
		}
	}

	targetOnr := &core.ObjectAndRelation{
		Namespace: start.Namespace,
		ObjectId:  start.ObjectId,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	if onrEqual(req.Subject, targetOnr) {
		return alwaysMember()
	}

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return notMember()
		}

		return checkError(err)
	}

	return cc.dispatch(ValidatedCheckRequest{
		&v1.DispatchCheckRequest{
			ObjectAndRelation: targetOnr,
			Subject:           req.Subject,
			Metadata:          decrementDepth(req.Metadata),
		},
		req.Revision,
	})
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, req ValidatedCheckRequest, ttu *core.TupleToUserset) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("ttu", req).Send()
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
		it, err := ds.QueryRelationships(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ObjectAndRelation.Namespace,
			OptionalResourceId: req.ObjectAndRelation.ObjectId,
			OptionalRelation:   ttu.Tupleset.Relation,
		})
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), emptyMetadata)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, cc.checkComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), emptyMetadata)
			return
		}

		resultChan <- union(ctx, requestsToDispatch)
	}
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
	}

	responseMetadata := emptyMetadata
	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	for i := 0; i < len(requests); i++ {
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

// checkError returns the error.
func checkError(err error) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResultError(err, emptyMetadata)
	}
}

// alwaysMember returns that the check always passes.
func alwaysMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, emptyMetadata)
	}
}

// notMember returns that the check always returns false.
func notMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
	}
}

// union returns whether any one of the lazy checks pass, and is used for union.
func union(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, emptyMetadata)
	}

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	responseMetadata := emptyMetadata

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			log.Ctx(ctx).Trace().Object("anyResult", result.Resp).Send()
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)

			if result.Err == nil && result.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_MEMBER, result.Resp.Metadata)
			}
			if result.Err != nil {
				return checkResultError(result.Err, result.Resp.Metadata)
			}
		case <-ctx.Done():
			log.Ctx(ctx).Trace().Msg("anyCanceled")
			return checkResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, responseMetadata)
}

// difference returns whether the first lazy check passes and none of the supsequent checks pass.
func difference(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(requests)-1)

	go requests[0](childCtx, baseChan)
	for _, req := range requests[1:] {
		go req(childCtx, othersChan)
	}

	responseMetadata := emptyMetadata

	for i := 0; i < len(requests); i++ {
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

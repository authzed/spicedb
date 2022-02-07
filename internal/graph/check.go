package graph

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1_proto "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check, nsm namespace.Manager) *ConcurrentChecker {
	return &ConcurrentChecker{d: d, nsm: nsm}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d   dispatch.Check
	nsm namespace.Manager
}

func onrEqual(lhs, rhs *v0.ObjectAndRelation) bool {
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
func (cc *ConcurrentChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *v0.Relation) (*v1.DispatchCheckResponse, error) {
	var directFunc ReduceableCheckFunc

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

	resolved := any(ctx, []ReduceableCheckFunc{directFunc})
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

func onrEqualOrWildcard(tpl, target *v0.ObjectAndRelation) bool {
	return onrEqual(tpl, target) || (tpl.Namespace == target.Namespace && tpl.ObjectId == tuple.PublicWildcard)
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req ValidatedCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("direct", req).Send()
		ds := datastoremw.MustFromContext(ctx)

		// TODO(jschorr): Use type information to further optimize this query.
		it, err := ds.QueryTuples(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ObjectAndRelation.Namespace,
			OptionalResourceId: req.ObjectAndRelation.ObjectId,
			OptionalRelation:   req.ObjectAndRelation.Relation,
		}, req.Revision)
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
		resultChan <- any(ctx, requestsToDispatch)
	}
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, req ValidatedCheckRequest, usr *v0.UsersetRewrite) ReduceableCheckFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		return cc.checkSetOperation(ctx, req, rw.Union, any)
	case *v0.UsersetRewrite_Intersection:
		return cc.checkSetOperation(ctx, req, rw.Intersection, all)
	case *v0.UsersetRewrite_Exclusion:
		return cc.checkSetOperation(ctx, req, rw.Exclusion, difference)
	default:
		return AlwaysFail
	}
}

func (cc *ConcurrentChecker) checkSetOperation(ctx context.Context, req ValidatedCheckRequest, so *v0.SetOperation, reducer Reducer) ReduceableCheckFunc {
	var requests []ReduceableCheckFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *v0.SetOperation_Child_XThis:
			requests = append(requests, cc.checkDirect(ctx, req))
		case *v0.SetOperation_Child_ComputedUserset:
			requests = append(requests, cc.checkComputedUserset(ctx, req, child.ComputedUserset, nil))
		case *v0.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cc.checkUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *v0.SetOperation_Child_TupleToUserset:
			requests = append(requests, cc.checkTupleToUserset(ctx, req, child.TupleToUserset))
		}
	}
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("setOperation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, requests)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, req ValidatedCheckRequest, cu *v0.ComputedUserset, tpl *v0.RelationTuple) ReduceableCheckFunc {
	var start *v0.ObjectAndRelation
	if cu.Object == v0.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.User.GetUserset()
	} else if cu.Object == v0.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ObjectAndRelation
		} else {
			start = req.ObjectAndRelation
		}
	}

	targetOnr := &v0.ObjectAndRelation{
		Namespace: start.Namespace,
		ObjectId:  start.ObjectId,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	if onrEqual(req.Subject, targetOnr) {
		return alwaysMember()
	}

	// Check if the target relation exists. If not, return nothing.
	err := cc.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true, req.Revision)
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

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, req ValidatedCheckRequest, ttu *v0.TupleToUserset) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Ctx(ctx).Trace().Object("ttu", req).Send()
		ds := datastoremw.MustFromContext(ctx)
		it, err := ds.QueryTuples(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ObjectAndRelation.Namespace,
			OptionalResourceId: req.ObjectAndRelation.ObjectId,
			OptionalRelation:   ttu.Tupleset.Relation,
		}, req.Revision)
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

		resultChan <- any(ctx, requestsToDispatch)
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

// any returns whether any one of the lazy checks pass, and is used for union.
func any(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
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

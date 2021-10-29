package graph

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check, ds datastore.GraphDatastore, nsm namespace.Manager) *ConcurrentChecker {
	return &ConcurrentChecker{d: d, ds: ds, nsm: nsm}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d   dispatch.Check
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

func onrEqual(lhs, rhs *v0.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

// Check performs a check request with the provided request and context
func (cc *ConcurrentChecker) Check(ctx context.Context, req *v1.DispatchCheckRequest, relation *v0.Relation) (*v1.DispatchCheckResponse, error) {
	var directFunc ReduceableCheckFunc

	if onrEqual(req.Subject, req.ObjectAndRelation) {
		// If we have found the goal's ONR, then we know that the ONR is a member.
		directFunc = alwaysMember()
	} else if relation.UsersetRewrite == nil {
		directFunc = cc.checkDirect(ctx, req)
	} else {
		directFunc = cc.checkUsersetRewrite(ctx, req, relation.UsersetRewrite)
	}

	resolved := any(ctx, []ReduceableCheckFunc{directFunc})
	return resolved.Resp, resolved.Err
}

func (cc *ConcurrentChecker) dispatch(req *v1.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("dispatch", req).Send()
		result, err := cc.d.DispatchCheck(ctx, req)
		resultChan <- CheckResult{result, err}
	}
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req *v1.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 0, 0)
			return
		}

		log.Trace().Object("direct", req).Send()
		it, err := cc.ds.QueryTuples(datastore.TupleQueryResourceFilter{
			ResourceType:             req.ObjectAndRelation.Namespace,
			OptionalResourceID:       req.ObjectAndRelation.ObjectId,
			OptionalResourceRelation: req.ObjectAndRelation.Relation,
		}, requestRevision).Execute(ctx)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 1, 0)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			tplUserset := tpl.User.GetUserset()
			if onrEqual(tplUserset, req.Subject) {
				resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, 1, 0)
				return
			}
			if tplUserset.Relation != Ellipsis {
				// We need to recursively call check here, potentially changing namespaces
				requestsToDispatch = append(requestsToDispatch, cc.dispatch(&v1.DispatchCheckRequest{
					ObjectAndRelation: tplUserset,
					Subject:           req.Subject,

					Metadata: decrementDepth(req.Metadata),
				}))
			}
		}
		if it.Err() != nil {
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), 1, 0)
			return
		}
		resultChan <- any(ctx, requestsToDispatch)
	}
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, req *v1.DispatchCheckRequest, usr *v0.UsersetRewrite) ReduceableCheckFunc {
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

func (cc *ConcurrentChecker) checkSetOperation(ctx context.Context, req *v1.DispatchCheckRequest, so *v0.SetOperation, reducer Reducer) ReduceableCheckFunc {
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
		log.Trace().Object("set operation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, requests)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(ctx context.Context, req *v1.DispatchCheckRequest, cu *v0.ComputedUserset, tpl *v0.RelationTuple) ReduceableCheckFunc {
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
	err := cc.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return notMember()
		}

		return checkError(err)
	}

	return cc.dispatch(&v1.DispatchCheckRequest{
		ObjectAndRelation: targetOnr,
		Subject:           req.Subject,
		Metadata:          decrementDepth(req.Metadata),
	})
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, req *v1.DispatchCheckRequest, ttu *v0.TupleToUserset) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 0, 0)
			return
		}

		log.Trace().Object("ttu", req).Send()
		it, err := cc.ds.QueryTuples(datastore.TupleQueryResourceFilter{
			ResourceType:             req.ObjectAndRelation.Namespace,
			OptionalResourceID:       req.ObjectAndRelation.ObjectId,
			OptionalResourceRelation: ttu.Tupleset.Relation,
		}, requestRevision).Execute(ctx)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 1, 0)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, cc.checkComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), 1, 0)
			return
		}

		resultChan <- any(ctx, requestsToDispatch)
	}
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0, 0)
	}

	var totalRequestCount uint32
	var maxDepthRequired uint32

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			totalRequestCount += result.Resp.Metadata.DispatchCount
			maxDepthRequired = max(maxDepthRequired, result.Resp.Metadata.DepthRequired)
			if result.Err != nil {
				return checkResultError(result.Err, totalRequestCount, maxDepthRequired)
			}

			if result.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount, maxDepthRequired)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), totalRequestCount, maxDepthRequired)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount, maxDepthRequired)
}

// checkError returns the error.
func checkError(err error) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResultError(err, 0, 0)
	}
}

// alwaysMember returns that the check always passes.
func alwaysMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, 0, 0)
	}
}

// notMember returns that the check always returns false.
func notMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0, 0)
	}
}

// any returns whether any one of the lazy checks pass, and is used for union.
func any(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0, 0)
	}

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	var totalRequestCount uint32
	var maxDepthRequired uint32

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			log.Trace().Object("any result", result.Resp).Send()
			totalRequestCount += result.Resp.Metadata.DispatchCount
			maxDepthRequired = max(maxDepthRequired, result.Resp.Metadata.DepthRequired)
			if result.Err == nil && result.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount, maxDepthRequired)
			}
			if result.Err != nil {
				return checkResultError(result.Err, totalRequestCount, maxDepthRequired)
			}
		case <-ctx.Done():
			log.Trace().Msg("any canceled")
			return checkResultError(NewRequestCanceledErr(), totalRequestCount, maxDepthRequired)
		}
	}

	return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount, maxDepthRequired)
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

	var totalRequestCount uint32
	var maxDepthRequired uint32

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			totalRequestCount += base.Resp.Metadata.DispatchCount
			maxDepthRequired = max(maxDepthRequired, base.Resp.Metadata.DepthRequired)

			if base.Err != nil {
				return checkResultError(base.Err, totalRequestCount, maxDepthRequired)
			}

			if base.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount, maxDepthRequired)
			}
		case sub := <-othersChan:
			totalRequestCount += sub.Resp.Metadata.DispatchCount
			maxDepthRequired = max(maxDepthRequired, sub.Resp.Metadata.DepthRequired)

			if sub.Err != nil {
				return checkResultError(sub.Err, totalRequestCount, maxDepthRequired)
			}

			if sub.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount, maxDepthRequired)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), totalRequestCount, maxDepthRequired)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount, maxDepthRequired)
}

func checkResult(membership v1.DispatchCheckResponse_Membership, numRequests uint32, maxDepthRequired uint32) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
				DepthRequired: maxDepthRequired + 1, // +1 for the current call.
			},
			Membership: membership,
		},
		nil,
	}
}

func checkResultError(err error, numRequests uint32, maxDepthRequired uint32) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
				DepthRequired: maxDepthRequired + 1, // +1 for the current call.
			},
			Membership: v1.DispatchCheckResponse_UNKNOWN,
		},
		err,
	}
}

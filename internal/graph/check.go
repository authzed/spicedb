package graph

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

func NewConcurrentChecker(d Dispatcher, ds datastore.GraphDatastore, nsm namespace.Manager) Checker {
	return &concurrentChecker{d: d, ds: ds, nsm: nsm}
}

type concurrentChecker struct {
	d   Dispatcher
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

func onrEqual(lhs, rhs *v0.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

func (cc *concurrentChecker) Check(ctx context.Context, req *v1.DispatchCheckRequest, relation *v0.Relation) ReduceableCheckFunc {
	// If we have found the goal's ONR, then we know that the ONR is a member.
	if onrEqual(req.Subject, req.ObjectAndRelation) {
		return AlwaysMember()
	}

	if relation.UsersetRewrite == nil {
		return cc.checkDirect(ctx, req)
	}

	return cc.checkUsersetRewrite(ctx, req, relation.UsersetRewrite)
}

func (cc *concurrentChecker) dispatch(req *v1.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("dispatch", req).Send()
		result := cc.d.DispatchCheck(ctx, req)
		resultChan <- result
	}
}

func (cc *concurrentChecker) checkDirect(ctx context.Context, req *v1.DispatchCheckRequest) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 0)
			return
		}

		log.Trace().Object("direct", req).Send()
		it, err := cc.ds.QueryTuples(req.ObjectAndRelation.Namespace, requestRevision).
			WithObjectID(req.ObjectAndRelation.ObjectId).
			WithRelation(req.ObjectAndRelation.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 1)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			tplUserset := tpl.User.GetUserset()
			if onrEqual(tplUserset, req.Subject) {
				resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, 1)
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
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), 1)
			return
		}
		resultChan <- Any(ctx, requestsToDispatch)
	}
}

func (cc *concurrentChecker) checkUsersetRewrite(ctx context.Context, req *v1.DispatchCheckRequest, usr *v0.UsersetRewrite) ReduceableCheckFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		return cc.checkSetOperation(ctx, req, rw.Union, Any)
	case *v0.UsersetRewrite_Intersection:
		return cc.checkSetOperation(ctx, req, rw.Intersection, All)
	case *v0.UsersetRewrite_Exclusion:
		return cc.checkSetOperation(ctx, req, rw.Exclusion, Difference)
	default:
		return AlwaysFail
	}
}

func (cc *concurrentChecker) checkSetOperation(ctx context.Context, req *v1.DispatchCheckRequest, so *v0.SetOperation, reducer Reducer) ReduceableCheckFunc {
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

func (cc *concurrentChecker) checkComputedUserset(ctx context.Context, req *v1.DispatchCheckRequest, cu *v0.ComputedUserset, tpl *v0.RelationTuple) ReduceableCheckFunc {
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
		return AlwaysMember()
	}

	// Check if the target relation exists. If not, return nothing.
	err := cc.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return NotMember()
		}

		return CheckError(err)
	}

	return cc.dispatch(&v1.DispatchCheckRequest{
		ObjectAndRelation: targetOnr,
		Subject:           req.Subject,
		Metadata:          decrementDepth(req.Metadata),
	})
}

func (cc *concurrentChecker) checkTupleToUserset(ctx context.Context, req *v1.DispatchCheckRequest, ttu *v0.TupleToUserset) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- checkResultError(NewCheckFailureErr(err), 0)
			return
		}

		log.Trace().Object("ttu", req).Send()
		it, err := cc.ds.QueryTuples(req.ObjectAndRelation.Namespace, requestRevision).
			WithObjectID(req.ObjectAndRelation.ObjectId).
			WithRelation(ttu.Tupleset.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- checkResultError(err, 1)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableCheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, cc.checkComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- checkResultError(NewCheckFailureErr(it.Err()), 1)
			return
		}

		resultChan <- Any(ctx, requestsToDispatch)
	}
}

// All returns whether all of the lazy checks pass, and is used for intersection.
func All(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0)
	}

	var totalRequestCount uint32

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
			if result.Err != nil {
				return checkResultError(result.Err, totalRequestCount)
			}

			if result.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount)
}

// CheckError returns the error.
func CheckError(err error) ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResultError(err, 0)
	}
}

// AlwaysMember returns that the check always passes.
func AlwaysMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_MEMBER, 0)
	}
}

// NotMember returns that the check always returns false.
func NotMember() ReduceableCheckFunc {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		resultChan <- checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0)
	}
}

// Any returns whether any one of the lazy checks pass, and is used for union.
func Any(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	if len(requests) == 0 {
		return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, 0)
	}

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	var totalRequestCount uint32
	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			log.Trace().Object("any result", result.Resp).Send()
			totalRequestCount += result.Resp.Metadata.DispatchCount
			if result.Err == nil && result.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount)
			}
			if result.Err != nil {
				return checkResultError(result.Err, totalRequestCount)
			}
		case <-ctx.Done():
			log.Trace().Msg("any canceled")
			return checkResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount)
}

// Difference returns whether the first lazy check passes and none of the supsequent checks pass.
func Difference(ctx context.Context, requests []ReduceableCheckFunc) CheckResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(requests)-1)

	go requests[0](childCtx, baseChan)
	for _, req := range requests[1:] {
		go req(childCtx, othersChan)
	}

	var totalRequestCount uint32
	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			totalRequestCount += base.Resp.Metadata.DispatchCount
			if base.Err != nil {
				return checkResultError(base.Err, totalRequestCount)
			}

			if base.Resp.Membership != v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount)
			}
		case sub := <-othersChan:
			totalRequestCount += sub.Resp.Metadata.DispatchCount
			if sub.Err != nil {
				return checkResultError(sub.Err, totalRequestCount)
			}

			if sub.Resp.Membership == v1.DispatchCheckResponse_MEMBER {
				return checkResult(v1.DispatchCheckResponse_NOT_MEMBER, totalRequestCount)
			}
		case <-ctx.Done():
			return checkResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return checkResult(v1.DispatchCheckResponse_MEMBER, totalRequestCount)
}

func checkResult(membership v1.DispatchCheckResponse_Membership, numRequests uint32) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
			Membership: membership,
		},
		nil,
	}
}

func checkResultError(err error, numRequests uint32) CheckResult {
	return CheckResult{
		&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
			Membership: v1.DispatchCheckResponse_UNKNOWN,
		},
		err,
	}
}

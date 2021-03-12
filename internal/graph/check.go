package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/rs/zerolog/log"
)

const (
	errCheckError = "error performing check: %w"
)

func newLazyChecker(d Dispatcher, ds datastore.GraphDatastore) checker {
	return &lazyChecker{d: d, ds: ds}
}

type lazyChecker struct {
	d  Dispatcher
	ds datastore.GraphDatastore
}

func compareONR(lhs *pb.ObjectAndRelation, rhs *pb.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

func (lc *lazyChecker) check(req CheckRequest, relation *pb.Relation) ReduceableCheck {
	if relation.UsersetRewrite == nil {
		return lc.checkDirect(req)
	}

	return lc.checkUsersetRewrite(req, relation.UsersetRewrite)
}

func (lc *lazyChecker) dispatch(req CheckRequest) ReduceableCheck {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("dispatch", req).Send()
		result := lc.d.Check(ctx, req)
		log.Trace().Object("dispatch", req).Object("result", result).Send()
		resultChan <- result
	}
}

func (lc *lazyChecker) checkDirect(req CheckRequest) ReduceableCheck {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("direct", req).Send()
		it, err := lc.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(req.Start.Relation).
			Execute()
		defer it.Close()
		if err != nil {
			resultChan <- CheckResult{false, fmt.Errorf(errCheckError, err)}
			return
		}

		var requestsToDispatch []ReduceableCheck
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			tplUserset := tpl.User.GetUserset()
			if compareONR(tplUserset, req.Goal) {
				resultChan <- CheckResult{true, nil}
				return
			}
			if tplUserset.Relation != Ellipsis {
				// We need to recursively call check here, potentially changing namespaces
				requestsToDispatch = append(requestsToDispatch, lc.dispatch(CheckRequest{
					Start:      tplUserset,
					Goal:       req.Goal,
					AtRevision: req.AtRevision,
				}))
			}
		}
		if it.Err() != nil {
			resultChan <- CheckResult{false, fmt.Errorf(errCheckError, it.Err())}
			return
		}
		resultChan <- Any(ctx, requestsToDispatch)
	}
}

func (lc *lazyChecker) checkUsersetRewrite(req CheckRequest, usr *pb.UsersetRewrite) ReduceableCheck {
	switch rw := usr.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return lc.checkSetOperation(req, rw.Union, Any)
	case *pb.UsersetRewrite_Intersection:
		return lc.checkSetOperation(req, rw.Intersection, All)
	case *pb.UsersetRewrite_Exclusion:
		return lc.checkSetOperation(req, rw.Exclusion, Difference)
	default:
		return alwaysFail
	}
}

func (lc *lazyChecker) checkSetOperation(req CheckRequest, so *pb.SetOperation, reducer Reducer) ReduceableCheck {
	var requests []ReduceableCheck
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			requests = append(requests, lc.checkDirect(req))
		case *pb.SetOperation_Child_ComputedUserset:
			requests = append(requests, lc.checkComputedUserset(req, child.ComputedUserset, nil))
		case *pb.SetOperation_Child_UsersetRewrite:
			requests = append(requests, lc.checkUsersetRewrite(req, child.UsersetRewrite))
		case *pb.SetOperation_Child_TupleToUserset:
			requests = append(requests, lc.checkTupleToUserset(req, child.TupleToUserset))
		}
	}
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("set operation", req).Stringer("operation", so).Send()
		resultChan <- reducer(ctx, requests)
	}
}

func (lc *lazyChecker) checkComputedUserset(req CheckRequest, cu *pb.ComputedUserset, tpl *pb.RelationTuple) ReduceableCheck {
	var start *pb.ObjectAndRelation
	if cu.Object == pb.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.User.GetUserset()
	} else if cu.Object == pb.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ObjectAndRelation
		} else {
			start = req.Start
		}
	}

	return lc.dispatch(CheckRequest{
		Start: &pb.ObjectAndRelation{
			Namespace: start.Namespace,
			ObjectId:  start.ObjectId,
			Relation:  cu.Relation,
		},
		Goal:       req.Goal,
		AtRevision: req.AtRevision,
	})
}

func (lc *lazyChecker) checkTupleToUserset(req CheckRequest, ttu *pb.TupleToUserset) ReduceableCheck {
	return func(ctx context.Context, resultChan chan<- CheckResult) {
		log.Trace().Object("ttu", req).Send()
		it, err := lc.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(ttu.Tupleset.Relation).
			Execute()
		defer it.Close()
		if err != nil {
			resultChan <- CheckResult{false, fmt.Errorf(errCheckError, err)}
			return
		}

		var requestsToDispatch []ReduceableCheck
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, lc.checkComputedUserset(req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- CheckResult{false, fmt.Errorf(errCheckError, it.Err())}
			return
		}

		resultChan <- Any(ctx, requestsToDispatch)
	}
}

// All returns whether all of the lazy checks pass, and is used for intersection.
func All(ctx context.Context, requests []ReduceableCheck) CheckResult {
	if len(requests) == 0 {
		return CheckResult{IsMember: false, Err: nil}
	}

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			if result.Err != nil || !result.IsMember {
				return result
			}
		case <-ctx.Done():
			return CheckResult{IsMember: false, Err: ErrRequestCanceled}
		}
	}

	return CheckResult{IsMember: true, Err: nil}
}

// Any returns whether any one of the lazy checks pass, and is used for union.
func Any(ctx context.Context, requests []ReduceableCheck) CheckResult {
	if len(requests) == 0 {
		return CheckResult{IsMember: false, Err: nil}
	}

	resultChan := make(chan CheckResult, len(requests))
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	for _, req := range requests {
		go req(childCtx, resultChan)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			log.Trace().Object("any result", result).Send()
			if result.Err == nil && result.IsMember {
				return result
			}
		case <-ctx.Done():
			log.Trace().Msg("any canceled")
			return CheckResult{IsMember: false, Err: ErrRequestCanceled}
		}
	}

	return CheckResult{IsMember: false, Err: nil}
}

// Difference returns whether the first lazy check passes and none of the supsequent checks pass.
func Difference(ctx context.Context, requests []ReduceableCheck) CheckResult {
	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	baseChan := make(chan CheckResult, 1)
	othersChan := make(chan CheckResult, len(requests)-1)

	go requests[0](childCtx, baseChan)
	for _, req := range requests[1:] {
		go req(childCtx, othersChan)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case base := <-baseChan:
			if base.Err != nil || !base.IsMember {
				return base
			}
		case sub := <-othersChan:
			if sub.Err == nil || sub.IsMember {
				return CheckResult{IsMember: false, Err: sub.Err}
			}
		case <-ctx.Done():
			return CheckResult{IsMember: false, Err: ErrRequestCanceled}
		}
	}

	return CheckResult{IsMember: true, Err: nil}
}

var errAlwaysFail = errors.New("always fail")

func alwaysFail(ctx context.Context, resultChan chan<- CheckResult) {
	resultChan <- CheckResult{IsMember: false, Err: errAlwaysFail}
}

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
	errExpandError = "error performing expand: %w"
)

func newConcurrentExpander(d Dispatcher, ds datastore.GraphDatastore) expander {
	return &concurrentExpander{d: d, ds: ds}
}

type concurrentExpander struct {
	d  Dispatcher
	ds datastore.GraphDatastore
}

func (ce *concurrentExpander) expand(ctx context.Context, req ExpandRequest, relation *pb.Relation) ReduceableExpandFunc {
	log.Trace().Object("expand", req).Send()
	if relation.UsersetRewrite == nil {
		return ce.expandDirect(ctx, req, true)
	}

	return ce.expandUsersetRewrite(ctx, req, relation.UsersetRewrite)
}

func (ce *concurrentExpander) expandDirect(
	ctx context.Context,
	req ExpandRequest,
	includeStart bool,
) ReduceableExpandFunc {

	log.Trace().Object("direct", req).Send()
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		it, err := ce.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(req.Start.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- ExpandResult{nil, fmt.Errorf(errExpandError, err)}
			return
		}
		defer it.Close()

		var foundUsersets []*pb.User
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			foundUsersets = append(foundUsersets, &pb.User{
				UserOneof: &pb.User_Userset{
					Userset: tpl.User.GetUserset(),
				},
			})
		}
		if it.Err() != nil {
			resultChan <- ExpandResult{nil, fmt.Errorf(errExpandError, it.Err())}
			return
		}

		// In some cases (such as _this{} expansion) including the start point is misleading.
		var start *pb.ObjectAndRelation
		if includeStart {
			start = req.Start
		}

		resultChan <- ExpandResult{
			Tree: &pb.RelationTupleTreeNode{
				NodeType: &pb.RelationTupleTreeNode_LeafNode{
					LeafNode: &pb.DirectUserset{
						Users: foundUsersets,
					},
				},
				Expanded: start,
			},
			Err: nil,
		}
	}
}

func (ce *concurrentExpander) expandUsersetRewrite(ctx context.Context, req ExpandRequest, usr *pb.UsersetRewrite) ReduceableExpandFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		log.Trace().Msg("union")
		return ce.expandSetOperation(ctx, req, rw.Union, ExpandAny)
	case *pb.UsersetRewrite_Intersection:
		log.Trace().Msg("intersection")
		return ce.expandSetOperation(ctx, req, rw.Intersection, ExpandAll)
	case *pb.UsersetRewrite_Exclusion:
		log.Trace().Msg("exclusion")
		return ce.expandSetOperation(ctx, req, rw.Exclusion, ExpandDifference)
	default:
		return alwaysFailExpand
	}
}

func (ce *concurrentExpander) expandSetOperation(ctx context.Context, req ExpandRequest, so *pb.SetOperation, reducer ExpandReducer) ReduceableExpandFunc {
	var requests []ReduceableExpandFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			requests = append(requests, ce.expandDirect(ctx, req, false))
		case *pb.SetOperation_Child_ComputedUserset:
			requests = append(requests, ce.expandComputedUserset(req, child.ComputedUserset, nil))
		case *pb.SetOperation_Child_UsersetRewrite:
			requests = append(requests, ce.expandUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *pb.SetOperation_Child_TupleToUserset:
			requests = append(requests, ce.expandTupleToUserset(ctx, req, child.TupleToUserset))
		}
	}
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- reducer(ctx, req.Start, requests)
	}
}

func (ce *concurrentExpander) dispatch(req ExpandRequest) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		log.Trace().Object("dispatch expand", req).Send()
		result := ce.d.Expand(ctx, req)
		resultChan <- result
	}
}

func (ce *concurrentExpander) expandComputedUserset(req ExpandRequest, cu *pb.ComputedUserset, tpl *pb.RelationTuple) ReduceableExpandFunc {
	log.Trace().Str("relation", cu.Relation).Msg("computed userset")
	var start *pb.ObjectAndRelation
	if cu.Object == pb.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			// TODO replace this with something else, AlwaysFail?
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

	return ce.dispatch(ExpandRequest{
		Start: &pb.ObjectAndRelation{
			Namespace: start.Namespace,
			ObjectId:  start.ObjectId,
			Relation:  cu.Relation,
		},
		AtRevision:     req.AtRevision,
		DepthRemaining: req.DepthRemaining - 1,
	})
}

func (ce *concurrentExpander) expandTupleToUserset(ctx context.Context, req ExpandRequest, ttu *pb.TupleToUserset) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		it, err := ce.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(ttu.Tupleset.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- ExpandResult{nil, fmt.Errorf(errExpandError, err)}
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableExpandFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, ce.expandComputedUserset(req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- ExpandResult{nil, fmt.Errorf(errExpandError, it.Err())}
			return
		}

		resultChan <- ExpandAny(ctx, req.Start, requestsToDispatch)
	}
}

func setResult(op pb.SetOperationUserset_Operation, start *pb.ObjectAndRelation, children []*pb.RelationTupleTreeNode) ExpandResult {
	return ExpandResult{
		Tree: &pb.RelationTupleTreeNode{
			NodeType: &pb.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &pb.SetOperationUserset{
					Operation:  op,
					ChildNodes: children,
				},
			},
			Expanded: start,
		},
		Err: nil,
	}
}

func expandSetOperation(
	ctx context.Context,
	start *pb.ObjectAndRelation,
	requests []ReduceableExpandFunc,
	op pb.SetOperationUserset_Operation,
) ExpandResult {
	children := make([]*pb.RelationTupleTreeNode, 0, len(requests))

	if len(requests) == 0 {
		return setResult(op, start, children)
	}

	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan ExpandResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan ExpandResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			if result.Err != nil {
				return ExpandResult{Tree: nil, Err: result.Err}
			}
			children = append(children, result.Tree)
		case <-ctx.Done():
			return ExpandResult{Tree: nil, Err: ErrRequestCanceled}
		}
	}

	return setResult(op, start, children)
}

// ExpandAll returns a tree with all of the children and an intersection node type.
func ExpandAll(ctx context.Context, start *pb.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, pb.SetOperationUserset_INTERSECTION)
}

// ExpandAny returns a tree with all of the children and a union node type.
func ExpandAny(ctx context.Context, start *pb.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, pb.SetOperationUserset_UNION)
}

// ExpandDifference returns a tree with all of the children and an exclusion node type.
func ExpandDifference(ctx context.Context, start *pb.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, pb.SetOperationUserset_EXCLUSION)
}

// ExpandOne waits for exactly one response
func ExpandOne(ctx context.Context, request ReduceableExpandFunc) ExpandResult {
	resultChan := make(chan ExpandResult, 1)
	go request(ctx, resultChan)

	select {
	case result := <-resultChan:
		if result.Err != nil {
			return ExpandResult{Tree: nil, Err: result.Err}
		}
		return result
	case <-ctx.Done():
		return ExpandResult{Tree: nil, Err: ErrRequestCanceled}
	}
}

var errAlwaysFailExpand = errors.New("always fail")

func alwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- ExpandResult{Tree: nil, Err: errAlwaysFailExpand}
}

package graph

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

type startInclusion int

const (
	includeStart startInclusion = iota
	excludeStart
)

func newConcurrentExpander(d Dispatcher, ds datastore.GraphDatastore, nsm namespace.Manager) expander {
	return &concurrentExpander{d: d, ds: ds, nsm: nsm}
}

type concurrentExpander struct {
	d   Dispatcher
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

func (ce *concurrentExpander) expand(ctx context.Context, req ExpandRequest, relation *v0.Relation) ReduceableExpandFunc {
	log.Trace().Object("expand", req).Send()
	if relation.UsersetRewrite == nil {
		return ce.expandDirect(ctx, req, includeStart)
	}

	return ce.expandUsersetRewrite(ctx, req, relation.UsersetRewrite)
}

func (ce *concurrentExpander) expandDirect(
	ctx context.Context,
	req ExpandRequest,
	startBehavior startInclusion,
) ReduceableExpandFunc {

	log.Trace().Object("direct", req).Send()
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		it, err := ce.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(req.Start.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- ExpandResult{nil, NewExpansionFailureErr(err)}
			return
		}
		defer it.Close()

		var foundNonTerminalUsersets []*v0.User
		var foundTerminalUsersets []*v0.User
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if tpl.User.GetUserset().Relation == Ellipsis {
				foundTerminalUsersets = append(foundTerminalUsersets, tpl.User)
			} else {
				foundNonTerminalUsersets = append(foundNonTerminalUsersets, tpl.User)
			}
		}
		if it.Err() != nil {
			resultChan <- ExpandResult{nil, NewExpansionFailureErr(it.Err())}
			return
		}

		// In some cases (such as _this{} expansion) including the start point is misleading.
		var start *v0.ObjectAndRelation
		if startBehavior == includeStart {
			start = req.Start
		}

		// If only shallow expansion was required, or there are no non-terminal subjects found,
		// nothing more to do.
		if req.ExpansionMode != RecursiveExpansion || len(foundNonTerminalUsersets) == 0 {
			resultChan <- ExpandResult{
				Tree: &v0.RelationTupleTreeNode{
					NodeType: &v0.RelationTupleTreeNode_LeafNode{
						LeafNode: &v0.DirectUserset{
							Users: append(foundTerminalUsersets, foundNonTerminalUsersets...),
						},
					},
					Expanded: start,
				},
				Err: nil,
			}
			return
		}

		// Otherwise, recursively issue expansion and collect the results from that, plus the
		// found terminals together.
		var requestsToDispatch []ReduceableExpandFunc
		for _, nonTerminalUser := range foundNonTerminalUsersets {
			requestsToDispatch = append(requestsToDispatch, ce.dispatch(ExpandRequest{
				Start:          nonTerminalUser.GetUserset(),
				AtRevision:     req.AtRevision,
				DepthRemaining: req.DepthRemaining - 1,
				ExpansionMode:  req.ExpansionMode,
			}))
		}

		result := ExpandAny(ctx, req.Start, requestsToDispatch)
		if result.Err != nil {
			resultChan <- result
			return
		}

		unionNode := result.Tree.GetIntermediateNode()
		unionNode.ChildNodes = append(unionNode.ChildNodes, &v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_LeafNode{
				LeafNode: &v0.DirectUserset{
					Users: append(foundTerminalUsersets, foundNonTerminalUsersets...),
				},
			},
			Expanded: start,
		})
		resultChan <- result
	}
}

func (ce *concurrentExpander) expandUsersetRewrite(ctx context.Context, req ExpandRequest, usr *v0.UsersetRewrite) ReduceableExpandFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		log.Trace().Msg("union")
		return ce.expandSetOperation(ctx, req, rw.Union, ExpandAny)
	case *v0.UsersetRewrite_Intersection:
		log.Trace().Msg("intersection")
		return ce.expandSetOperation(ctx, req, rw.Intersection, ExpandAll)
	case *v0.UsersetRewrite_Exclusion:
		log.Trace().Msg("exclusion")
		return ce.expandSetOperation(ctx, req, rw.Exclusion, ExpandDifference)
	default:
		return alwaysFailExpand
	}
}

func (ce *concurrentExpander) expandSetOperation(ctx context.Context, req ExpandRequest, so *v0.SetOperation, reducer ExpandReducer) ReduceableExpandFunc {
	var requests []ReduceableExpandFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *v0.SetOperation_Child_XThis:
			requests = append(requests, ce.expandDirect(ctx, req, excludeStart))
		case *v0.SetOperation_Child_ComputedUserset:
			requests = append(requests, ce.expandComputedUserset(ctx, req, child.ComputedUserset, nil))
		case *v0.SetOperation_Child_UsersetRewrite:
			requests = append(requests, ce.expandUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *v0.SetOperation_Child_TupleToUserset:
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

func (ce *concurrentExpander) expandComputedUserset(ctx context.Context, req ExpandRequest, cu *v0.ComputedUserset, tpl *v0.RelationTuple) ReduceableExpandFunc {
	log.Trace().Str("relation", cu.Relation).Msg("computed userset")
	var start *v0.ObjectAndRelation
	if cu.Object == v0.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			// TODO replace this with something else, AlwaysFail?
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.User.GetUserset()
	} else if cu.Object == v0.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ObjectAndRelation
		} else {
			start = req.Start
		}
	}

	// Check if the target relation exists. If not, return nothing.
	err := ce.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return EmptyExpansion(req.Start)
		}

		return ExpandError(err)
	}

	return ce.dispatch(ExpandRequest{
		Start: &v0.ObjectAndRelation{
			Namespace: start.Namespace,
			ObjectId:  start.ObjectId,
			Relation:  cu.Relation,
		},
		AtRevision:     req.AtRevision,
		DepthRemaining: req.DepthRemaining - 1,
		ExpansionMode:  req.ExpansionMode,
	})
}

func (ce *concurrentExpander) expandTupleToUserset(ctx context.Context, req ExpandRequest, ttu *v0.TupleToUserset) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		it, err := ce.ds.QueryTuples(req.Start.Namespace, req.AtRevision).
			WithObjectID(req.Start.ObjectId).
			WithRelation(ttu.Tupleset.Relation).
			Execute(ctx)
		if err != nil {
			resultChan <- ExpandResult{nil, NewExpansionFailureErr(err)}
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableExpandFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, ce.expandComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- ExpandResult{nil, NewExpansionFailureErr(it.Err())}
			return
		}

		resultChan <- ExpandAny(ctx, req.Start, requestsToDispatch)
	}
}

func setResult(op v0.SetOperationUserset_Operation, start *v0.ObjectAndRelation, children []*v0.RelationTupleTreeNode) ExpandResult {
	return ExpandResult{
		Tree: &v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &v0.SetOperationUserset{
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
	start *v0.ObjectAndRelation,
	requests []ReduceableExpandFunc,
	op v0.SetOperationUserset_Operation,
) ExpandResult {
	children := make([]*v0.RelationTupleTreeNode, 0, len(requests))

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
			return ExpandResult{Tree: nil, Err: NewRequestCanceledErr()}
		}
	}

	return setResult(op, start, children)
}

// EmptyExpansion returns an empty expansion.
func EmptyExpansion(start *v0.ObjectAndRelation) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- ExpandResult{&v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_LeafNode{
				LeafNode: &v0.DirectUserset{},
			},
			Expanded: start,
		}, nil}
	}
}

// ExpandError returns the error.
func ExpandError(err error) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- ExpandResult{nil, err}
	}
}

// ExpandAll returns a tree with all of the children and an intersection node type.
func ExpandAll(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_INTERSECTION)
}

// ExpandAny returns a tree with all of the children and a union node type.
func ExpandAny(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_UNION)
}

// ExpandDifference returns a tree with all of the children and an exclusion node type.
func ExpandDifference(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_EXCLUSION)
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
		return ExpandResult{Tree: nil, Err: NewRequestCanceledErr()}
	}
}

var errAlwaysFailExpand = errors.New("always fail")

func alwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- ExpandResult{Tree: nil, Err: errAlwaysFailExpand}
}

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

type startInclusion int

const (
	includeStart startInclusion = iota
	excludeStart
)

// NewConcurrentExpander creates an instance of ConcurrentExpander
func NewConcurrentExpander(d dispatch.Expand, ds datastore.GraphDatastore, nsm namespace.Manager) *ConcurrentExpander {
	return &ConcurrentExpander{d: d, ds: ds, nsm: nsm}
}

// ConcurrentExpander exposes a method to perform Expand requests, and delegates subproblems to the
// provided dispatch.Expand instance.
type ConcurrentExpander struct {
	d   dispatch.Expand
	ds  datastore.GraphDatastore
	nsm namespace.Manager
}

// Expand performs an expand request with the provided request and context.
func (ce *ConcurrentExpander) Expand(ctx context.Context, req *v1.DispatchExpandRequest, relation *v0.Relation) (*v1.DispatchExpandResponse, error) {
	log.Trace().Object("expand", req).Send()

	var directFunc ReduceableExpandFunc
	if relation.UsersetRewrite == nil {
		directFunc = ce.expandDirect(ctx, req, includeStart)
	} else {
		directFunc = ce.expandUsersetRewrite(ctx, req, relation.UsersetRewrite)
	}

	resolved := expandOne(ctx, directFunc)
	return resolved.Resp, resolved.Err
}

func (ce *ConcurrentExpander) expandDirect(
	ctx context.Context,
	req *v1.DispatchExpandRequest,
	startBehavior startInclusion,
) ReduceableExpandFunc {

	log.Trace().Object("direct", req).Send()
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), 0)
			return
		}

		it, err := ce.ds.QueryTuples(
			req.ObjectAndRelation.Namespace,
			req.ObjectAndRelation.ObjectId,
			req.ObjectAndRelation.Relation,
			requestRevision,
		).Execute(ctx)
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), 1)
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
			resultChan <- expandResultError(NewExpansionFailureErr(it.Err()), 1)
			return
		}

		// In some cases (such as _this{} expansion) including the start point is misleading.
		var start *v0.ObjectAndRelation
		if startBehavior == includeStart {
			start = req.ObjectAndRelation
		}

		// If only shallow expansion was required, or there are no non-terminal subjects found,
		// nothing more to do.
		if req.ExpansionMode == v1.DispatchExpandRequest_SHALLOW || len(foundNonTerminalUsersets) == 0 {
			resultChan <- expandResult(
				&v0.RelationTupleTreeNode{
					NodeType: &v0.RelationTupleTreeNode_LeafNode{
						LeafNode: &v0.DirectUserset{
							Users: append(foundTerminalUsersets, foundNonTerminalUsersets...),
						},
					},
					Expanded: start,
				},
				1,
			)
			return
		}

		// Otherwise, recursively issue expansion and collect the results from that, plus the
		// found terminals together.
		var requestsToDispatch []ReduceableExpandFunc
		for _, nonTerminalUser := range foundNonTerminalUsersets {
			requestsToDispatch = append(requestsToDispatch, ce.dispatch(&v1.DispatchExpandRequest{
				ObjectAndRelation: nonTerminalUser.GetUserset(),
				Metadata:          decrementDepth(req.Metadata),
				ExpansionMode:     req.ExpansionMode,
			}))
		}

		result := expandAny(ctx, req.ObjectAndRelation, requestsToDispatch)
		if result.Err != nil {
			resultChan <- result
			return
		}

		unionNode := result.Resp.TreeNode.GetIntermediateNode()
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

func (ce *ConcurrentExpander) expandUsersetRewrite(ctx context.Context, req *v1.DispatchExpandRequest, usr *v0.UsersetRewrite) ReduceableExpandFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		log.Trace().Msg("union")
		return ce.expandSetOperation(ctx, req, rw.Union, expandAny)
	case *v0.UsersetRewrite_Intersection:
		log.Trace().Msg("intersection")
		return ce.expandSetOperation(ctx, req, rw.Intersection, expandAll)
	case *v0.UsersetRewrite_Exclusion:
		log.Trace().Msg("exclusion")
		return ce.expandSetOperation(ctx, req, rw.Exclusion, expandDifference)
	default:
		return alwaysFailExpand
	}
}

func (ce *ConcurrentExpander) expandSetOperation(ctx context.Context, req *v1.DispatchExpandRequest, so *v0.SetOperation, reducer ExpandReducer) ReduceableExpandFunc {
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
		resultChan <- reducer(ctx, req.ObjectAndRelation, requests)
	}
}

func (ce *ConcurrentExpander) dispatch(req *v1.DispatchExpandRequest) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		log.Trace().Object("dispatch expand", req).Send()
		result, err := ce.d.DispatchExpand(ctx, req)
		resultChan <- ExpandResult{result, err}
	}
}

func (ce *ConcurrentExpander) expandComputedUserset(ctx context.Context, req *v1.DispatchExpandRequest, cu *v0.ComputedUserset, tpl *v0.RelationTuple) ReduceableExpandFunc {
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
			start = req.ObjectAndRelation
		}
	}

	// Check if the target relation exists. If not, return nothing.
	err := ce.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return emptyExpansion(req.ObjectAndRelation)
		}

		return expandError(err)
	}

	return ce.dispatch(&v1.DispatchExpandRequest{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: start.Namespace,
			ObjectId:  start.ObjectId,
			Relation:  cu.Relation,
		},
		Metadata:      decrementDepth(req.Metadata),
		ExpansionMode: req.ExpansionMode,
	})
}

func (ce *ConcurrentExpander) expandTupleToUserset(ctx context.Context, req *v1.DispatchExpandRequest, ttu *v0.TupleToUserset) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		requestRevision, err := decimal.NewFromString(req.Metadata.AtRevision)
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), 0)
			return
		}

		it, err := ce.ds.QueryTuples(
			req.ObjectAndRelation.Namespace,
			req.ObjectAndRelation.ObjectId,
			ttu.Tupleset.Relation,
			requestRevision,
		).Execute(ctx)
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), 1)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableExpandFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, ce.expandComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(it.Err()), 1)
			return
		}

		resultChan <- expandAny(ctx, req.ObjectAndRelation, requestsToDispatch)
	}
}

func setResult(
	op v0.SetOperationUserset_Operation,
	start *v0.ObjectAndRelation,
	children []*v0.RelationTupleTreeNode,
	totalRequestCount uint32,
) ExpandResult {
	return expandResult(
		&v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &v0.SetOperationUserset{
					Operation:  op,
					ChildNodes: children,
				},
			},
			Expanded: start,
		},
		totalRequestCount,
	)
}

func expandSetOperation(
	ctx context.Context,
	start *v0.ObjectAndRelation,
	requests []ReduceableExpandFunc,
	op v0.SetOperationUserset_Operation,
) ExpandResult {
	children := make([]*v0.RelationTupleTreeNode, 0, len(requests))

	if len(requests) == 0 {
		return setResult(op, start, children, 0)
	}

	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan ExpandResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan ExpandResult)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	var totalRequestCount uint32
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			totalRequestCount += result.Resp.Metadata.DispatchCount
			if result.Err != nil {
				return expandResultError(result.Err, totalRequestCount)
			}
			children = append(children, result.Resp.TreeNode)
		case <-ctx.Done():
			return expandResultError(NewRequestCanceledErr(), totalRequestCount)
		}
	}

	return setResult(op, start, children, totalRequestCount)
}

// emptyExpansion returns an empty expansion.
func emptyExpansion(start *v0.ObjectAndRelation) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- expandResult(&v0.RelationTupleTreeNode{
			NodeType: &v0.RelationTupleTreeNode_LeafNode{
				LeafNode: &v0.DirectUserset{},
			},
			Expanded: start,
		}, 0)
	}
}

// expandError returns the error.
func expandError(err error) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- expandResultError(err, 0)
	}
}

// expandAll returns a tree with all of the children and an intersection node type.
func expandAll(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_INTERSECTION)
}

// expandAny returns a tree with all of the children and a union node type.
func expandAny(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_UNION)
}

// expandDifference returns a tree with all of the children and an exclusion node type.
func expandDifference(ctx context.Context, start *v0.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, v0.SetOperationUserset_EXCLUSION)
}

// expandOne waits for exactly one response
func expandOne(ctx context.Context, request ReduceableExpandFunc) ExpandResult {
	resultChan := make(chan ExpandResult, 1)
	go request(ctx, resultChan)

	select {
	case result := <-resultChan:
		if result.Err != nil {
			return result
		}
		return result
	case <-ctx.Done():
		return expandResultError(NewRequestCanceledErr(), 0)
	}
}

var errAlwaysFailExpand = errors.New("always fail")

func alwaysFailExpand(ctx context.Context, resultChan chan<- ExpandResult) {
	resultChan <- expandResultError(errAlwaysFailExpand, 0)
}

func expandResult(treeNode *v0.RelationTupleTreeNode, numRequests uint32) ExpandResult {
	return ExpandResult{
		&v1.DispatchExpandResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
			TreeNode: treeNode,
		},
		nil,
	}
}

func expandResultError(err error, numRequests uint32) ExpandResult {
	return ExpandResult{
		&v1.DispatchExpandResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: numRequests,
			},
		},
		err,
	}
}

package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/caveats"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// NewConcurrentExpander creates an instance of ConcurrentExpander
func NewConcurrentExpander(d dispatch.Expand) *ConcurrentExpander {
	return &ConcurrentExpander{d: d}
}

// ConcurrentExpander exposes a method to perform Expand requests, and delegates subproblems to the
// provided dispatch.Expand instance.
type ConcurrentExpander struct {
	d dispatch.Expand
}

// ValidatedExpandRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedExpandRequest struct {
	*v1.DispatchExpandRequest
	Revision datastore.Revision
}

// Expand performs an expand request with the provided request and context.
func (ce *ConcurrentExpander) Expand(ctx context.Context, req ValidatedExpandRequest, relation *core.Relation) (*v1.DispatchExpandResponse, error) {
	log.Ctx(ctx).Trace().Object("expand", req).Send()

	var directFunc ReduceableExpandFunc
	if relation.UsersetRewrite == nil {
		directFunc = ce.expandDirect(ctx, req)
	} else {
		directFunc = ce.expandUsersetRewrite(ctx, req, relation.UsersetRewrite)
	}

	resolved := expandOne(ctx, directFunc)
	resolved.Resp.Metadata = addCallToResponseMetadata(resolved.Resp.Metadata)
	return resolved.Resp, resolved.Err
}

func (ce *ConcurrentExpander) expandDirect(
	ctx context.Context,
	req ValidatedExpandRequest,
) ReduceableExpandFunc {
	log.Ctx(ctx).Trace().Object("direct", req).Send()
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
		it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType:             req.ResourceAndRelation.Namespace,
			OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
			OptionalResourceRelation: req.ResourceAndRelation.Relation,
		})
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
			return
		}
		defer it.Close()

		var foundNonTerminalUsersets []*core.DirectSubject
		var foundTerminalUsersets []*core.DirectSubject
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if it.Err() != nil {
				resultChan <- expandResultError(NewExpansionFailureErr(it.Err()), emptyMetadata)
				return
			}

			ds := &core.DirectSubject{
				Subject:          tpl.Subject,
				CaveatExpression: caveats.CaveatAsExpr(tpl.Caveat),
			}
			if tpl.Subject.Relation == Ellipsis {
				foundTerminalUsersets = append(foundTerminalUsersets, ds)
			} else {
				foundNonTerminalUsersets = append(foundNonTerminalUsersets, ds)
			}
		}
		it.Close()

		// If only shallow expansion was required, or there are no non-terminal subjects found,
		// nothing more to do.
		if req.ExpansionMode == v1.DispatchExpandRequest_SHALLOW || len(foundNonTerminalUsersets) == 0 {
			resultChan <- expandResult(
				&core.RelationTupleTreeNode{
					NodeType: &core.RelationTupleTreeNode_LeafNode{
						LeafNode: &core.DirectSubjects{
							Subjects: append(foundTerminalUsersets, foundNonTerminalUsersets...),
						},
					},
					Expanded: req.ResourceAndRelation,
				},
				emptyMetadata,
			)
			return
		}

		// Otherwise, recursively issue expansion and collect the results from that, plus the
		// found terminals together.
		var requestsToDispatch []ReduceableExpandFunc
		for _, nonTerminalUser := range foundNonTerminalUsersets {
			toDispatch := ce.dispatch(ValidatedExpandRequest{
				&v1.DispatchExpandRequest{
					ResourceAndRelation: nonTerminalUser.Subject,
					Metadata:            decrementDepth(req.Metadata),
					ExpansionMode:       req.ExpansionMode,
				},
				req.Revision,
			})

			requestsToDispatch = append(requestsToDispatch, decorateWithCaveatIfNecessary(toDispatch, nonTerminalUser.CaveatExpression))
		}

		result := expandAny(ctx, req.ResourceAndRelation, requestsToDispatch)
		if result.Err != nil {
			resultChan <- result
			return
		}

		unionNode := result.Resp.TreeNode.GetIntermediateNode()
		unionNode.ChildNodes = append(unionNode.ChildNodes, &core.RelationTupleTreeNode{
			NodeType: &core.RelationTupleTreeNode_LeafNode{
				LeafNode: &core.DirectSubjects{
					Subjects: append(foundTerminalUsersets, foundNonTerminalUsersets...),
				},
			},
			Expanded: req.ResourceAndRelation,
		})
		resultChan <- result
	}
}

func decorateWithCaveatIfNecessary(toDispatch ReduceableExpandFunc, caveatExpr *core.CaveatExpression) ReduceableExpandFunc {
	// If no caveat expression, simply return the func unmodified.
	if caveatExpr == nil {
		return toDispatch
	}

	// Otherwise return a wrapped function that expands the underlying func to be dispatched, and then decorates
	// the resulting node with the caveat expression.
	//
	// TODO(jschorr): This will generate a lot of function closures, so we should change Expand to avoid them
	// like we did in Check.
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		result := expandOne(ctx, toDispatch)
		if result.Err != nil {
			resultChan <- result
			return
		}

		result.Resp.TreeNode.CaveatExpression = caveatExpr
		resultChan <- result
	}
}

func (ce *ConcurrentExpander) expandUsersetRewrite(ctx context.Context, req ValidatedExpandRequest, usr *core.UsersetRewrite) ReduceableExpandFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		log.Ctx(ctx).Trace().Msg("union")
		return ce.expandSetOperation(ctx, req, rw.Union, expandAny)
	case *core.UsersetRewrite_Intersection:
		log.Ctx(ctx).Trace().Msg("intersection")
		return ce.expandSetOperation(ctx, req, rw.Intersection, expandAll)
	case *core.UsersetRewrite_Exclusion:
		log.Ctx(ctx).Trace().Msg("exclusion")
		return ce.expandSetOperation(ctx, req, rw.Exclusion, expandDifference)
	default:
		return alwaysFailExpand
	}
}

func (ce *ConcurrentExpander) expandSetOperation(ctx context.Context, req ValidatedExpandRequest, so *core.SetOperation, reducer ExpandReducer) ReduceableExpandFunc {
	var requests []ReduceableExpandFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return expandError(errors.New("use of _this is unsupported; please rewrite your schema"))
		case *core.SetOperation_Child_ComputedUserset:
			requests = append(requests, ce.expandComputedUserset(ctx, req, child.ComputedUserset, nil))
		case *core.SetOperation_Child_UsersetRewrite:
			requests = append(requests, ce.expandUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *core.SetOperation_Child_TupleToUserset:
			requests = append(requests, ce.expandTupleToUserset(ctx, req, child.TupleToUserset))
		case *core.SetOperation_Child_XNil:
			requests = append(requests, emptyExpansion(req.ResourceAndRelation))
		default:
			return expandError(fmt.Errorf("unknown set operation child `%T` in expand", child))
		}
	}
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- reducer(ctx, req.ResourceAndRelation, requests)
	}
}

func (ce *ConcurrentExpander) dispatch(req ValidatedExpandRequest) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		log.Ctx(ctx).Trace().Object("dispatchExpand", req).Send()
		result, err := ce.d.DispatchExpand(ctx, req.DispatchExpandRequest)
		resultChan <- ExpandResult{result, err}
	}
}

func (ce *ConcurrentExpander) expandComputedUserset(ctx context.Context, req ValidatedExpandRequest, cu *core.ComputedUserset, tpl *core.RelationTuple) ReduceableExpandFunc {
	log.Ctx(ctx).Trace().Str("relation", cu.Relation).Msg("computed userset")
	var start *core.ObjectAndRelation
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			return expandError(spiceerrors.MustBugf("computed userset for tupleset without tuple"))
		}

		start = tpl.Subject
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ResourceAndRelation
		} else {
			start = req.ResourceAndRelation
		}
	}

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return emptyExpansion(req.ResourceAndRelation)
		}

		return expandError(err)
	}

	return ce.dispatch(ValidatedExpandRequest{
		&v1.DispatchExpandRequest{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: start.Namespace,
				ObjectId:  start.ObjectId,
				Relation:  cu.Relation,
			},
			Metadata:      decrementDepth(req.Metadata),
			ExpansionMode: req.ExpansionMode,
		},
		req.Revision,
	})
}

func (ce *ConcurrentExpander) expandTupleToUserset(_ context.Context, req ValidatedExpandRequest, ttu *core.TupleToUserset) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
		it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
			ResourceType:             req.ResourceAndRelation.Namespace,
			OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
			OptionalResourceRelation: ttu.Tupleset.Relation,
		})
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
			return
		}
		defer it.Close()

		var requestsToDispatch []ReduceableExpandFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if it.Err() != nil {
				resultChan <- expandResultError(NewExpansionFailureErr(it.Err()), emptyMetadata)
				return
			}

			toDispatch := ce.expandComputedUserset(ctx, req, ttu.ComputedUserset, tpl)
			requestsToDispatch = append(requestsToDispatch, decorateWithCaveatIfNecessary(toDispatch, caveats.CaveatAsExpr(tpl.Caveat)))
		}
		it.Close()

		resultChan <- expandAny(ctx, req.ResourceAndRelation, requestsToDispatch)
	}
}

func setResult(
	op core.SetOperationUserset_Operation,
	start *core.ObjectAndRelation,
	children []*core.RelationTupleTreeNode,
	metadata *v1.ResponseMeta,
) ExpandResult {
	return expandResult(
		&core.RelationTupleTreeNode{
			NodeType: &core.RelationTupleTreeNode_IntermediateNode{
				IntermediateNode: &core.SetOperationUserset{
					Operation:  op,
					ChildNodes: children,
				},
			},
			Expanded: start,
		},
		metadata,
	)
}

func expandSetOperation(
	ctx context.Context,
	start *core.ObjectAndRelation,
	requests []ReduceableExpandFunc,
	op core.SetOperationUserset_Operation,
) ExpandResult {
	children := make([]*core.RelationTupleTreeNode, 0, len(requests))

	if len(requests) == 0 {
		return setResult(op, start, children, emptyMetadata)
	}

	childCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()

	resultChans := make([]chan ExpandResult, 0, len(requests))
	for _, req := range requests {
		resultChan := make(chan ExpandResult, 1)
		resultChans = append(resultChans, resultChan)
		go req(childCtx, resultChan)
	}

	responseMetadata := emptyMetadata
	for _, resultChan := range resultChans {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return expandResultError(result.Err, responseMetadata)
			}
			children = append(children, result.Resp.TreeNode)
		case <-ctx.Done():
			return expandResultError(NewRequestCanceledErr(), responseMetadata)
		}
	}

	return setResult(op, start, children, responseMetadata)
}

// emptyExpansion returns an empty expansion.
func emptyExpansion(start *core.ObjectAndRelation) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- expandResult(&core.RelationTupleTreeNode{
			NodeType: &core.RelationTupleTreeNode_LeafNode{
				LeafNode: &core.DirectSubjects{},
			},
			Expanded: start,
		}, emptyMetadata)
	}
}

// expandError returns the error.
func expandError(err error) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		resultChan <- expandResultError(err, emptyMetadata)
	}
}

// expandAll returns a tree with all of the children and an intersection node type.
func expandAll(ctx context.Context, start *core.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, core.SetOperationUserset_INTERSECTION)
}

// expandAny returns a tree with all of the children and a union node type.
func expandAny(ctx context.Context, start *core.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, core.SetOperationUserset_UNION)
}

// expandDifference returns a tree with all of the children and an exclusion node type.
func expandDifference(ctx context.Context, start *core.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult {
	return expandSetOperation(ctx, start, requests, core.SetOperationUserset_EXCLUSION)
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
		return expandResultError(NewRequestCanceledErr(), emptyMetadata)
	}
}

var errAlwaysFailExpand = errors.New("always fail")

func alwaysFailExpand(_ context.Context, resultChan chan<- ExpandResult) {
	resultChan <- expandResultError(errAlwaysFailExpand, emptyMetadata)
}

func expandResult(treeNode *core.RelationTupleTreeNode, subProblemMetadata *v1.ResponseMeta) ExpandResult {
	return ExpandResult{
		&v1.DispatchExpandResponse{
			Metadata: ensureMetadata(subProblemMetadata),
			TreeNode: treeNode,
		},
		nil,
	}
}

func expandResultError(err error, subProblemMetadata *v1.ResponseMeta) ExpandResult {
	return ExpandResult{
		&v1.DispatchExpandResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}

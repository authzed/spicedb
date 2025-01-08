package graph

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/caveats"

	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
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
			OptionalResourceType:     req.ResourceAndRelation.Namespace,
			OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
			OptionalResourceRelation: req.ResourceAndRelation.Relation,
		})
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
			return
		}

		var foundNonTerminalUsersets []*core.DirectSubject
		var foundTerminalUsersets []*core.DirectSubject
		for rel, err := range it {
			if err != nil {
				resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
				return
			}

			ds := &core.DirectSubject{
				Subject:          rel.Subject.ToCoreONR(),
				CaveatExpression: caveats.CaveatAsExpr(rel.OptionalCaveat),
			}

			if rel.Subject.Relation == Ellipsis {
				foundTerminalUsersets = append(foundTerminalUsersets, ds)
			} else {
				foundNonTerminalUsersets = append(foundNonTerminalUsersets, ds)
			}
		}

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
			requests = append(requests, expandTupleToUserset(ctx, ce, req, child.TupleToUserset, expandAny))
		case *core.SetOperation_Child_FunctionedTupleToUserset:
			switch child.FunctionedTupleToUserset.Function {
			case core.FunctionedTupleToUserset_FUNCTION_ANY:
				requests = append(requests, expandTupleToUserset(ctx, ce, req, child.FunctionedTupleToUserset, expandAny))

			case core.FunctionedTupleToUserset_FUNCTION_ALL:
				requests = append(requests, expandTupleToUserset(ctx, ce, req, child.FunctionedTupleToUserset, expandAll))

			default:
				return expandError(spiceerrors.MustBugf("unknown function `%s` in expand", child.FunctionedTupleToUserset.Function))
			}
		case *core.SetOperation_Child_XNil:
			requests = append(requests, emptyExpansion(req.ResourceAndRelation))
		default:
			return expandError(spiceerrors.MustBugf("unknown set operation child `%T` in expand", child))
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

func (ce *ConcurrentExpander) expandComputedUserset(ctx context.Context, req ValidatedExpandRequest, cu *core.ComputedUserset, rel *tuple.Relationship) ReduceableExpandFunc {
	log.Ctx(ctx).Trace().Str("relation", cu.Relation).Msg("computed userset")
	var start tuple.ObjectAndRelation
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if rel == nil {
			return expandError(spiceerrors.MustBugf("computed userset for tupleset without tuple"))
		}

		start = rel.Subject
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if rel != nil {
			start = rel.Resource
		} else {
			start = tuple.FromCoreObjectAndRelation(req.ResourceAndRelation)
		}
	}

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, start.ObjectType, cu.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.RelationNotFoundError{}) {
			return emptyExpansion(req.ResourceAndRelation)
		}

		return expandError(err)
	}

	return ce.dispatch(ValidatedExpandRequest{
		&v1.DispatchExpandRequest{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: start.ObjectType,
				ObjectId:  start.ObjectID,
				Relation:  cu.Relation,
			},
			Metadata:      decrementDepth(req.Metadata),
			ExpansionMode: req.ExpansionMode,
		},
		req.Revision,
	})
}

type expandFunc func(ctx context.Context, start *core.ObjectAndRelation, requests []ReduceableExpandFunc) ExpandResult

func expandTupleToUserset[T relation](
	_ context.Context,
	ce *ConcurrentExpander,
	req ValidatedExpandRequest,
	ttu ttu[T],
	expandFunc expandFunc,
) ReduceableExpandFunc {
	return func(ctx context.Context, resultChan chan<- ExpandResult) {
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
		it, err := ds.QueryRelationships(ctx, datastore.RelationshipsFilter{
			OptionalResourceType:     req.ResourceAndRelation.Namespace,
			OptionalResourceIds:      []string{req.ResourceAndRelation.ObjectId},
			OptionalResourceRelation: ttu.GetTupleset().GetRelation(),
		})
		if err != nil {
			resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
			return
		}

		var requestsToDispatch []ReduceableExpandFunc
		for rel, err := range it {
			if err != nil {
				resultChan <- expandResultError(NewExpansionFailureErr(err), emptyMetadata)
				return
			}

			toDispatch := ce.expandComputedUserset(ctx, req, ttu.GetComputedUserset(), &rel)
			requestsToDispatch = append(requestsToDispatch, decorateWithCaveatIfNecessary(toDispatch, caveats.CaveatAsExpr(rel.OptionalCaveat)))
		}

		resultChan <- expandFunc(ctx, req.ResourceAndRelation, requestsToDispatch)
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
			responseMetadata = combineResponseMetadata(ctx, responseMetadata, result.Resp.Metadata)
			if result.Err != nil {
				return expandResultError(result.Err, responseMetadata)
			}
			children = append(children, result.Resp.TreeNode)
		case <-ctx.Done():
			return expandResultError(context.Canceled, responseMetadata)
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
		return expandResultError(context.Canceled, emptyMetadata)
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

package compiler

import (
	"google.golang.org/protobuf/proto"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/decorators"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
)

// translateDecorators validates and compiles the decorators attached to the given node.
func translateDecorators(tctx *translationContext, node *dslNode, site decorators.Site) ([]*core.Decorator, error) {
	decoratorNodes := node.List(dslshape.NodePredicateDecorator)
	if len(decoratorNodes) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(decoratorNodes))
	compiled := make([]*core.Decorator, 0, len(decoratorNodes))

	for _, decoratorNode := range decoratorNodes {
		applied, err := appliedDecorator(decoratorNode)
		if err != nil {
			return nil, err
		}

		if _, found := seen[applied.Name]; found {
			return nil, decoratorNode.WithSourceErrorf(applied.Name,
				"decorator `@%s` specified more than once", applied.Name)
		}
		seen[applied.Name] = struct{}{}

		result, err := tctx.decoratorRegistry.Validate(
			applied,
			site,
			tctx.enabledFlags.Has,
			tctx.allowedFlags.Has,
		)
		if err != nil {
			return nil, decoratorNode.WithSourceErrorf(applied.Name, "%s", err.Error())
		}

		compiled = append(compiled, result)
	}

	return compiled, nil
}

// appliedDecorator reads a decorator AST node into its pre-validation form.
func appliedDecorator(decoratorNode *dslNode) (decorators.Applied, error) {
	name, err := decoratorNode.GetString(dslshape.NodeDecoratorPredicateName)
	if err != nil {
		return decorators.Applied{}, err
	}

	paramNodes := decoratorNode.List(dslshape.NodeDecoratorPredicateParameters)
	params := make([]decorators.AppliedParameter, 0, len(paramNodes))

	for _, paramNode := range paramNodes {
		paramName, err := paramNode.GetString(dslshape.NodeDecoratorParameterPredicateName)
		if err != nil {
			return decorators.Applied{}, err
		}

		kind, err := paramNode.GetString(dslshape.NodeDecoratorParameterPredicateKind)
		if err != nil {
			return decorators.Applied{}, err
		}

		value, err := paramNode.GetString(dslshape.NodeDecoratorParameterPredicateValue)
		if err != nil {
			return decorators.Applied{}, err
		}

		params = append(params, decorators.AppliedParameter{
			Name:  paramName,
			Value: decorators.Value{Kind: decorators.ValueKind(kind), Raw: value},
		})
	}

	return decorators.Applied{Name: name, Parameters: params}, nil
}

// mergeDecorators appends `incoming` to `existing`, collapsing identical duplicates and
// rejecting same-name decorators whose parameters differ.
func mergeDecorators(existing []*core.Decorator, incoming []*core.Decorator, node *dslNode) ([]*core.Decorator, error) {
	for _, candidate := range incoming {
		duplicate := false
		for _, present := range existing {
			if present.GetName() != candidate.GetName() {
				continue
			}
			if !proto.Equal(present, candidate) {
				return nil, node.WithSourceErrorf(candidate.GetName(),
					"decorator `@%s` is applied with conflicting parameters", candidate.GetName())
			}
			duplicate = true
			break
		}
		if !duplicate {
			existing = append(existing, candidate)
		}
	}
	return existing, nil
}

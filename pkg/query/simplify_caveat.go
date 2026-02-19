package query

import (
	"context"
	"maps"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// mergeContexts combines relationship context from a caveat expression with query context
// Query context takes precedence over relationship context
func mergeContexts(expr *core.CaveatExpression, queryContext map[string]any) map[string]any {
	fullContext := make(map[string]any)

	// Start with the relationship context from the caveat expression
	if expr.GetCaveat() != nil && expr.GetCaveat().Context != nil {
		maps.Copy(fullContext, expr.GetCaveat().Context.AsMap())
	}

	// Overlay with query-time context (takes precedence)
	maps.Copy(fullContext, queryContext)

	return fullContext
}

// mergeContextsForExpression recursively merges contexts for complex expressions
// For AND/OR expressions, it collects all relationship contexts from all children
func mergeContextsForExpression(expr *core.CaveatExpression, queryContext map[string]any) map[string]any {
	fullContext := make(map[string]any)

	// Collect all relationship contexts from the expression tree
	collectRelationshipContexts(expr, fullContext)

	// Overlay with query-time context (takes precedence)
	maps.Copy(fullContext, queryContext)

	return fullContext
}

// collectRelationshipContexts recursively collects relationship contexts from a caveat expression tree
func collectRelationshipContexts(expr *core.CaveatExpression, contextMap map[string]any) {
	if expr == nil {
		return
	}

	// If this is a leaf caveat, collect its context
	if expr.GetCaveat() != nil && expr.GetCaveat().Context != nil {
		for k, v := range expr.GetCaveat().Context.AsMap() {
			if _, exists := contextMap[k]; !exists {
				contextMap[k] = v
			}
		}
	}

	// If this is an operation, recursively collect from children
	if expr.GetOperation() != nil {
		for _, child := range expr.GetOperation().Children {
			collectRelationshipContexts(child, contextMap)
		}
	}
}

// SimplifyCaveatExpression simplifies a caveat expression by applying AND/OR logic and
// running them with a CaveatRunner if they match the expected caveat:
// - For AND: if a caveat evaluates to true, remove it from the expression
// - For OR: if a caveat evaluates to true, the entire expression becomes true
// Returns:
//   - simplified: the simplified expression (nil if unconditionally true)
//   - passes: true if passes unconditionally or conditionally, false if fails
//   - error: any error that occurred during simplification
func SimplifyCaveatExpression(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	if err := runner.PopulateCaveatDefinitionsForExpr(ctx, expr, reader); err != nil {
		return nil, false, err
	}

	return simplifyCaveatExpressionInternal(ctx, runner, expr, context, reader)
}

func simplifyCaveatExpressionInternal(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	// If this is a leaf caveat, evaluate it
	if expr.GetCaveat() != nil {
		return evaluateLeaf(ctx, runner, expr, context, reader)
	}

	// Handle operation nodes
	cop := expr.GetOperation()
	if cop == nil {
		return expr, true, nil // Unknown operation, treat as conditional
	}

	return simplifyOperation(ctx, runner, expr, cop, context, reader)
}

func evaluateLeaf(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	// For complex expressions (AND/OR), we need to collect contexts from the entire expression tree
	var fullContext map[string]any
	if expr.GetOperation() != nil {
		// Complex expression - collect all relationship contexts
		fullContext = mergeContextsForExpression(expr, context)
	} else {
		// Simple leaf caveat - use simple context merging
		fullContext = mergeContexts(expr, context)
	}

	result, err := runner.RunCaveatExpression(ctx, expr, fullContext, reader, caveats.RunCaveatExpressionNoDebugging)
	if err != nil {
		return nil, false, err
	}

	if result.IsPartial() {
		// Caveat is partial, return as-is - this is conditional (passes if context provided later)
		return expr, true, nil
	}

	if result.Value() {
		// Caveat is true - passes unconditionally
		return nil, true, nil // nil indicates "true"
	} else {
		// Caveat is false - fails
		return expr, false, nil // Keep the expression to indicate what failed
	}
}

func simplifyOperation(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	cop *core.CaveatOperation,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	switch cop.Op {
	case core.CaveatOperation_AND:
		return simplifyAndOperation(ctx, runner, cop, context, reader)
	case core.CaveatOperation_OR:
		return simplifyOrOperation(ctx, runner, expr, cop, context, reader)
	case core.CaveatOperation_NOT:
		return simplifyNotOperation(ctx, runner, expr, cop, context, reader)
	default:
		// Unknown operation, treat as conditional
		return expr, true, nil
	}
}

func simplifyAndOperation(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	cop *core.CaveatOperation,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	var simplifiedChildren []*core.CaveatExpression

	for _, child := range cop.Children {
		simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
		if err != nil {
			return nil, false, err
		}

		switch {
		case simplified == nil && passes:
			// Child evaluated to true unconditionally - remove it from AND
			continue
		case !passes:
			// Child failed - entire AND fails
			return simplified, false, nil
		default:
			// Child is conditional - keep it
			simplifiedChildren = append(simplifiedChildren, simplified)
		}
	}

	// All children processed for AND
	if len(simplifiedChildren) == 0 {
		// All children were true, so AND is true
		return nil, true, nil
	}
	if len(simplifiedChildren) == 1 {
		// Only one child left, return it directly
		return simplifiedChildren[0], true, nil
	}

	// Return simplified AND with remaining children
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_AND,
				Children: simplifiedChildren,
			},
		},
	}, true, nil
}

func simplifyOrOperation(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	cop *core.CaveatOperation,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	var simplifiedChildren []*core.CaveatExpression

	for _, child := range cop.Children {
		simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
		if err != nil {
			return nil, false, err
		}

		switch {
		case simplified == nil && passes:
			// Child evaluated to true unconditionally - entire OR is true
			return nil, true, nil
		case !passes:
			// Child failed - remove it from OR
			continue
		default:
			// Child is conditional - keep it
			simplifiedChildren = append(simplifiedChildren, simplified)
		}
	}

	// All children processed for OR
	if len(simplifiedChildren) == 0 {
		// All children were false, so OR is false
		return expr, false, nil
	}
	if len(simplifiedChildren) == 1 {
		// Only one child left, return it directly
		return simplifiedChildren[0], true, nil
	}

	// Return simplified OR with remaining children
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_OR,
				Children: simplifiedChildren,
			},
		},
	}, true, nil
}

func simplifyNotOperation(
	ctx context.Context,
	runner *caveats.CaveatRunner,
	expr *core.CaveatExpression,
	cop *core.CaveatOperation,
	context map[string]any,
	reader caveats.CaveatDefinitionLookup,
) (*core.CaveatExpression, bool, error) {
	if len(cop.Children) != 1 {
		// NOT should have exactly one child
		return expr, true, spiceerrors.MustBugf("simplifyNotOperation: returned a Caveat NOT operation with more than one child")
	}

	child := cop.Children[0]
	simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
	if err != nil {
		return nil, false, err
	}

	// Since we're simplifying the NOT operation, the results are basically flipped.

	if simplified == nil && passes {
		// Child caveat evaluated to true unconditionally - NOT true is false
		return expr, false, nil // Return original expr to indicate what failed
	}

	if !passes {
		// Child caveat did NOT pass the caveat check, or simplified to impossible.
		// Since the NOT of false is true, we DO pass the check, unconditionally.
		return nil, true, nil
	}

	// Child is conditional - NOT conditional is still conditional
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_NOT,
				Children: []*core.CaveatExpression{simplified},
			},
		},
	}, true, nil
}

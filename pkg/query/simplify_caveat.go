package query

import (
	"context"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// SimplifyCaveatExpression simplifies a caveat expression by applying AND/OR logic:
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
	reader datastore.CaveatReader,
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
	reader datastore.CaveatReader,
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
	reader datastore.CaveatReader,
) (*core.CaveatExpression, bool, error) {
	result, err := runner.RunCaveatExpression(ctx, expr, context, reader, caveats.RunCaveatExpressionNoDebugging)
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
	reader datastore.CaveatReader,
) (*core.CaveatExpression, bool, error) {
	switch cop.Op {
	case core.CaveatOperation_AND:
		return simplifyAndOperation(ctx, runner, expr, cop, context, reader)
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
	expr *core.CaveatExpression,
	cop *core.CaveatOperation,
	context map[string]any,
	reader datastore.CaveatReader,
) (*core.CaveatExpression, bool, error) {
	var simplifiedChildren []*core.CaveatExpression

	for _, child := range cop.Children {
		simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
		if err != nil {
			return nil, false, err
		}

		if simplified == nil && passes {
			// Child evaluated to true unconditionally - remove it from AND
			continue
		} else if !passes {
			// Child failed - entire AND fails
			return simplified, false, nil
		} else {
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
	reader datastore.CaveatReader,
) (*core.CaveatExpression, bool, error) {
	var simplifiedChildren []*core.CaveatExpression

	for _, child := range cop.Children {
		simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
		if err != nil {
			return nil, false, err
		}

		if simplified == nil && passes {
			// Child evaluated to true unconditionally - entire OR is true
			return nil, true, nil
		} else if !passes {
			// Child failed - remove it from OR
			continue
		} else {
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
	reader datastore.CaveatReader,
) (*core.CaveatExpression, bool, error) {
	if len(cop.Children) != 1 {
		// NOT should have exactly one child
		return expr, true, nil
	}

	child := cop.Children[0]
	simplified, passes, err := simplifyCaveatExpressionInternal(ctx, runner, child, context, reader)
	if err != nil {
		return nil, false, err
	}

	if simplified == nil && passes {
		// Child evaluated to true unconditionally - NOT true is false
		return expr, false, nil // Return original expr to indicate what failed
	} else if !passes {
		// Child failed - NOT false is true
		return nil, true, nil
	} else {
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
}

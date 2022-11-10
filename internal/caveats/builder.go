package caveats

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// CaveatForTesting returns a new ContextualizedCaveat for testing, with empty context.
func CaveatForTesting(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
	}
}

// CaveatExprForTesting returns a CaveatExpression referencing a caveat with the given name and
// empty context.
func CaveatExprForTesting(name string) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Caveat{
			Caveat: CaveatForTesting(name),
		},
	}
}

// ShortcircuitedOr combines two caveat expressions via an `||`. If one of the expressions is nil,
// then the entire expression is *short-circuited*, and a nil is returned.
func ShortcircuitedOr(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil || second == nil {
		return nil
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// Or `||`'s together two caveat expressions. If one expression is nil, the other is returned.
func Or(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	if first.EqualVT(second) {
		return first
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_OR,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// And `&&`'s together two caveat expressions. If one expression is nil, the other is returned.
func And(first *v1.CaveatExpression, second *v1.CaveatExpression) *v1.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	if first.EqualVT(second) {
		return first
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_AND,
				Children: []*v1.CaveatExpression{first, second},
			},
		},
	}
}

// Invert returns the caveat expression with a `!` placed in front of it. If the expression is
// nil, returns nil.
func Invert(ce *v1.CaveatExpression) *v1.CaveatExpression {
	if ce == nil {
		return nil
	}

	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Operation{
			Operation: &v1.CaveatOperation{
				Op:       v1.CaveatOperation_NOT,
				Children: []*v1.CaveatExpression{ce},
			},
		},
	}
}

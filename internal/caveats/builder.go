package caveats

import (
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatAsExpr wraps a contextualized caveat into a caveat expression.
func CaveatAsExpr(caveat *core.ContextualizedCaveat) *core.CaveatExpression {
	if caveat == nil {
		return nil
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: caveat,
		},
	}
}

// CaveatForTesting returns a new ContextualizedCaveat for testing, with empty context.
func CaveatForTesting(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
	}
}

// CaveatExprForTesting returns a CaveatExpression referencing a caveat with the given name and
// empty context.
func CaveatExprForTesting(name string) *core.CaveatExpression {
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: CaveatForTesting(name),
		},
	}
}

// CaveatExprForTesting returns a CaveatExpression referencing a caveat with the given name and
// empty context.
func MustCaveatExprForTestingWithContext(name string, context map[string]any) *core.CaveatExpression {
	contextStruct, err := structpb.NewStruct(context)
	if err != nil {
		panic(err)
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: name,
				Context:    contextStruct,
			},
		},
	}
}

// ShortcircuitedOr combines two caveat expressions via an `||`. If one of the expressions is nil,
// then the entire expression is *short-circuited*, and a nil is returned.
func ShortcircuitedOr(first *core.CaveatExpression, second *core.CaveatExpression) *core.CaveatExpression {
	if first == nil || second == nil {
		return nil
	}

	return Or(first, second)
}

// Or `||`'s together two caveat expressions. If one expression is nil, the other is returned.
func Or(first *core.CaveatExpression, second *core.CaveatExpression) *core.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	if first.EqualVT(second) {
		return first
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_OR,
				Children: []*core.CaveatExpression{first, second},
			},
		},
	}
}

// And `&&`'s together two caveat expressions. If one expression is nil, the other is returned.
func And(first *core.CaveatExpression, second *core.CaveatExpression) *core.CaveatExpression {
	if first == nil {
		return second
	}

	if second == nil {
		return first
	}

	if first.EqualVT(second) {
		return first
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_AND,
				Children: []*core.CaveatExpression{first, second},
			},
		},
	}
}

// Invert returns the caveat expression with a `!` placed in front of it. If the expression is
// nil, returns nil.
func Invert(ce *core.CaveatExpression) *core.CaveatExpression {
	if ce == nil {
		return nil
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_NOT,
				Children: []*core.CaveatExpression{ce},
			},
		},
	}
}

// Subtract returns a caveat expression representing the subtracted expression subtracted from the given
// expression.
func Subtract(caveat *core.CaveatExpression, subtracted *core.CaveatExpression) *core.CaveatExpression {
	inversion := Invert(subtracted)
	if caveat == nil {
		return inversion
	}

	if subtracted == nil {
		return caveat
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op:       core.CaveatOperation_AND,
				Children: []*core.CaveatExpression{caveat, inversion},
			},
		},
	}
}

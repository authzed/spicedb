package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestBuildDebugInformation(t *testing.T) {
	tcs := []struct {
		name               string
		result             func(t *testing.T) ExpressionResult
		expectedContext    map[string]any
		expectedExprString string
	}{
		{
			name:               "basic direct caveat",
			result:             eval("true", nil),
			expectedContext:    map[string]any{},
			expectedExprString: "true",
		},
		{
			name: "basic variable",
			result: eval("input == 5", map[string]any{
				"input": 5,
			}),
			expectedContext: map[string]any{
				"input": 5.0,
			},
			expectedExprString: "input == 5",
		},
		{
			name: "basic operation",
			result: eval("input == 5 && input2 == 64", map[string]any{
				"input":  5,
				"input2": 64,
			}),
			expectedContext: map[string]any{
				"input":  5.0,
				"input2": 64.0,
			},
			expectedExprString: "input == 5 && input2 == 64",
		},
		{
			name: "nested operation",
			result: and(
				eval("input == 5", map[string]any{
					"input": 5,
				}),
				eval("input2 == 64", map[string]any{
					"input2": 64,
				}),
			),
			expectedContext: map[string]any{
				"input":  5.0,
				"input2": 64.0,
			},
			expectedExprString: "(input == 5) && (input2 == 64)",
		},
		{
			name: "nested AND operation with name reuse",
			result: and(
				eval("input == 5", map[string]any{
					"input": 5,
				}),
				eval("input == 64", map[string]any{
					"input": 64,
				}),
				eval("input == 42", map[string]any{
					"input": 42,
				}),
			),
			expectedContext: map[string]any{
				"input__0": 5.0,
				"input__1": 64.0,
				"input__2": 42.0,
			},
			expectedExprString: "(input__0 == 5) && (input__1 == 64) && (input__2 == 42)",
		},
		{
			name: "nested OR operation with name reuse",
			result: or(
				eval("input == 5", map[string]any{
					"input": 5,
				}),
				eval("input == 64", map[string]any{
					"input": 64,
				}),
				eval("input == 42", map[string]any{
					"input": 42,
				}),
			),
			expectedContext: map[string]any{
				"input__0": 5.0,
				"input__1": 64.0,
				"input__2": 42.0,
			},
			expectedExprString: "(input__0 == 5) || (input__1 == 64) || (input__2 == 42)",
		},
		{
			name: "NOT operation ",
			result: not(
				eval("input == 5", map[string]any{
					"input": 5,
				}),
			),
			expectedContext: map[string]any{
				"input": 5.0,
			},
			expectedExprString: "!(input == 5)",
		},
		{
			name: "complex operation with some name reuse",
			result: and(
				not(
					eval("input == 5", map[string]any{
						"input": 5,
					}),
				),
				or(
					eval("input == 64", map[string]any{
						"input": 64,
					}),
					eval("input2 == 42", map[string]any{
						"input2": 42,
					}),
				),
				eval("input3 == 56", map[string]any{
					"input3": 123,
				}),
			),
			expectedContext: map[string]any{
				"input__0": 5.0,
				"input__1": 64.0,
				"input2":   42.0,
				"input3":   123.0,
			},
			expectedExprString: "(!(input__0 == 5)) && ((input__1 == 64) || (input2 == 42)) && (input3 == 56)",
		},
		{
			name: "nested AND operation with multiple name reuse",
			result: and(
				eval("a + b + c == 5", map[string]any{
					"a": 2,
					"b": 3,
					"c": 1200,
				}),
				eval("a - b - d == 64", map[string]any{
					"a": 64,
					"b": 0,
					"d": 2400,
				}),
			),
			expectedContext: map[string]any{
				"a__0": 2.0,
				"b__0": 3.0,
				"c":    1200.0,
				"a__1": 64.0,
				"b__1": 0.0,
				"d":    2400.0,
			},
			expectedExprString: "(a__0 + b__0 + c == 5) && (a__1 - b__1 - d == 64)",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			exprString, contextStruct, err := BuildDebugInformation(tc.result(t))
			require.NoError(t, err)

			require.Equal(t, tc.expectedExprString, exprString)
			require.Equal(t, tc.expectedContext, contextStruct.AsMap())
		})
	}
}

func and(opFuncs ...func(t *testing.T) ExpressionResult) func(t *testing.T) ExpressionResult {
	return func(t *testing.T) ExpressionResult {
		ops := make([]ExpressionResult, 0, len(opFuncs))
		for _, opFunc := range opFuncs {
			ops = append(ops, opFunc(t))
		}

		return syntheticResult{
			value:               true,
			op:                  core.CaveatOperation_AND,
			exprResultsForDebug: ops,
		}
	}
}

func or(opFuncs ...func(t *testing.T) ExpressionResult) func(t *testing.T) ExpressionResult {
	return func(t *testing.T) ExpressionResult {
		ops := make([]ExpressionResult, 0, len(opFuncs))
		for _, opFunc := range opFuncs {
			ops = append(ops, opFunc(t))
		}

		return syntheticResult{
			value:               true,
			op:                  core.CaveatOperation_OR,
			exprResultsForDebug: ops,
		}
	}
}

func not(opFuncs ...func(t *testing.T) ExpressionResult) func(t *testing.T) ExpressionResult {
	return func(t *testing.T) ExpressionResult {
		ops := make([]ExpressionResult, 0, len(opFuncs))
		for _, opFunc := range opFuncs {
			ops = append(ops, opFunc(t))
		}

		return syntheticResult{
			value:               true,
			op:                  core.CaveatOperation_NOT,
			exprResultsForDebug: ops,
		}
	}
}

func eval(expr string, context map[string]any) func(t *testing.T) ExpressionResult {
	return func(t *testing.T) ExpressionResult {
		env := caveats.NewEnvironment()

		for name := range context {
			require.NoError(t, env.AddVariable(name, types.AnyType))
		}

		caveat, err := caveats.CompileCaveatWithName(env, expr, "test")
		require.NoError(t, err)

		result, err := caveats.EvaluateCaveat(caveat, context)
		require.NoError(t, err)

		return result
	}
}

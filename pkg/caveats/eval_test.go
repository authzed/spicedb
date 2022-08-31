package caveats

import (
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
)

func TestEvaluateCaveat(t *testing.T) {
	wetTz, err := time.LoadLocation("WET")
	require.NoError(t, err)
	tcs := []struct {
		name       string
		env        *Environment
		exprString string

		context map[string]any

		expectedError string

		expectedValue       bool
		expectedPartialExpr string
	}{
		{
			"static expression",
			mustEnvForVariables(map[string]VariableType{}),
			"true",
			map[string]any{},
			"",
			true,
			"",
		},
		{
			"static false expression",
			mustEnvForVariables(map[string]VariableType{}),
			"false",
			map[string]any{},
			"",
			false,
			"",
		},
		{
			"static numeric expression",
			mustEnvForVariables(map[string]VariableType{}),
			"1 + 2 == 3",
			map[string]any{},
			"",
			true,
			"",
		},
		{
			"static false numeric expression",
			mustEnvForVariables(map[string]VariableType{}),
			"2 - 2 == 1",
			map[string]any{},
			"",
			false,
			"",
		},
		{
			"computed expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
			}),
			"a + 2 == 4",
			map[string]any{
				"a": 2,
			},
			"",
			true,
			"",
		},
		{
			"missing variables for expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
			}),
			"a + 2 == 4",
			map[string]any{},
			"",
			false,
			"a + 2 == 4",
		},
		{
			"missing variables for right side of boolean expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"(a == 2) || (b == 6)",
			map[string]any{
				"a": 2,
			},
			"",
			true,
			"",
		},
		{
			"missing variables for left side of boolean expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"(a == 2) || (b == 6)",
			map[string]any{
				"b": 6,
			},
			"",
			true,
			"",
		},
		{
			"missing variables for both sides of boolean expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"(a == 2) || (b == 6)",
			map[string]any{},
			"",
			false,
			"a == 2 || b == 6",
		},
		{
			"missing variable for left side of and boolean expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"(a == 2) && (b == 6)",
			map[string]any{
				"b": 6,
			},
			"",
			false,
			"a == 2 && true",
		},
		{
			"missing variable for right side of and boolean expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"(a == 2) && (b == 6)",
			map[string]any{
				"a": 2,
			},
			"",
			false,
			"true && b == 6",
		},
		{
			"map evaluation",
			mustEnvForVariables(map[string]VariableType{
				"m":   MapType(IntType, BooleanType),
				"idx": IntType,
			}),
			"m[idx]",
			map[string]any{
				"m": map[int]bool{
					1: true,
				},
				"idx": 1,
			},
			"",
			true,
			"",
		},
		{
			"map evaluation, wrong map kind",
			mustEnvForVariables(map[string]VariableType{
				"m":   MapType(IntType, BooleanType),
				"idx": IntType,
			}),
			"m[idx]",
			map[string]any{
				"m": map[string]bool{
					"1": true,
				},
				"idx": 1,
			},
			"no such key: 1",
			false,
			"",
		},
		{
			"map dot evaluation",
			mustEnvForVariables(map[string]VariableType{
				"m": MapType(StringType, BooleanType),
			}),
			"m.foo",
			map[string]any{
				"m": map[string]bool{
					"foo": true,
				},
			},
			"",
			true,
			"",
		},
		{
			"nested evaluation",
			mustEnvForVariables(map[string]VariableType{
				"metadata.l":   ListType(StringType),
				"metadata.idx": IntType,
			}),
			"metadata.l[metadata.idx] == 'hello'",
			map[string]any{
				"metadata.l":   []string{"hi", "hello", "yo"},
				"metadata.idx": 1,
			},
			"",
			true,
			"",
		},
		{
			"nested evaluation with missing value",
			mustEnvForVariables(map[string]VariableType{
				"metadata.l":   ListType(StringType),
				"metadata.idx": IntType,
			}),
			"metadata.l[metadata.idx] == 'hello'",
			map[string]any{
				"metadata.l": []string{"hi", "hello", "yo"},
			},
			"",
			false,
			`metadata.l[metadata.idx] == "hello"`,
		},
		{
			"nested evaluation with missing list",
			mustEnvForVariables(map[string]VariableType{
				"metadata.l":   ListType(StringType),
				"metadata.idx": IntType,
			}),
			"metadata.l[metadata.idx] == 'hello'",
			map[string]any{
				"metadata.idx": 1,
			},
			"",
			false,
			`metadata.l[metadata.idx] == "hello"`,
		},
		{
			"timestamp operations default to UTC",
			mustEnvForVariables(map[string]VariableType{
				"a": TimestampType,
			}),
			"a.getHours() == 9",
			map[string]any{
				"a": time.Date(2000, 10, 10, 10, 10, 10, 10, wetTz),
			},
			"",
			true,
			"",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := CompileCaveat(tc.env, tc.exprString)
			require.NoError(t, err)

			result, err := EvaluateCaveat(compiled, tc.context)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tc.expectedValue, result.Value())

				if tc.expectedPartialExpr != "" {
					require.True(t, result.IsPartial())

					partialValue, err := result.PartialValue()
					require.NoError(t, err)

					astExpr, err := cel.AstToString(partialValue.ast)
					require.NoError(t, err)

					require.Equal(t, tc.expectedPartialExpr, astExpr)
				} else {
					require.False(t, result.IsPartial())
					_, partialErr := result.PartialValue()
					require.Error(t, partialErr)
				}
			}
		})
	}
}

func TestPartialEvaluation(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"a": IntType,
		"b": IntType,
	}), "a + b > 47")
	require.NoError(t, err)

	result, err := EvaluateCaveat(compiled, map[string]any{
		"a": 42,
	})
	require.NoError(t, err)
	require.False(t, result.Value())
	require.True(t, result.IsPartial())

	partialValue, err := result.PartialValue()
	require.NoError(t, err)

	astExpr, err := cel.AstToString(partialValue.ast)
	require.NoError(t, err)
	require.Equal(t, "42 + b > 47", astExpr)

	fullResult, err := EvaluateCaveat(partialValue, map[string]any{
		"b": 6,
	})
	require.NoError(t, err)
	require.True(t, fullResult.Value())
	require.False(t, fullResult.IsPartial())

	fullResult, err = EvaluateCaveat(partialValue, map[string]any{
		"b": 2,
	})
	require.NoError(t, err)
	require.False(t, fullResult.Value())
	require.False(t, fullResult.IsPartial())
}

func TestEvalWithMaxCost(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"a": IntType,
		"b": IntType,
	}), "a + b > 47")
	require.NoError(t, err)

	_, err = EvaluateCaveatWithConfig(compiled, map[string]any{
		"a": 42,
		"b": 4,
	}, &EvaluationConfig{
		MaxCost: 1,
	})
	require.Error(t, err)
	require.Equal(t, "operation cancelled: actual cost limit exceeded", err.Error())
}

func TestEvalWithNesting(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"foo.a": IntType,
		"foo.b": IntType,
	}), "foo.a + foo.b > 47")
	require.NoError(t, err)

	result, err := EvaluateCaveat(compiled, map[string]any{
		"foo.a": 42,
		"foo.b": 4,
	})
	require.NoError(t, err)
	require.False(t, result.Value())
	require.False(t, result.IsPartial())
}

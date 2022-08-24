package caveats

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	tcs := []struct {
		name           string
		env            *Environment
		exprString     string
		expectedErrors []string
	}{
		{
			"missing var",
			NewEnvironment(),
			"hiya",
			[]string{"undeclared reference to 'hiya'"},
		},
		{
			"empty expression",
			NewEnvironment(),
			"",
			[]string{"mismatched input"},
		},
		{
			"invalid expression",
			NewEnvironment(),
			"a +",
			[]string{"mismatched input"},
		},
		{
			"missing variable",
			NewEnvironment(),
			"a + 2",
			[]string{"undeclared reference to 'a'"},
		},
		{
			"missing variables",
			NewEnvironment(),
			"a + b",
			[]string{"undeclared reference to 'a'", "undeclared reference to 'b'"},
		},
		{
			"type mismatch",
			mustEnvForVariables(map[string]VariableType{
				"a": UIntType,
				"b": BooleanType,
			}),
			"a + b",
			[]string{"found no matching overload for '_+_'"},
		},
		{
			"valid expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"a + b == 2",
			[]string{},
		},
		{
			"invalid expression over an int",
			mustEnvForVariables(map[string]VariableType{
				"a": UIntType,
			}),
			"a[0]",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"valid expression over a list",
			mustEnvForVariables(map[string]VariableType{
				"a": ListType(IntType),
			}),
			"a[0] == 1",
			[]string{},
		},
		{
			"invalid expression over a list",
			mustEnvForVariables(map[string]VariableType{
				"a": ListType(UIntType),
			}),
			"a['hi']",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"valid expression over a map",
			mustEnvForVariables(map[string]VariableType{
				"a": MapType(StringType, IntType),
			}),
			"a['hi'] == 1",
			[]string{},
		},
		{
			"invvalid expression over a map",
			mustEnvForVariables(map[string]VariableType{
				"a": MapType(BooleanType, UIntType),
			}),
			"a['hi']",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"non-boolean valid expression",
			mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			}),
			"a + b",
			[]string{"caveat expression must result in a boolean value: found `int`"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := CompileCaveat(tc.env, tc.exprString)
			if len(tc.expectedErrors) == 0 {
				require.NoError(t, err)
				require.NotNil(t, compiled)
			} else {
				require.Error(t, err)
				require.Nil(t, compiled)

				isCompilationError := errors.As(err, &CompilationErrors{})
				require.True(t, isCompilationError)

				for _, expectedError := range tc.expectedErrors {
					require.Contains(t, err.Error(), expectedError)
				}
			}
		})
	}
}

func TestSerialization(t *testing.T) {
	exprs := []string{"a == 1", "a + b == 2", "b - a == 4"}

	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			env := mustEnvForVariables(map[string]VariableType{
				"a": IntType,
				"b": IntType,
			})
			compiled, err := CompileCaveat(env, expr)
			require.NoError(t, err)

			serialized, err := compiled.Serialize()
			require.NoError(t, err)

			deserialized, err := DeserializeCaveat(env, serialized)
			require.NoError(t, err)

			astExpr, err := deserialized.ExprString()
			require.NoError(t, err)
			require.Equal(t, expr, astExpr)
		})
	}
}

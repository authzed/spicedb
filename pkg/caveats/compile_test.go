package caveats

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats/types"
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
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.UIntType,
				"b": types.BooleanType,
			}),
			"a + b",
			[]string{"found no matching overload for '_+_'"},
		},
		{
			"valid expression",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.IntType,
				"b": types.IntType,
			}),
			"a + b == 2",
			[]string{},
		},
		{
			"invalid expression over an int",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.UIntType,
			}),
			"a[0]",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"valid expression over a list",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.MustListType(types.IntType),
			}),
			"a[0] == 1",
			[]string{},
		},
		{
			"invalid expression over a list",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.MustListType(types.UIntType),
			}),
			"a['hi']",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"valid expression over a map",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.MustMapType(types.IntType),
			}),
			"a['hi'] == 1",
			[]string{},
		},
		{
			"invalid expression over a map",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.MustMapType(types.UIntType),
			}),
			"a[42]",
			[]string{"found no matching overload for '_[_]'"},
		},
		{
			"non-boolean valid expression",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.IntType,
				"b": types.IntType,
			}),
			"a + b",
			[]string{"caveat expression must result in a boolean value: found `int`"},
		},
		{
			"valid expression over a byte sequence",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.BytesType,
			}),
			"a == b\"abc\"",
			[]string{},
		},
		{
			"invalid expression over a byte sequence",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.BytesType,
			}),
			"a == \"abc\"",
			[]string{"found no matching overload for '_==_'"},
		},
		{
			"valid expression over a double",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.DoubleType,
			}),
			"a == 7.23",
			[]string{},
		},
		{
			"invalid expression over a double",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.DoubleType,
			}),
			"a == true",
			[]string{"found no matching overload for '_==_'"},
		},
		{
			"valid expression over a duration",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.DurationType,
			}),
			"a > duration(\"1h3m\")",
			[]string{},
		},
		{
			"invalid expression over a duration",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.DurationType,
			}),
			"a > \"1h3m\"",
			[]string{"found no matching overload for '_>_'"},
		},
		{
			"valid expression over a timestamp",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.TimestampType,
			}),
			"a == timestamp(\"1972-01-01T10:00:20.021-05:00\")",
			[]string{},
		},
		{
			"invalid expression over a timestamp",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.TimestampType,
			}),
			"a == \"1972-01-01T10:00:20.021-05:00\"",
			[]string{"found no matching overload for '_==_'"},
		},
		{
			"valid expression over any type",
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.AnyType,
			}),
			"a == true",
			[]string{},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := compileCaveat(tc.env, tc.exprString)
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

func TestDeserializeEmpty(t *testing.T) {
	_, err := DeserializeCaveat([]byte{}, nil)
	require.NotNil(t, err)
}

func TestSerialization(t *testing.T) {
	exprs := []string{"a == 1", "a + b == 2", "b - a == 4", "l.all(i, i > 42)"}

	for _, expr := range exprs {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			vars := map[string]types.VariableType{
				"a": types.IntType,
				"b": types.IntType,
				"l": types.MustListType(types.IntType),
			}

			env := MustEnvForVariables(vars)
			compiled, err := compileCaveat(env, expr)
			require.NoError(t, err)

			serialized, err := compiled.Serialize()
			require.NoError(t, err)

			deserialized, err := DeserializeCaveat(serialized, vars)
			require.NoError(t, err)

			astExpr, err := deserialized.ExprString()
			require.NoError(t, err)
			require.Equal(t, expr, astExpr)
		})
	}
}

func TestSerializeName(t *testing.T) {
	vars := map[string]types.VariableType{
		"a": types.IntType,
		"b": types.IntType,
	}

	env := MustEnvForVariables(vars)
	compiled, err := CompileCaveatWithName(env, "a == 1", "hi")
	require.NoError(t, err)

	serialized, err := compiled.Serialize()
	require.NoError(t, err)

	deserialized, err := DeserializeCaveat(serialized, vars)
	require.NoError(t, err)

	require.Equal(t, "hi", deserialized.name)
}

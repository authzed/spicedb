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
				"a": UIntType,
				"b": UIntType,
			}),
			"a + b",
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
				"a": ListType(UIntType),
			}),
			"a[0]",
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
				"a": MapType(StringType, UIntType),
			}),
			"a['hi']",
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

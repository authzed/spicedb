package caveats

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/caveats/types"
)

func TestReferencedParameters(t *testing.T) {
	tcs := []struct {
		env                  *Environment
		expr                 string
		referencedParamNames []string
	}{
		{
			MustEnvForVariables(map[string]types.VariableType{
				"hi": types.BooleanType,
			}),
			"hi",
			[]string{
				"hi",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.IntType,
				"b": types.IntType,
			}),
			"a == 42",
			[]string{
				"a",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"a": types.MustMapType(types.StringType),
				"b": types.StringType,
			}),
			"a[b] == 'hi'",
			[]string{
				"a", "b",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"name":  types.StringType,
				"group": types.StringType,
			}),
			`name.startsWith("/groups/" + group)`,
			[]string{
				"group", "name",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"somemap": types.MustMapType(types.StringType),
			}),
			`somemap.foo == 'hi'`,
			[]string{
				"somemap",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"tweets":    types.MustListType(types.MustMapType(types.IntType)),
				"something": types.IntType,
			}),
			`tweets.all(t, t.size <= 140 && something > 42)`,
			[]string{
				"tweets",
				"something",
			},
		},
		{
			MustEnvForVariables(map[string]types.VariableType{
				"tweets":    types.MustListType(types.MustMapType(types.IntType)),
				"something": types.IntType,
			}),
			`tweets.all(t, t.size <= 140)`,
			[]string{
				"tweets",
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.expr, func(t *testing.T) {
			compiled, err := compileCaveat(tc.env, tc.expr)
			require.NoError(t, err)

			sort.Strings(tc.referencedParamNames)

			found, err := compiled.ReferencedParameters(maps.Keys(tc.env.variables))
			require.NoError(t, err)

			foundSlice := found.AsSlice()
			sort.Strings(foundSlice)
			require.Equal(t, tc.referencedParamNames, foundSlice)
		})
	}
}

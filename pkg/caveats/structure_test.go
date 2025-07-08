package caveats

import (
	"maps"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats/types"
)

func TestReferencedParameters(t *testing.T) {
	tcs := []struct {
		env                  *Environment
		expr                 string
		referencedParamNames []string
	}{
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"hi": types.Default.BooleanType,
			}),
			"hi",
			[]string{
				"hi",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"a": types.Default.IntType,
				"b": types.Default.IntType,
			}),
			"a == 42",
			[]string{
				"a",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"a": types.Default.MustMapType(types.Default.StringType),
				"b": types.Default.StringType,
			}),
			"a[b] == 'hi'",
			[]string{
				"a", "b",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"name":  types.Default.StringType,
				"group": types.Default.StringType,
			}),
			`name.startsWith("/groups/" + group)`,
			[]string{
				"group", "name",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"somemap": types.Default.MustMapType(types.Default.StringType),
			}),
			`somemap.foo == 'hi'`,
			[]string{
				"somemap",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"tweets":    types.Default.MustListType(types.Default.MustMapType(types.Default.IntType)),
				"something": types.Default.IntType,
			}),
			`tweets.all(t, t.size <= 140 && something > 42)`,
			[]string{
				"tweets",
				"something",
			},
		},
		{
			MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
				"tweets":    types.Default.MustListType(types.Default.MustMapType(types.Default.IntType)),
				"something": types.Default.IntType,
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

			found, err := compiled.ReferencedParameters(slices.Collect(maps.Keys(tc.env.variables)))
			require.NoError(t, err)

			foundSlice := found.AsSlice()
			sort.Strings(foundSlice)
			require.Equal(t, tc.referencedParamNames, foundSlice)
		})
	}
}

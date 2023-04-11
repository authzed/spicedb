package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/caveats/types"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestCaveatDiff(t *testing.T) {
	testCases := []struct {
		name           string
		existing       *core.CaveatDefinition
		updated        *core.CaveatDefinition
		expectedDeltas []Delta
	}{
		{
			"added caveat",
			nil,
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{Type: CaveatAdded},
			},
		},
		{
			"removed caveat",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			nil,
			[]Delta{
				{Type: CaveatRemoved},
			},
		},
		{
			"no changes",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{},
		},
		{
			"added parameter",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam":    types.IntType,
					"anotherparam": types.BooleanType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{Type: AddedParameter, ParameterName: "anotherparam"},
			},
		},
		{
			"removed parameter",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{Type: RemovedParameter, ParameterName: "someparam"},
			},
		},
		{
			"changed parameter type",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.BooleanType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{
					Type:          ParameterTypeChanged,
					ParameterName: "someparam",
					PreviousType:  types.EncodeParameterType(types.IntType),
					CurrentType:   types.EncodeParameterType(types.BooleanType),
				},
			},
		},
		{
			"rename parameter",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"anotherparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{Type: RemovedParameter, ParameterName: "someparam"},
				{Type: AddedParameter, ParameterName: "anotherparam"},
			},
		},
		{
			"changed expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"false",
			),
			[]Delta{
				{Type: CaveatExpressionMayHaveChanged},
			},
		},
		{
			"another changed expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"someparam == 1",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariables(map[string]types.VariableType{
					"someparam": types.IntType,
				}),
				"somecaveat",
				"someparam <= 1",
			),
			[]Delta{
				{Type: CaveatExpressionMayHaveChanged},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			diff, err := DiffCaveats(tc.existing, tc.updated)
			require.Nil(err)
			require.Equal(tc.expectedDeltas, diff.Deltas())
		})
	}
}

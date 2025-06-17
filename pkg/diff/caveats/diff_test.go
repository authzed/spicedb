package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{},
		},
		{
			"changed comments",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinitionWithComment(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"some comment",
				"true",
			),
			[]Delta{
				{Type: CaveatCommentsChanged},
			},
		},
		{
			"added parameter",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam":    caveattypes.Default.IntType,
					"anotherparam": caveattypes.Default.BooleanType,
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{}),
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.BooleanType,
				}),
				"somecaveat",
				"true",
			),
			[]Delta{
				{
					Type:          ParameterTypeChanged,
					ParameterName: "someparam",
					PreviousType:  caveattypes.EncodeParameterType(caveattypes.Default.IntType),
					CurrentType:   caveattypes.EncodeParameterType(caveattypes.Default.BooleanType),
				},
			},
		},
		{
			"rename parameter",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"anotherparam": caveattypes.Default.IntType,
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
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"true",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"false",
			),
			[]Delta{
				{Type: CaveatExpressionChanged},
			},
		},
		{
			"another changed expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == 1",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam <= 1",
			),
			[]Delta{
				{Type: CaveatExpressionChanged},
			},
		},
		{
			"third changed expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == 1",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == 2",
			),
			[]Delta{
				{Type: CaveatExpressionChanged},
			},
		},
		{
			"unchanged expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == 1",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == 1",
			),
			[]Delta{},
		},
		{
			"unchanged slightly more complex expression",
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == {\"a\": 42}.a",
			),
			ns.MustCaveatDefinition(
				caveats.MustEnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
					"someparam": caveattypes.Default.IntType,
				}),
				"somecaveat",
				"someparam == {\"a\": 42}.a",
			),
			[]Delta{},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			diff, err := DiffCaveats(tc.existing, tc.updated, caveattypes.Default.TypeSet)
			require.NoError(err)
			require.Equal(tc.expectedDeltas, diff.Deltas())
		})
	}
}

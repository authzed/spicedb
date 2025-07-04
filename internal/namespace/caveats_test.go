package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestValidateCaveatDefinition(t *testing.T) {
	tcs := []struct {
		caveat        *core.CaveatDefinition
		expectedError string
	}{
		{
			ns.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"someCondition": caveattypes.Default.IntType,
				},
			), "valid", "someCondition == 42"),
			"",
		},
		{
			ns.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{
					"someCondition": caveattypes.Default.IntType,
				},
			), "test", "true"),
			"parameter `someCondition` for caveat `test` is unused",
		},
		{
			ns.MustCaveatDefinition(caveats.MustEnvForVariablesWithDefaultTypeSet(
				map[string]caveattypes.VariableType{},
			), "test", "true"),
			"caveat `test` must have at least one parameter defined",
		},
		{
			&core.CaveatDefinition{
				SerializedExpression: []byte("123"),
			},
			"could not decode caveat",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.caveat.Name, func(t *testing.T) {
			err := ValidateCaveatDefinition(caveattypes.Default.TypeSet, tc.caveat)
			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

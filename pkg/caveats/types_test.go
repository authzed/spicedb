package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats/types"
)

func TestIPAddress(t *testing.T) {
	compiled, err := compileCaveat(MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
		"user_ip": types.Default.IPAddressType,
	}), "user_ip.in_cidr('192.168.0.0/16')")
	require.NoError(t, err)

	parsed, _ := types.ParseIPAddress("192.168.10.10")
	result, err := EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.True(t, result.Value())

	parsed, _ = types.ParseIPAddress("1.2.3.4")
	result, err = EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.False(t, result.Value())
}

func TestIPAddressIPv4Mapped(t *testing.T) {
	compiled, err := compileCaveat(MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
		"user_ip": types.Default.IPAddressType,
	}), "user_ip.in_cidr('10.0.0.0/8')")
	require.NoError(t, err)

	parsed, _ := types.ParseIPAddress("10.1.2.3")
	result, err := EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.True(t, result.Value())

	// ::ffff:10.1.2.3 is the same host as 10.1.2.3 (RFC 4291 section 2.5.5.2).
	parsed, _ = types.ParseIPAddress("::ffff:10.1.2.3")
	result, err = EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.True(t, result.Value())
}

func TestIPAddressInvalidCIDR(t *testing.T) {
	compiled, err := compileCaveat(MustEnvForVariablesWithDefaultTypeSet(map[string]types.VariableType{
		"user_ip": types.Default.IPAddressType,
	}), "user_ip.in_cidr('invalidcidr')")
	require.NoError(t, err)

	parsed, _ := types.ParseIPAddress("192.168.10.10")
	_, err = EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.Error(t, err)
	require.Equal(t, "invalid CIDR string: `invalidcidr`", err.Error())
}

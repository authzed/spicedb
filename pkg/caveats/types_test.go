package caveats

import (
	"math"
	"testing"
	"time"

	"github.com/google/cel-go/common/types"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats/customtypes"
)

func TestIPAddress(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"user_ip": IPAddressType,
	}), "user_ip.in_cidr('192.168.0.0/16')")
	require.NoError(t, err)

	parsed, _ := customtypes.ParseIPAddress("192.168.10.10")
	result, err := EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.True(t, result.Value())

	parsed, _ = customtypes.ParseIPAddress("1.2.3.4")
	result, err = EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.NoError(t, err)
	require.False(t, result.Value())
}

func TestIPAddressInvalidCIDR(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"user_ip": IPAddressType,
	}), "user_ip.in_cidr('invalidcidr')")
	require.NoError(t, err)

	parsed, _ := customtypes.ParseIPAddress("192.168.10.10")
	_, err = EvaluateCaveat(compiled, map[string]any{
		"user_ip": parsed,
	})
	require.Error(t, err)
	require.Equal(t, "invalid CIDR string: `invalidcidr`", err.Error())
}

func TestNowFunction(t *testing.T) {
	compiled, err := CompileCaveat(mustEnvForVariables(map[string]VariableType{
		"a_timestamp": TimestampType,
	}), "now() > a_timestamp")
	require.NoError(t, err)

	val, err := EvaluateCaveat(compiled, map[string]any{
		"a_timestamp": types.Timestamp{Time: time.UnixMilli(100)},
	})
	require.NoError(t, err)
	require.True(t, val.Value())

	val, err = EvaluateCaveat(compiled, map[string]any{
		"a_timestamp": types.Timestamp{Time: time.UnixMilli(math.MaxInt)},
	})
	require.NoError(t, err)
	require.False(t, val.Value())
}

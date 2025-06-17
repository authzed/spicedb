package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type testCase struct {
	vtype VariableType
}

func TestEncodeDecodeTypes(t *testing.T) {
	tcs := []testCase{
		{
			vtype: Default.IntType,
		},
		{
			vtype: Default.MustListType(Default.IntType),
		},
		{
			vtype: Default.MustListType(Default.StringType),
		},
		{
			vtype: Default.MustMapType(Default.AnyType),
		},
		{
			vtype: Default.MustMapType(Default.UIntType),
		},
		{
			vtype: Default.IPAddressType,
		},
	}

	for _, def := range Default.definitions {
		if def.childTypeCount == 0 {
			v, err := def.asVariableType(nil)
			require.NoError(t, err)
			tcs = append(tcs, testCase{
				vtype: *v,
			})
		}
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.vtype.String(), func(t *testing.T) {
			encoded := EncodeParameterType(tc.vtype)
			decoded, err := DecodeParameterType(Default.TypeSet, encoded)
			require.NoError(t, err)
			require.Equal(t, tc.vtype.String(), decoded.String())
		})
	}
}

func TestDecodeUnknownType(t *testing.T) {
	_, err := DecodeParameterType(Default.TypeSet, &core.CaveatTypeReference{
		TypeName: "unknown",
	})
	require.Error(t, err)
	require.Equal(t, "unknown caveat parameter type `unknown`", err.Error())
}

func TestDecodeWrongChildTypeCount(t *testing.T) {
	_, err := DecodeParameterType(Default.TypeSet, &core.CaveatTypeReference{
		TypeName: "list",
	})
	require.Error(t, err)
	require.Equal(t, "caveat parameter type `list` requires 0 child types; found 1", err.Error())
}

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
			vtype: IntType,
		},
		{
			vtype: ListType(IntType),
		},
		{
			vtype: ListType(StringType),
		},
		{
			vtype: MapType(AnyType),
		},
		{
			vtype: MapType(UIntType),
		},
		{
			vtype: IPAddressType,
		},
	}

	for _, def := range definitions {
		if def.childTypeCount == 0 {
			tcs = append(tcs, testCase{
				vtype: def.asVariableType(nil),
			})
		}
	}

	for _, tc := range tcs {
		t.Run(tc.vtype.String(), func(t *testing.T) {
			encoded := EncodeParameterType(tc.vtype)
			decoded, err := DecodeParameterType(encoded)
			require.NoError(t, err)
			require.Equal(t, tc.vtype.String(), decoded.String())
		})
	}
}

func TestDecodeUnknownType(t *testing.T) {
	_, err := DecodeParameterType(&core.CaveatTypeReference{
		TypeName: "unknown",
	})
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "unknown caveat parameter type `unknown`")
}

func TestDecodeWrongChildTypeCount(t *testing.T) {
	_, err := DecodeParameterType(&core.CaveatTypeReference{
		TypeName: "list",
	})
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "caveat parameter type `list` requires 0 child types; found 1")
}

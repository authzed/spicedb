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
			vtype: MustListType(IntType),
		},
		{
			vtype: MustListType(StringType),
		},
		{
			vtype: MustMapType(AnyType),
		},
		{
			vtype: MustMapType(UIntType),
		},
		{
			vtype: IPAddressType,
		},
	}

	for _, def := range definitions {
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

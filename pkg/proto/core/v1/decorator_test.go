package corev1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecoratorRoundTrips(t *testing.T) {
	ns := &NamespaceDefinition{
		Name: "document",
		Decorators: []*Decorator{
			{
				Name:         "testdef",
				RequiredFlag: "testdecorators",
				Parameters: []*DecoratorParameter{
					{Name: "count", Value: &DecoratorParameter_IntValue{IntValue: -16}},
					{Name: "label", Value: &DecoratorParameter_StringValue{StringValue: "hi"}},
					{Name: "on", Value: &DecoratorParameter_BoolValue{BoolValue: true}},
					{Name: "mode", Value: &DecoratorParameter_EnumValue{EnumValue: "hash"}},
				},
			},
		},
	}

	encoded, err := ns.MarshalVT()
	require.NoError(t, err)

	decoded := &NamespaceDefinition{}
	require.NoError(t, decoded.UnmarshalVT(encoded))

	require.Len(t, decoded.GetDecorators(), 1)
	d := decoded.GetDecorators()[0]
	require.Equal(t, "testdef", d.GetName())
	require.Equal(t, "testdecorators", d.GetRequiredFlag())
	require.Len(t, d.GetParameters(), 4)
	require.Equal(t, int64(-16), d.GetParameters()[0].GetIntValue())
	require.Equal(t, "hi", d.GetParameters()[1].GetStringValue())
	require.True(t, d.GetParameters()[2].GetBoolValue())
	require.Equal(t, "hash", d.GetParameters()[3].GetEnumValue())
}

func TestDecoratorsOnAllSites(t *testing.T) {
	require.Empty(t, (&NamespaceDefinition{}).GetDecorators())
	require.Empty(t, (&Relation{}).GetDecorators())
	require.Empty(t, (&CaveatDefinition{}).GetDecorators())
	require.Empty(t, (&AllowedRelation{}).GetDecorators())
}

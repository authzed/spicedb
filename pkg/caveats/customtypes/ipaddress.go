package customtypes

import (
	"fmt"
	"net/netip"
	"reflect"

	"github.com/google/cel-go/cel"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// ParseIPAddress parses the string form of an IP Address into an IPAddress object type.
func ParseIPAddress(ip string) (IPAddress, error) {
	parsed, err := netip.ParseAddr(ip)
	return IPAddress{parsed}, err
}

var ipaddressCelType = types.NewTypeValue("IPAddress", traits.ReceiverType)

// IPAddress defines a custom type for representing an IP Address in caveats.
type IPAddress struct {
	ip netip.Addr
}

func (ipa IPAddress) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	switch typeDesc {
	case reflect.TypeOf(""):
		return ipa.ip.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'IPAddress' to '%v'", typeDesc)
}

func (ipa IPAddress) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case types.StringType:
		return types.String(ipa.ip.String())
	case types.TypeType:
		return ipaddressCelType
	}
	return types.NewErr("type conversion error from '%s' to '%s'", ipaddressCelType, typeVal)
}

func (ipa IPAddress) Equal(other ref.Val) ref.Val {
	o2, ok := other.(IPAddress)
	if !ok {
		return types.ValOrErr(other, "no such overload")
	}
	return types.Bool(ipa == o2)
}

func (ipa IPAddress) Type() ref.Type {
	return ipaddressCelType
}

func (ipa IPAddress) Value() interface{} {
	return ipa
}

func init() {
	registerCustomType("IPAddress", cel.Function("in_cidr",
		cel.MemberOverload("ipaddress_in_cidr_string",
			[]*cel.Type{cel.ObjectType("IPAddress"), cel.StringType},
			cel.BoolType,
			cel.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
				cidr, ok := rhs.Value().(string)
				if !ok {
					return types.NewErr("expected CIDR string")
				}

				network, err := netip.ParsePrefix(cidr)
				if err != nil {
					return types.NewErr("invalid CIDR string: `%s`", cidr)
				}

				return types.Bool(network.Contains(lhs.(IPAddress).ip))
			}),
		),
	))
}

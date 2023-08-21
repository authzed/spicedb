package types

import (
	"fmt"
	"net/netip"
	"reflect"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"
)

// ParseIPAddress parses the string form of an IP Address into an IPAddress object type.
func ParseIPAddress(ip string) (IPAddress, error) {
	parsed, err := netip.ParseAddr(ip)
	return IPAddress{parsed}, err
}

// MustParseIPAddress parses the string form of an IP Address into an IPAddress object type.
func MustParseIPAddress(ip string) IPAddress {
	ipAddress, err := ParseIPAddress(ip)
	if err != nil {
		panic(err)
	}
	return ipAddress
}

var ipaddressCelType = cel.OpaqueType("IPAddress")

// IPAddress defines a custom type for representing an IP Address in caveats.
type IPAddress struct {
	ip netip.Addr
}

func (ipa IPAddress) SerializedString() string {
	return ipa.ip.String()
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

var IPAddressType = registerCustomType[IPAddress](
	"ipaddress",
	cel.ObjectType("IPAddress"),
	func(value any) (any, error) {
		ipvalue, ok := value.(IPAddress)
		if ok {
			return ipvalue, nil
		}

		vle, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("ipaddress requires an ipaddress string, found: %T `%v`", value, value)
		}

		d, err := ParseIPAddress(vle)
		if err != nil {
			return nil, fmt.Errorf("could not parse ip address string `%s`: %w", vle, err)
		}

		return d, nil
	},
	cel.Function("in_cidr",
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

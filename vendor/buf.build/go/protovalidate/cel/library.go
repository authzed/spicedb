// Copyright 2023-2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cel

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	// See https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address
	emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
)

// NewLibrary creates a new CEL library that specifies all of the functions and
// settings required by protovalidate beyond the standard definitions of the CEL
// Specification:
//
//	https://github.com/google/cel-spec/blob/master/doc/langdef.md#list-of-standard-definitions
//
// Using this function, you can create a CEL environment that is identical to
// the one used to evaluate protovalidate CEL expressions.
func NewLibrary() cel.Library {
	return &library{
		uniqueScalarPool: sync.Pool{New: func() any {
			return map[ref.Val]struct{}{}
		}},
		uniqueBytesPool: sync.Pool{New: func() any {
			return map[string]struct{}{}
		}},
	}
}

// library is the collection of functions and settings required by protovalidate
// beyond the standard definitions of the CEL Specification:
//
//	https://github.com/google/cel-spec/blob/master/doc/langdef.md#list-of-standard-definitions
//
// All implementations of protovalidate MUST implement these functions and
// should avoid exposing additional functions as they will not be portable.
type library struct {
	uniqueScalarPool sync.Pool
	uniqueBytesPool  sync.Pool
}

func (l *library) CompileOptions() []cel.EnvOption { //nolint:funlen,gocyclo
	return []cel.EnvOption{
		cel.TypeDescs(protoregistry.GlobalFiles),
		cel.DefaultUTCTimeZone(true),
		cel.CrossTypeNumericComparisons(true),
		cel.EagerlyValidateDeclarations(true),
		// TODO: reduce this to just the functionality we want to support
		ext.Strings(ext.StringsValidateFormatCalls(true)),
		cel.Variable("now", cel.TimestampType),
		cel.Function("unique",
			l.uniqueMemberOverload(cel.BoolType, l.uniqueScalar),
			l.uniqueMemberOverload(cel.IntType, l.uniqueScalar),
			l.uniqueMemberOverload(cel.UintType, l.uniqueScalar),
			l.uniqueMemberOverload(cel.DoubleType, l.uniqueScalar),
			l.uniqueMemberOverload(cel.StringType, l.uniqueScalar),
			l.uniqueMemberOverload(cel.BytesType, l.uniqueBytes),
		),
		cel.Function("getField",
			cel.Overload(
				"get_field_any_string",
				[]*cel.Type{cel.DynType, cel.StringType},
				cel.DynType,
				cel.FunctionBinding(func(values ...ref.Val) ref.Val {
					message, ok := values[0].(traits.Indexer)
					if !ok {
						return types.UnsupportedRefValConversionErr(values[0])
					}
					fieldName, ok := values[1].Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(values[1])
					}
					return message.Get(types.String(fieldName))
				}),
			),
		),
		cel.Function("isNan",
			cel.MemberOverload(
				"double_is_nan_bool",
				[]*cel.Type{cel.DoubleType},
				cel.BoolType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					num, ok := value.Value().(float64)
					if !ok {
						return types.UnsupportedRefValConversionErr(value)
					}
					return types.Bool(math.IsNaN(num))
				}),
			),
		),
		cel.Function("isInf",
			cel.MemberOverload(
				"double_is_inf_bool",
				[]*cel.Type{cel.DoubleType},
				cel.BoolType,
				cel.UnaryBinding(func(value ref.Val) ref.Val {
					num, ok := value.Value().(float64)
					if !ok {
						return types.UnsupportedRefValConversionErr(value)
					}
					return types.Bool(math.IsInf(num, 0))
				}),
			),
			cel.MemberOverload(
				"double_int_is_inf_bool",
				[]*cel.Type{cel.DoubleType, cel.IntType},
				cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					num, ok := lhs.Value().(float64)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					sign, ok := rhs.Value().(int64)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					return types.Bool(math.IsInf(num, int(sign)))
				}),
			),
		),
		cel.Function("isHostname",
			cel.MemberOverload(
				"string_is_hostname_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					host, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isHostname(host))
				}),
			),
		),
		cel.Function("isEmail",
			cel.MemberOverload(
				"string_is_email_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isEmail(addr))
				}),
			),
		),
		cel.Function("isIp",
			cel.MemberOverload(
				"string_is_ip_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isIP(addr, 0))
				}),
			),
			cel.MemberOverload(
				"string_int_is_ip_bool",
				[]*cel.Type{cel.StringType, cel.IntType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					addr, aok := args[0].Value().(string)
					vers, vok := args[1].Value().(int64)
					if !aok || !vok {
						return types.Bool(false)
					}
					return types.Bool(isIP(addr, vers))
				})),
		),
		cel.Function("isIpPrefix",
			cel.MemberOverload(
				"string_is_ip_prefix_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					prefix, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isIPPrefix(prefix, 0, false))
				})),
			cel.MemberOverload(
				"string_int_is_ip_prefix_bool",
				[]*cel.Type{cel.StringType, cel.IntType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					prefix, pok := args[0].Value().(string)
					vers, vok := args[1].Value().(int64)
					if !pok || !vok {
						return types.Bool(false)
					}
					return types.Bool(isIPPrefix(prefix, vers, false))
				})),
			cel.MemberOverload(
				"string_bool_is_ip_prefix_bool",
				[]*cel.Type{cel.StringType, cel.BoolType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					prefix, pok := args[0].Value().(string)
					strict, sok := args[1].Value().(bool)
					if !pok || !sok {
						return types.Bool(false)
					}
					return types.Bool(isIPPrefix(prefix, 0, strict))
				})),
			cel.MemberOverload(
				"string_int_bool_is_ip_prefix_bool",
				[]*cel.Type{cel.StringType, cel.IntType, cel.BoolType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					prefix, pok := args[0].Value().(string)
					vers, vok := args[1].Value().(int64)
					strict, sok := args[2].Value().(bool)
					if !pok || !vok || !sok {
						return types.Bool(false)
					}
					return types.Bool(isIPPrefix(prefix, vers, strict))
				})),
		),
		cel.Function("isUri",
			cel.MemberOverload(
				"string_is_uri_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isURI(s))
				}),
			),
		),
		cel.Function("isUriRef",
			cel.MemberOverload(
				"string_is_uri_ref_bool",
				[]*cel.Type{cel.StringType},
				cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					s, ok := args[0].Value().(string)
					if !ok {
						return types.Bool(false)
					}
					return types.Bool(isURIRef(s))
				}),
			),
		),
		cel.Function(overloads.Contains,
			cel.MemberOverload(
				overloads.ContainsString, []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					substr, ok := rhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(strings.Contains(value, substr))
				}),
			),
			cel.MemberOverload("contains_bytes", []*cel.Type{cel.BytesType, cel.BytesType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					substr, ok := rhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(bytes.Contains(value, substr))
				}),
			),
		),
		cel.Function(overloads.EndsWith,
			cel.MemberOverload(
				overloads.EndsWithString, []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					suffix, ok := rhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(strings.HasSuffix(value, suffix))
				}),
			),
			cel.MemberOverload("ends_with_bytes", []*cel.Type{cel.BytesType, cel.BytesType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					suffix, ok := rhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(bytes.HasSuffix(value, suffix))
				}),
			),
		),
		cel.Function(overloads.StartsWith,
			cel.MemberOverload(
				overloads.StartsWithString, []*cel.Type{cel.StringType, cel.StringType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					prefix, ok := rhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().(string)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(strings.HasPrefix(value, prefix))
				}),
			),
			cel.MemberOverload("starts_with_bytes", []*cel.Type{cel.BytesType, cel.BytesType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					prefix, ok := rhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(rhs)
					}
					value, ok := lhs.Value().([]byte)
					if !ok {
						return types.UnsupportedRefValConversionErr(lhs)
					}
					return types.Bool(bytes.HasPrefix(value, prefix))
				}),
			),
		),
		cel.Function("isHostAndPort",
			cel.MemberOverload("string_bool_is_host_and_port_bool",
				[]*cel.Type{cel.StringType, cel.BoolType}, cel.BoolType,
				cel.BinaryBinding(func(lhs ref.Val, rhs ref.Val) ref.Val {
					val, vok := lhs.Value().(string)
					portReq, pok := rhs.Value().(bool)
					if !vok || !pok {
						return types.Bool(false)
					}
					return types.Bool(isHostAndPort(val, portReq))
				}),
			),
		),
	}
}

func (l *library) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{
		cel.EvalOptions(
			cel.OptOptimize,
		),
	}
}

func (l *library) uniqueMemberOverload(itemType *cel.Type, overload func(lister traits.Lister) ref.Val) cel.FunctionOpt {
	return cel.MemberOverload(
		itemType.String()+"_unique_bool",
		[]*cel.Type{cel.ListType(itemType)},
		cel.BoolType,
		cel.UnaryBinding(func(value ref.Val) ref.Val {
			list, ok := value.(traits.Lister)
			if !ok {
				return types.UnsupportedRefValConversionErr(value)
			}
			return overload(list)
		}),
	)
}

func (l *library) uniqueScalar(list traits.Lister) ref.Val {
	size, ok := list.Size().Value().(int64)
	if !ok {
		return types.UnsupportedRefValConversionErr(list.Size().Value())
	}
	if size <= 1 {
		return types.Bool(true)
	}
	exist := l.uniqueScalarPool.Get().(map[ref.Val]struct{}) //nolint:errcheck // guaranteed to match
	defer func() {
		clear(exist)
		l.uniqueScalarPool.Put(exist)
	}()
	for i := range size {
		val := list.Get(types.Int(i))
		if _, ok := exist[val]; ok {
			return types.Bool(false)
		}
		exist[val] = struct{}{}
	}
	return types.Bool(true)
}

// uniqueBytes is an overload implementation of the unique function that
// compares bytes type CEL values. This function is used instead of uniqueScalar
// as the bytes ([]uint8) type is not hashable in Go; we cheat this by converting
// the value to a string.
func (l *library) uniqueBytes(list traits.Lister) ref.Val {
	size, ok := list.Size().Value().(int64)
	if !ok {
		return types.UnsupportedRefValConversionErr(list.Size().Value())
	}
	if size <= 1 {
		return types.Bool(true)
	}
	exist := l.uniqueBytesPool.Get().(map[string]struct{}) //nolint:errcheck // guaranteed to match
	defer func() {
		clear(exist)
		l.uniqueBytesPool.Put(exist)
	}()
	for i := range size {
		val := list.Get(types.Int(i)).Value()
		b, ok := val.([]byte)
		if !ok {
			return types.NewErr("expected bytes, got %v", val)
		}
		str := string(b)
		if _, ok := exist[str]; ok {
			return types.Bool(false)
		}
		exist[str] = struct{}{}
	}
	return types.Bool(true)
}

// isEmail reports whether val is an email address, for example "foo@example.com".
//
// Conforms to the definition for a valid email address from the HTML standard.
// Note that this standard willfully deviates from RFC 5322, which allows many
// unexpected forms of email addresses and will easily match a typographical
// error.
func isEmail(val string) bool {
	return emailRegex.MatchString(val)
}

// isURI reports whether val is a URI, for example "https://example.com/foo/bar?baz=quux#frag".
//
// URI is defined in the internet standard RFC 3986.
// Zone Identifiers in IPv6 address literals are supported (RFC 6874).
func isURI(val string) bool {
	uri := &uri{
		str: val,
	}
	return uri.uri()
}

// isURIRef reports whether val is a URI Reference - a URI such as
// "https://example.com/foo/bar?baz=quux#frag", or a Relative Reference such as
// "./foo/bar?query".
//
// URI, URI Reference, and Relative Reference are defined in the internet
// standard RFC 3986. Zone Identifiers in IPv6 address literals are supported
// (RFC 6874).
func isURIRef(val string) bool {
	uri := &uri{
		str: val,
	}
	return uri.uriReference()
}

// RequiredEnvOptions returns the options required to have expressions which
// rely on the provided descriptor.
func RequiredEnvOptions(fieldDesc protoreflect.FieldDescriptor) []cel.EnvOption {
	if fieldDesc.IsMap() {
		return append(
			RequiredEnvOptions(fieldDesc.MapKey()),
			RequiredEnvOptions(fieldDesc.MapValue())...,
		)
	}
	if fieldDesc.Kind() == protoreflect.MessageKind ||
		fieldDesc.Kind() == protoreflect.GroupKind {
		return []cel.EnvOption{
			cel.Types(dynamicpb.NewMessage(fieldDesc.Message())),
		}
	}
	return nil
}

type ipv4 struct {
	str       string
	index     int
	octets    []uint8
	prefixLen int64
}

// getBits returns the 32-bit value of an address parsed through address() or addressPrefix().
// Returns 0 if no address was parsed successfully.
func (i *ipv4) getBits() uint32 {
	if len(i.octets) != 4 {
		return 0
	}
	return (uint32(i.octets[0]) << 24) | (uint32(i.octets[1]) << 16) | (uint32(i.octets[2]) << 8) | uint32(i.octets[3])
}

// isPrefixOnly returns true if all bits to the right of the prefix-length are
// all zeros. Behavior is undefined if addressPrefix() has not been called before,
// or has returned false.
func (i *ipv4) isPrefixOnly() bool {
	bits := i.getBits()
	var mask uint32
	if i.prefixLen == 32 {
		mask = 0xffffffff
	} else {
		mask = ^(0xffffffff >> i.prefixLen)
	}
	masked := bits & mask
	return bits == masked
}

// address parses an IPv4 Address in dotted decimal notation.
func (i *ipv4) address() bool {
	return i.addressPart() && i.index == len(i.str)
}

// addressPrefix parses an IPv4 Address prefix.
func (i *ipv4) addressPrefix() bool {
	return i.addressPart() &&
		i.take('/') &&
		i.prefixLength() &&
		i.index == len(i.str)
}

// prefixLength parses the length of the prefix and stores the value in prefixLen.
func (i *ipv4) prefixLength() bool {
	start := i.index
	for i.digit() {
		if i.index-start > 2 {
			// max prefix-length is 32 bits, so anything more than 2 digits is invalid
			return false
		}
	}
	str := i.str[start:i.index]
	if len(str) == 0 {
		// too short
		return false
	}
	if len(str) > 1 && str[0] == '0' {
		// bad leading 0
		return false
	}
	value, err := strconv.ParseInt(str, 0, 32)
	if err != nil {
		// Error converting to number
		return false
	}
	if value > 32 {
		// max 32 bits
		return false
	}
	i.prefixLen = value
	return true
}

// addressPart parses str from the current index to determine an address part.
func (i *ipv4) addressPart() bool {
	start := i.index
	if i.decOctet() &&
		i.take('.') &&
		i.decOctet() &&
		i.take('.') &&
		i.decOctet() &&
		i.take('.') &&
		i.decOctet() {
		return true
	}
	i.index = start
	return false
}

// decOctet parses str from the current index to determine a decimal octet.
func (i *ipv4) decOctet() bool {
	start := i.index
	for i.digit() {
		if i.index-start > 3 {
			// decimal octet can be three characters at most
			return false
		}
	}
	str := i.str[start:i.index]
	if len(str) == 0 {
		// too short
		return false
	}
	if len(str) > 1 && str[0] == '0' {
		// bad leading 0
		return false
	}
	value, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return false
	}
	if value > 255 {
		return false
	}
	i.octets = append(i.octets, byte(value))
	return true
}

// digit parses the rule:
//
//	DIGIT = %x30-39  ; 0-9
func (i *ipv4) digit() bool {
	if i.index >= len(i.str) {
		return false
	}
	c := i.str[i.index]
	if '0' <= c && c <= '9' {
		i.index++
		return true
	}
	return false
}

// take reports whether the current position in the string is the character char.
func (i *ipv4) take(char byte) bool {
	if i.index >= len(i.str) {
		return false
	}
	if i.str[i.index] == char {
		i.index++
		return true
	}
	return false
}

// newIpv4 creates a new ipv4 based on str.
func newIpv4(str string) *ipv4 {
	return &ipv4{
		str: str,
	}
}

type ipv6 struct {
	str             string
	index           int
	pieces          []uint16 // 16-bit pieces found
	doubleColonAt   int      // number of 16-bit pieces found when double colon was found
	doubleColonSeen bool
	dottedRaw       string // dotted notation for right-most 32 bits
	dottedAddr      *ipv4  // dotted notation successfully parsed as IPv4
	zoneIDFound     bool
	prefixLen       int64 // 0 - 128
}

// getBits returns the 128-bit value of an address parsed through address() or
// addressPrefix(), as a 2-tuple of 64-bit values.
// Returns [0,0] if no address was parsed successfully.
func (i *ipv6) getBits() [2]uint64 {
	p16 := i.pieces
	// handle dotted decimal, add to p16
	if i.dottedAddr != nil {
		dotted32 := i.dottedAddr.getBits()      // right-most 32 bits
		p16 = append(p16, uint16(dotted32>>16)) //nolint:gosec // this is ok, we only want the high 16 bits
		p16 = append(p16, uint16(dotted32))     //nolint:gosec // this is ok, we only want the low 16 bits
	}
	// handle double colon, fill pieces with 0
	if i.doubleColonSeen {
		for len(p16) < 8 {
			// delete 0 entries at pos, insert a 0
			p16 = slices.Insert(p16, i.doubleColonAt, 0x00000000)
		}
	}
	if len(p16) != 8 {
		return [2]uint64{0, 0}
	}
	return [2]uint64{
		(uint64(p16[0]) << 48) | (uint64(p16[1]) << 32) | (uint64(p16[2]) << 16) | uint64(p16[3]),
		(uint64(p16[4]) << 48) | (uint64(p16[5]) << 32) | (uint64(p16[6]) << 16) | uint64(p16[7]),
	}
}

// isPrefixOnly returns true if all bits to the right of the prefix-length are
// all zeros. Behavior is undefined if addressPrefix() has not been called before,
// or has returned false.
func (i *ipv6) isPrefixOnly() bool {
	// For each 64-bit piece of the address, require that values to the right of the prefix are zero
	for idx, p64 := range i.getBits() {
		size := i.prefixLen - 64*int64(idx)
		var mask uint64
		if size >= 64 { //nolint:gocritic
			mask = 0xFFFFFFFFFFFFFFFF
		} else if size < 0 {
			mask = 0x0
		} else {
			mask = ^(0xFFFFFFFFFFFFFFFF >> size)
		}
		masked := p64 & mask
		if p64 != masked {
			return false
		}
	}
	return true
}

// address parses an IPv6 Address following RFC 4291, with optional zone id following RFC 4007.
func (i *ipv6) address() bool {
	return i.addressPart() && i.index == len(i.str)
}

// addressPrefix parses an IPv6 Address Prefix following RFC 4291. Zone id is not permitted.
func (i *ipv6) addressPrefix() bool {
	return i.addressPart() &&
		!i.zoneIDFound &&
		i.take('/') &&
		i.prefixLength() &&
		i.index == len(i.str)
}

// prefixLength parses the length of the prefix and stores the value in prefixLen.
func (i *ipv6) prefixLength() bool {
	start := i.index
	for i.digit() {
		if i.index-start > 3 {
			return false
		}
	}
	str := i.str[start:i.index]
	if len(str) == 0 {
		// too short
		return false
	}
	if len(str) > 1 && str[0] == '0' {
		// bad leading 0
		return false
	}
	value, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return false
	}
	if value > 128 {
		// max 128 bits
		return false
	}
	i.prefixLen = value
	return true
}

// addressPart stores the dotted notation for right-most 32 bits in dottedRaw / dottedAddr if found.
func (i *ipv6) addressPart() bool {
	for i.index < len(i.str) {
		// dotted notation for right-most 32 bits, e.g. 0:0:0:0:0:ffff:192.1.56.10
		if (i.doubleColonSeen || len(i.pieces) == 6) && i.dotted() {
			dotted := newIpv4(i.dottedRaw)
			if dotted.address() {
				i.dottedAddr = dotted
				return true
			}
			return false
		}
		ok, err := i.h16()
		if err != nil {
			return false
		}
		if ok {
			continue
		}
		if i.take(':') { //nolint:nestif
			if i.take(':') {
				if i.doubleColonSeen {
					return false
				}
				i.doubleColonSeen = true
				i.doubleColonAt = len(i.pieces)
				if i.take(':') {
					return false
				}
			} else if i.index == 1 || i.index == len(i.str) {
				// invalid - string cannot start or end on single colon
				return false
			}
			continue
		}
		if i.str[i.index] == '%' && !i.zoneID() {
			return false
		}
		break
	}
	if i.doubleColonSeen {
		return len(i.pieces) < 8
	}
	return len(i.pieces) == 8
}

// zoneID parses the rule from RFC 6874:
//
//	ZoneID = 1*( unreserved / pct-encoded )
//
// There is no definition for the character set allowed in the zone
// identifier. RFC 4007 permits basically any non-null string.
func (i *ipv6) zoneID() bool {
	start := i.index
	if i.take('%') {
		if len(i.str)-i.index > 0 {
			// permit any non-null string
			i.index = len(i.str)
			i.zoneIDFound = true
			return true
		}
	}
	i.index = start
	i.zoneIDFound = false
	return false
}

// dotted parses the rule:
//
//	1*3DIGIT "." 1*3DIGIT "." 1*3DIGIT "." 1*3DIGIT
//
// Stores match in dottedRaw.
func (i *ipv6) dotted() bool {
	start := i.index
	i.dottedRaw = ""
	for i.digit() || i.take('.') {
		// Consume '*( DIGIT "." )'
	}
	if i.index-start >= 7 {
		i.dottedRaw = i.str[start:i.index]
		return true
	}
	i.index = start
	return false
}

// h16 parses the rule:
//
//	h16 = 1*4HEXDIG
//
// If 1-4 hex digits are found, the parsed 16-bit unsigned integer is stored
// in pieces and true is returned.
// If 0 hex digits are found, returns false.
// If more than 4 hex digits are found, returns an error.
func (i *ipv6) h16() (bool, error) {
	start := i.index
	for i.hexdig() {
		if i.index-start > 4 {
			// too long
			// this is an error condition, it means we found a string of more than
			// four valid hex digits, which is invalid in ipv6 addresses.
			return false, errors.New("invalid hex")
		}
	}
	str := i.str[start:i.index]
	if len(str) == 0 {
		// too short, just return false
		// this is not an error condition, it just means we didn't find any
		// hex digits at the current position.
		return false, nil
	}

	value, err := strconv.ParseUint(str, 16, 16)
	if err != nil {
		// This is also an error condition. It means the parsed hextet we found
		// cannot be converted into a number
		return false, err
	}
	i.pieces = append(i.pieces, uint16(value))
	return true, nil
}

// hexdig parses the rule:
//
//	HEXDIG =  DIGIT / "A" / "B" / "C" / "D" / "E" / "F"
func (i *ipv6) hexdig() bool {
	if i.index >= len(i.str) {
		return false
	}
	c := i.str[i.index]
	if ('0' <= c && c <= '9') ||
		('a' <= c && c <= 'f') ||
		('A' <= c && c <= 'F') {
		i.index++
		return true
	}
	return false
}

// digit parses the rule:
//
//	DIGIT = %x30-39  ; 0-9
func (i *ipv6) digit() bool {
	if i.index >= len(i.str) {
		return false
	}
	c := i.str[i.index]
	if '0' <= c && c <= '9' {
		i.index++
		return true
	}
	return false
}

// take reports whether the current position in the string is the character char.
func (i *ipv6) take(char byte) bool {
	if i.index >= len(i.str) {
		return false
	}
	if i.str[i.index] == char {
		i.index++
		return true
	}
	return false
}

// newIpv6 creates a new ipv6 based on str.
func newIpv6(str string) *ipv6 {
	return &ipv6{
		str:           str,
		doubleColonAt: -1,
	}
}

// isIP returns true if the string is an IPv4 or IPv6 address, optionally limited to
// a specific version.
//
// Version 0 means either 4 or 6. Passing a version other than 0, 4, or 6 always
// returns false.
//
// IPv4 addresses are expected in the dotted decimal format, for example "192.168.5.21".
// IPv6 addresses are expected in their text representation, for example "::1",
// or "2001:0DB8:ABCD:0012::0".
//
// Both formats are well-defined in the internet standard RFC 3986. Zone
// identifiers for IPv6 addresses (for example "fe80::a%en1") are supported.
func isIP(str string, version int64) bool {
	if version == 6 {
		return newIpv6(str).address()
	}
	if version == 4 {
		return newIpv4(str).address()
	}
	if version == 0 {
		return newIpv4(str).address() || newIpv6(str).address()
	}
	return false
}

// isIPPrefix returns true if the string is a valid IP with prefix length, optionally
// limited to a specific version (v4 or v6), and optionally requiring the host
// portion to be all zeros.
//
// An address prefix divides an IP address into a network portion, and a host
// portion. The prefix length specifies how many bits the network portion has.
// For example, the IPv6 prefix "2001:db8:abcd:0012::0/64" designates the
// left-most 64 bits as the network prefix. The range of the network is 2**64
// addresses, from 2001:db8:abcd:0012::0 to 2001:db8:abcd:0012:ffff:ffff:ffff:ffff.
//
// An address prefix may include a specific host address, for example
// "2001:db8:abcd:0012::1f/64". With strict = true, this is not permitted. The
// host portion must be all zeros, as in "2001:db8:abcd:0012::0/64".
//
// The same principle applies to IPv4 addresses. "192.168.1.0/24" designates
// the first 24 bits of the 32-bit IPv4 as the network prefix.
func isIPPrefix(
	str string,
	version int64,
	strict bool,
) bool {
	if version == 6 {
		ip := newIpv6(str)
		return ip.addressPrefix() && (!strict || ip.isPrefixOnly())
	}
	if version == 4 {
		ip := newIpv4(str)
		return ip.addressPrefix() && (!strict || ip.isPrefixOnly())
	}
	if version == 0 {
		return isIPPrefix(str, 6, strict) || isIPPrefix(str, 4, strict)
	}
	return false
}

// isHostname returns true if the string is a valid hostname, for example "foo.example.com".
//
// A valid hostname follows the rules below:
//   - The name consists of one or more labels, separated by a dot (".").
//   - Each label can be 1 to 63 alphanumeric characters.
//   - A label can contain hyphens ("-"), but must not start or end with a hyphen.
//   - The right-most label must not be digits only.
//   - The name can have a trailing dot, for example "foo.example.com.".
//   - The name can be 253 characters at most, excluding the optional trailing dot.
func isHostname(val string) bool {
	if len(val) > 253 {
		return false
	}
	var str string
	if strings.HasSuffix(val, ".") {
		str = val[0 : len(val)-1]
	} else {
		str = val
	}

	allDigits := false

	// split hostname on '.' and validate each part
	for part := range strings.SplitSeq(str, ".") {
		allDigits = true
		// if part is empty, longer than 63 chars, or starts/ends with '-', it is invalid
		l := len(part)
		if l == 0 || l > 63 || strings.HasPrefix(part, "-") || strings.HasSuffix(part, "-") {
			return false
		}
		// for each character in part
		for i := range len(part) {
			c := part[i]
			// if the character is not a-z, A-Z, 0-9, or '-', it is invalid
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
			allDigits = allDigits && c >= '0' && c <= '9'
		}
	}
	// the last part cannot be all numbers
	return !allDigits
}

// isHostAndPort returns true if the string is a valid host/port pair, for example
// "example.com:8080".
//
// If the argument portRequired is true, the port is required. If the argument
// is false, the port is optional.
//
// The host can be one of:
//   - An IPv4 address in dotted decimal format, for example "192.168.0.1".
//   - An IPv6 address enclosed in square brackets, for example "[::1]".
//   - A hostname, for example "example.com".
//
// The port is separated by a colon. It must be non-empty, with a decimal number
// in the range of 0-65535, inclusive.
func isHostAndPort(str string, portRequired bool) bool {
	if len(str) == 0 {
		return false
	}
	splitIdx := strings.LastIndex(str, ":")
	if str[0] == '[' {
		end := strings.LastIndex(str, "]")
		switch end + 1 {
		case len(str): // no port
			return !portRequired && isIP(str[1:end], 6)
		case splitIdx: // port
			return isIP(str[1:end], 6) && isPort(str[splitIdx+1:])
		default: // malformed
			return false
		}
	}
	if splitIdx < 0 {
		return !portRequired && (isHostname(str) || isIP(str, 4))
	}
	host := str[0:splitIdx]
	port := str[splitIdx+1:]
	return (isHostname(host) || isIP(host, 4)) && isPort(port)
}

// isPort returns true if the string is a valid port for isHostAndPort.
func isPort(str string) bool {
	if len(str) == 0 {
		return false
	}
	for i := range len(str) {
		c := str[i]
		if '0' <= c && c <= '9' {
			continue
		}
		return false
	}
	if len(str) > 1 && str[0] == '0' {
		// bad leading 0
		return false
	}
	val, err := strconv.ParseUint(str, 0, 32)
	if err != nil {
		return false
	}
	return val <= 65535
}

type uri struct {
	str             string
	index           int
	pctEncodedFound bool
}

// uri parses the rule:
//
//	URI = scheme ":" hier-part [ "?" query ] [ "#" fragment ]
func (u *uri) uri() bool {
	start := u.index
	if !(u.scheme() && u.take(':') && u.hierPart()) {
		u.index = start
		return false
	}
	if u.take('?') && !u.query() {
		return false
	}
	if u.take('#') && !u.fragment() {
		return false
	}
	if u.index != len(u.str) {
		u.index = start
		return false
	}
	return true
}

// uriReference parses the rule:
//
//	URI-reference = URI / relative-ref.
func (u *uri) uriReference() bool {
	return u.uri() || u.relativeRef()
}

// hierPart parses the rule:
//
//	hier-part = "//" authority path-abempty.
//			    / path-absolute
//		        / path-rootless
//		        / path-empty.
func (u *uri) hierPart() bool {
	start := u.index
	if u.takeDoubleSlash() &&
		u.authority() &&
		u.pathAbempty() {
		return true
	}
	u.index = start
	return u.pathAbsolute() || u.pathRootless() || u.pathEmpty()
}

// relativeRef parses the rule:
//
//	relative-ref = relative-part [ "?" query ] [ "#" fragment ].
func (u *uri) relativeRef() bool {
	start := u.index
	if !u.relativePart() {
		return false
	}
	if u.take('?') && !u.query() {
		u.index = start
		return false
	}
	if u.take('#') && !u.fragment() {
		u.index = start
		return false
	}
	if u.index != len(u.str) {
		u.index = start
		return false
	}
	return true
}

// relativePart parses the rule:
//
//	relative-part = "//" authority path-abempty
//		          / path-absolute
//		          / path-noscheme
//		          / path-empty
func (u *uri) relativePart() bool {
	start := u.index
	if u.takeDoubleSlash() &&
		u.authority() &&
		u.pathAbempty() {
		return true
	}
	u.index = start
	return u.pathAbsolute() || u.pathNoscheme() || u.pathEmpty()
}

// scheme parses the rule:
//
//	scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
//
// Terminated by ":".
func (u *uri) scheme() bool {
	start := u.index
	if u.alpha() {
		for u.alpha() || u.digit() || u.take('+') || u.take('-') || u.take('.') {
			// Consume '*( ALPHA / DIGIT / "+" / "-" / "." )'
		}
		if u.peek(':') {
			return true
		}
	}
	u.index = start
	return false
}

// authority parses the rule:
//
//	authority = [ userinfo "@" ] host [ ":" port ]
//
// Lead by double slash ("") and terminated by "/", "?", "#", or end of URI.
func (u *uri) authority() bool {
	start := u.index
	if u.userinfo() {
		if !u.take('@') {
			u.index = start
			return false
		}
	}
	if !u.host() {
		u.index = start
		return false
	}
	if u.take(':') {
		if !u.port() {
			u.index = start
			return false
		}
	}
	if !u.isAuthorityEnd() {
		u.index = start
		return false
	}
	return true
}

// isAuthorityEnd reports whether the current position is the end of the authority.
//
//	The authority component [...] is terminated by the next slash ("/"),
//	question mark ("?"), or number sign ("#") character, or by the
//	end of the URI.
func (u *uri) isAuthorityEnd() bool {
	return u.index >= len(u.str) ||
		u.str[u.index] == '?' ||
		u.str[u.index] == '#' ||
		u.str[u.index] == '/'
}

// userinfo parses the rule:
//
//	userinfo = *( unreserved / pct-encoded / sub-delims / ":" )
//
// Terminated by "@" in authority.
func (u *uri) userinfo() bool {
	start := u.index
	for {
		if u.unreserved() ||
			u.pctEncoded() ||
			u.subDelims() ||
			u.take(':') {
			continue
		}
		if u.index < len(u.str) {
			if u.str[u.index] == '@' {
				return true
			}
		}
		u.index = start
		return false
	}
}

// checkHostPctEncoded verifies that str is correctly percent-encoded.
func (u *uri) checkHostPctEncoded(str string) bool {
	unhex := func(char byte) byte {
		switch {
		case '0' <= char && char <= '9':
			return char - '0'
		case 'a' <= char && char <= 'f':
			return char - 'a' + 10
		case 'A' <= char && char <= 'F':
			return char - 'A' + 10
		}
		return 0
	}
	escaped := make([]byte, 0, len(str))
	for i := 0; i < len(str); {
		switch str[i] {
		case '%':
			escaped = append(escaped, unhex(str[i+1])<<4|unhex(str[i+2]))
			i += 3
		default:
			escaped = append(escaped, str[i])
			i++
		}
	}
	return utf8.Valid(escaped)
}

// host parses the rule:
//
//	host = IP-literal / IPv4address / reg-name
func (u *uri) host() bool {
	start := u.index
	u.pctEncodedFound = false
	// Note: IPv4address is a subset of reg-name
	if (u.peek('[') && u.ipLiteral()) || u.regName() {
		if u.pctEncodedFound {
			rawHost := u.str[start:u.index]
			// RFC 3986:
			// > URI producing applications must not use percent-encoding in host
			// > unless it is used to represent a UTF-8 character sequence.
			if !u.checkHostPctEncoded(rawHost) {
				return false
			}
		}
		return true
	}
	return false
}

// port parses the rule:
//
//	port = *DIGIT
//
// Terminated by end of authority.
func (u *uri) port() bool {
	start := u.index
	for u.digit() {
		// Consume '*DIGIT'
	}
	if u.isAuthorityEnd() {
		return true
	}
	u.index = start
	return false
}

// ipLiteral parses the rule from RFC 6874:
//
//	IP-literal = "[" ( IPv6address / IPv6addrz / IPvFuture  ) "]"
func (u *uri) ipLiteral() bool {
	start := u.index
	if u.take('[') {
		currIdx := u.index
		if u.ipv6Address() && u.take(']') {
			return true
		}
		u.index = currIdx
		if u.ipv6addrz() && u.take(']') {
			return true
		}
		u.index = currIdx
		if u.ipvFuture() && u.take(']') {
			return true
		}
	}
	u.index = start
	return false
}

// ipv6Address parses the rule "IPv6address".
//
// Relies on the implementation of isIP.
func (u *uri) ipv6Address() bool {
	start := u.index
	for u.hexdig() || u.take(':') {
		// Consume '*( HEXDIG / ":" )'
	}
	if isIP(u.str[start:u.index], 6) {
		return true
	}
	u.index = start
	return false
}

// ipv6addrz parses the rule from RFC 6874:
//
//	IPv6addrz = IPv6address "%25" ZoneID
func (u *uri) ipv6addrz() bool {
	start := u.index
	if u.ipv6Address() &&
		u.take('%') &&
		u.take('2') &&
		u.take('5') &&
		u.zoneID() {
		return true
	}
	u.index = start
	return false
}

// zoneID parses the rule from RFC 6874:
//
//	ZoneID = 1*( unreserved / pct-encoded )
func (u *uri) zoneID() bool {
	start := u.index
	for u.unreserved() || u.pctEncoded() {
		// Consume '*( unreserved / pct-encoded )'
	}
	if u.index-start > 0 {
		return true
	}
	u.index = start
	return false
}

// ipvFuture parses the rule:
//
//	IPvFuture  = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
func (u *uri) ipvFuture() bool {
	start := u.index
	if u.take('v') && u.hexdig() {
		for u.hexdig() {
			// Consume '*HEXDIG'
		}
		if u.take('.') {
			counter := 0
			for u.unreserved() || u.subDelims() || u.take(':') {
				counter++
			}
			if counter >= 1 {
				return true
			}
		}
	}
	u.index = start
	return false
}

// regName parses the rule:
//
//	reg-name = *( unreserved / pct-encoded / sub-delims )
//
// Terminates on start of port (":") or end of authority.
func (u *uri) regName() bool {
	start := u.index
	for {
		if u.unreserved() || u.pctEncoded() || u.subDelims() {
			continue
		}
		if u.isAuthorityEnd() {
			// End of authority
			return true
		}
		if u.str[u.index] == ':' {
			return true
		}
		u.index = start
		return false
	}
}

// isPathEnd reports whether the current position is the end of the path.
//
//	The path is terminated by the first question mark ("?") or
//	number sign ("#") character, or by the end of the URI.
func (u *uri) isPathEnd() bool {
	return u.index >= len(u.str) || u.str[u.index] == '?' || u.str[u.index] == '#'
}

// pathAbempty parses the rule:
//
//	path-abempty = *( "/" segment )
//
// Terminated by end of path: "?", "#", or end of URI.
func (u *uri) pathAbempty() bool {
	start := u.index
	for u.take('/') && u.segment() {
		// Consume '*( "/" segment )'
	}
	if u.isPathEnd() {
		return true
	}
	u.index = start
	return false
}

// pathAbsolute parses the rule:
//
//	path-absolute = "/" [ segment-nz *( "/" segment ) ]
//
// Terminated by end of path: "?", "#", or end of URI.
func (u *uri) pathAbsolute() bool {
	start := u.index
	if u.take('/') {
		if u.segmentNz() {
			for u.take('/') && u.segment() {
				// Consume '*( "/" segment )'
			}
		}
		if u.isPathEnd() {
			return true
		}
	}
	u.index = start
	return false
}

// pathNoscheme parses the rule:
//
//	path-noscheme = segment-nz-nc *( "/" segment )
//
// Terminated by end of path: "?", "#", or end of URI.
func (u *uri) pathNoscheme() bool {
	start := u.index
	if u.segmentNzNc() {
		for u.take('/') && u.segment() {
			// Consume *( "/" segment )
		}
		if u.isPathEnd() {
			return true
		}
	}
	u.index = start
	return false
}

// pathRootless parses the rule:
//
//	path-rootless = segment-nz *( "/" segment )
//
// Terminated by end of path: "?", "#", or end of URI.
func (u *uri) pathRootless() bool {
	start := u.index
	if u.segmentNz() {
		for u.take('/') && u.segment() {
			// Consume *( '/' segment )
		}
		if u.isPathEnd() {
			return true
		}
	}
	u.index = start
	return false
}

// pathEmpty parses the rule:
//
//	path-empty = 0<pchar>
//
// Terminated by end of path: "?", "#", or end of URI.
func (u *uri) pathEmpty() bool {
	return u.isPathEnd()
}

// segment parses the rule:
//
//	segment = *pchar
func (u *uri) segment() bool {
	for u.pchar() {
		// Consume '*pchar'
	}
	return true
}

// segmentNz parses the rule:
//
//	segment-nz = 1*pchar
func (u *uri) segmentNz() bool {
	start := u.index
	if u.pchar() {
		return u.segment()
	}
	u.index = start
	return false
}

// segmentNzNc parses the rule:
//
//	segment-nz-nc = 1*( unreserved / pct-encoded / sub-delims / "@" )
//	             ; non-zero-length segment without any colon ":"
func (u *uri) segmentNzNc() bool {
	start := u.index
	for u.unreserved() || u.pctEncoded() || u.subDelims() || u.take('@') {
		// Consume '*( unreserved / pct-encoded / sub-delims / "@" )'
	}
	if u.index-start > 0 {
		return true
	}
	u.index = start
	return false
}

// pchar parses the rule:
//
//	pchar = unreserved / pct-encoded / sub-delims / ":" / "@"
func (u *uri) pchar() bool {
	return u.unreserved() ||
		u.pctEncoded() ||
		u.subDelims() ||
		u.take(':') ||
		u.take('@')
}

// query parses the rule:
//
//	query = *( pchar / "/" / "?" )
//
// Terminated by "#" or end of URI.
func (u *uri) query() bool {
	start := u.index
	for {
		if u.pchar() || u.take('/') || u.take('?') {
			continue
		}
		if u.index == len(u.str) || u.str[u.index] == '#' {
			return true
		}
		u.index = start
		return false
	}
}

// fragment parses the rule:
//
//	fragment = *( pchar / "/" / "?" )
//
// Terminated by end of URI.
func (u *uri) fragment() bool {
	start := u.index
	for {
		if u.pchar() || u.take('/') || u.take('?') {
			continue
		}
		if u.index == len(u.str) {
			return true
		}
		u.index = start
		return false
	}
}

// pctEncoded parses the rule:
//
//	pct-encoded = "%"+HEXDIG+HEXDIG
//
// Sets `pctEncodedFound` to true if a valid triplet was found.
func (u *uri) pctEncoded() bool {
	start := u.index
	if u.take('%') && u.hexdig() && u.hexdig() {
		u.pctEncodedFound = true
		return true
	}
	u.index = start
	return false
}

// unreserved parses the rule:
//
//	unreserved = ALPHA / DIGIT / "-" / "." / "_" / "~"
func (u *uri) unreserved() bool {
	return u.alpha() ||
		u.digit() ||
		u.take('-') ||
		u.take('_') ||
		u.take('.') ||
		u.take('~')
}

// subDelims parses the rule:
//
//	sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
//	            / "*" / "+" / "," / ";" / "="
func (u *uri) subDelims() bool {
	return u.take('!') ||
		u.take('$') ||
		u.take('&') ||
		u.take('\'') ||
		u.take('(') ||
		u.take(')') ||
		u.take('*') ||
		u.take('+') ||
		u.take(',') ||
		u.take(';') ||
		u.take('=')
}

// alpha parses the rule:
//
//	ALPHA =  %x41-5A / %x61-7A ; A-Z / a-z
func (u *uri) alpha() bool {
	if u.index >= len(u.str) {
		return false
	}
	c := u.str[u.index]
	if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') {
		u.index++
		return true
	}
	return false
}

// digit parses the rule:
//
//	DIGIT = %x30-39  ; 0-9
func (u *uri) digit() bool {
	if u.index >= len(u.str) {
		return false
	}
	c := u.str[u.index]
	if '0' <= c && c <= '9' {
		u.index++
		return true
	}
	return false
}

// hexdig parses the rule:
//
//	HEXDIG =  DIGIT / "A" / "B" / "C" / "D" / "E" / "F"
func (u *uri) hexdig() bool {
	if u.index >= len(u.str) {
		return false
	}
	c := u.str[u.index]
	if ('0' <= c && c <= '9') ||
		('a' <= c && c <= 'f') ||
		('A' <= c && c <= 'F') {
		u.index++
		return true
	}
	return false
}

// take reports whether the current position in the string is the character char.
func (u *uri) take(char byte) bool {
	if u.index >= len(u.str) {
		return false
	}
	if u.str[u.index] == char {
		u.index++
		return true
	}
	return false
}

func (u *uri) takeDoubleSlash() bool {
	first := u.take('/')
	return first && u.take('/')
}

func (u *uri) peek(char byte) bool {
	return u.index < len(u.str) && u.str[u.index] == char
}

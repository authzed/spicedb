package types

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/stretchr/testify/require"
)

func TestConversion(t *testing.T) {
	tcs := []struct {
		name          string
		vtype         VariableType
		inputValue    any
		expectedValue any
		expectedErr   string
	}{
		{
			name:          "bool to bool",
			vtype:         BooleanType,
			inputValue:    true,
			expectedValue: true,
			expectedErr:   "",
		},
		{
			name:          "string to bool",
			vtype:         BooleanType,
			inputValue:    "hiya",
			expectedValue: nil,
			expectedErr:   "for bool: a bool value is required, but found string `hiya`",
		},
		{
			name:          "number to string",
			vtype:         StringType,
			inputValue:    42,
			expectedValue: nil,
			expectedErr:   "for string: a string value is required, but found int `42`",
		},
		{
			name:          "float to float",
			vtype:         DoubleType,
			inputValue:    42.0,
			expectedValue: 42.0,
			expectedErr:   "",
		},
		{
			name:          "string to float",
			vtype:         DoubleType,
			inputValue:    "42.0",
			expectedValue: 42.0,
			expectedErr:   "",
		},
		{
			name:          "float to uint",
			vtype:         UIntType,
			inputValue:    42.0,
			expectedValue: uint64(42),
			expectedErr:   "",
		},
		{
			name:          "invalid float to uint",
			vtype:         UIntType,
			inputValue:    42.1,
			expectedValue: uint64(42),
			expectedErr:   "for uint: a uint value is required, but found numeric value `42.1`",
		},
		{
			name:          "negative float to uint",
			vtype:         UIntType,
			inputValue:    -42.0,
			expectedValue: nil,
			expectedErr:   "for uint: a uint value is required, but found int64 value `-42`",
		},
		{
			name:          "float to int",
			vtype:         IntType,
			inputValue:    42.0,
			expectedValue: int64(42),
			expectedErr:   "",
		},
		{
			name:          "string to int",
			vtype:         IntType,
			inputValue:    "42",
			expectedValue: int64(42),
			expectedErr:   "",
		},
		{
			name:          "string to uint",
			vtype:         UIntType,
			inputValue:    "42",
			expectedValue: uint64(42),
			expectedErr:   "",
		},
		{
			name:          "invalid float to int",
			vtype:         IntType,
			inputValue:    42.1,
			expectedValue: int64(42),
			expectedErr:   "for int: a int value is required, but found numeric value `42.1`",
		},
		{
			name:          "negative float to int",
			vtype:         IntType,
			inputValue:    -42.0,
			expectedValue: int64(-42),
			expectedErr:   "",
		},
		{
			name:          "valid list<uint>",
			vtype:         MustListType(UIntType),
			inputValue:    []any{uint64(1)},
			expectedValue: []any{uint64(1)},
			expectedErr:   "",
		},
		{
			name:          "valid list<uint> with conversion",
			vtype:         MustListType(UIntType),
			inputValue:    []any{42.0},
			expectedValue: []any{uint64(42)},
			expectedErr:   "",
		},
		{
			name:          "valid list<uint> with string conversion",
			vtype:         MustListType(UIntType),
			inputValue:    []any{"42"},
			expectedValue: []any{uint64(42)},
			expectedErr:   "",
		},
		{
			name:          "valid list<uint> with string conversion of max int",
			vtype:         MustListType(UIntType),
			inputValue:    []any{"9223372036854775807"},
			expectedValue: []any{uint64(9223372036854775807)},
			expectedErr:   "",
		},
		{
			name:          "map with invalid value type",
			vtype:         MustMapType(UIntType),
			inputValue:    map[string]any{"foo": "a1"},
			expectedValue: nil,
			expectedErr:   "for map<uint>: found an invalid value for key `foo`: for uint: a uint64 value is required, but found invalid string value `a1`",
		},
		{
			name:          "map with invalid value type in different key",
			vtype:         MustMapType(UIntType),
			inputValue:    map[string]any{"foo": "1", "bar": "a1"},
			expectedValue: nil,
			expectedErr:   "for map<uint>: found an invalid value for key `bar`: for uint: a uint64 value is required, but found invalid string value `a1`",
		},
		{
			name:          "map with valid converted value type",
			vtype:         MustMapType(UIntType),
			inputValue:    map[string]any{"foo": "1"},
			expectedValue: map[string]any{"foo": uint64(1)},
			expectedErr:   "",
		},
		{
			name:          "map with invalid nested value in list",
			vtype:         MustMapType(MustListType(UIntType)),
			inputValue:    map[string]any{"foo": []any{"-1"}},
			expectedValue: nil,
			expectedErr:   "for map<list<uint>>: found an invalid value for key `foo`: for list<uint>: found an invalid value for item at index 0: for uint: a uint value is required, but found int64 value `-1`",
		},
		{
			name:          "map with valid nested value in list",
			vtype:         MustMapType(MustListType(UIntType)),
			inputValue:    map[string]any{"foo": []any{"1"}},
			expectedValue: map[string]any{"foo": []any{uint64(1)}},
			expectedErr:   "",
		},
		{
			name:          "map with invalid nested map value",
			vtype:         MustMapType(MustMapType(StringType)),
			inputValue:    map[string]any{"foo": map[string]any{"bar": 42.0}},
			expectedValue: nil,
			expectedErr:   "for map<map<string>>: found an invalid value for key `foo`: for map<string>: found an invalid value for key `bar`: for string: a string value is required, but found float64 `42`",
		},
		{
			name:          "map with valid nested map value",
			vtype:         MustMapType(MustMapType(StringType)),
			inputValue:    map[string]any{"foo": map[string]any{"bar": "hiya"}},
			expectedValue: map[string]any{"foo": map[string]any{"bar": "hiya"}},
			expectedErr:   "",
		},
		{
			name:  "valid bytes",
			vtype: BytesType,
			inputValue: func() string {
				v, _ := structpb.NewValue([]byte{1, 2, 3, 42})
				return v.GetStringValue()
			}(),
			expectedValue: []byte{1, 2, 3, 42},
			expectedErr:   "",
		},
		{
			name:          "invalid type for bytes",
			vtype:         BytesType,
			inputValue:    42.0,
			expectedValue: nil,
			expectedErr:   "for bytes: bytes requires a base64 unicode string, found: float64 `42`",
		},
		{
			name:          "invalid bytes",
			vtype:         BytesType,
			inputValue:    "testing123",
			expectedValue: nil,
			expectedErr:   "for bytes: bytes requires a base64 encoded string: illegal base64 data at input byte 8",
		},
		{
			name:          "valid duration",
			vtype:         DurationType,
			inputValue:    "10h4m",
			expectedValue: 604 * time.Minute,
			expectedErr:   "",
		},
		{
			name:          "invalid duration",
			vtype:         DurationType,
			inputValue:    "10hm",
			expectedValue: nil,
			expectedErr:   "for duration: could not parse duration string `10hm`: time: unknown unit \"hm\" in duration \"10hm\"",
		},
		{
			name:          "valid timestamp",
			vtype:         TimestampType,
			inputValue:    "2023-01-22T01:13:00Z",
			expectedValue: time.Date(2023, time.January, 22, 1, 13, 0, 0, time.UTC),
			expectedErr:   "",
		},
		{
			name:          "invalid timestamp",
			vtype:         TimestampType,
			inputValue:    "2023-0122",
			expectedValue: nil,
			expectedErr:   "for timestamp: could not parse RFC 3339 formatted timestamp string `2023-0122`: parsing time \"2023-0122\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"22\" as \"-\"",
		},
		{
			name:          "invalid timestamp value",
			vtype:         TimestampType,
			inputValue:    42.0,
			expectedValue: nil,
			expectedErr:   "for timestamp: timestamps requires a RFC 3339 formatted timestamp string, found: float64 `42`",
		},
		{
			name:          "valid ipaddress",
			vtype:         IPAddressType,
			inputValue:    "1.2.3.4",
			expectedValue: MustParseIPAddress("1.2.3.4"),
			expectedErr:   "",
		},
		{
			name:          "invalid ipaddress",
			vtype:         IPAddressType,
			inputValue:    "1...23.4",
			expectedValue: nil,
			expectedErr:   "for ipaddress: could not parse ip address string `1...23.4`: ParseAddr(\"1...23.4\"): IPv4 field must have at least one digit (at \"..23.4\")",
		},
		{
			name:          "invalid ipaddress type",
			vtype:         IPAddressType,
			inputValue:    42.0,
			expectedValue: nil,
			expectedErr:   "for ipaddress: ipaddress requires an ipaddress string, found: float64 `42`",
		},
		{
			name:          "invalid ipaddress in list",
			vtype:         MustListType(IPAddressType),
			inputValue:    []any{"1...23.4"},
			expectedValue: nil,
			expectedErr:   "for list<ipaddress>: found an invalid value for item at index 0: for ipaddress: could not parse ip address string `1...23.4`: ParseAddr(\"1...23.4\"): IPv4 field must have at least one digit (at \"..23.4\")",
		},
		{
			name:          "ipaddress in list",
			vtype:         MustListType(IPAddressType),
			inputValue:    []any{"1.2.3.4", "4.5.6.7"},
			expectedValue: []any{MustParseIPAddress("1.2.3.4"), MustParseIPAddress("4.5.6.7")},
			expectedErr:   "",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.vtype.ConvertValue(tc.inputValue)
			if err != nil {
				require.Equal(t, tc.expectedErr, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedValue, result)
			}
		})
	}
}

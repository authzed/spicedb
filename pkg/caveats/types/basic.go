package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types"
	"github.com/authzed/cel-go/common/types/ref"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// RegisterBasicTypes registers the basic types with the given keyword, CEL type, and converter.
func RegisterBasicTypes(sts *StandardTypeSet) error {
	ts := sts.TypeSet

	// Register "any" type
	anyType, err := RegisterBasicType(ts, "any", cel.DynType, func(value any) (any, error) { return value, nil })
	if err != nil {
		return err
	}
	sts.AnyType = anyType

	// Register "bool" type
	boolType, err := RegisterBasicType(ts, "bool", cel.BoolType, requireType[bool])
	if err != nil {
		return err
	}
	sts.BooleanType = boolType

	// Register "string" type
	stringType, err := RegisterBasicType(ts, "string", cel.StringType, requireType[string])
	if err != nil {
		return err
	}
	sts.StringType = stringType

	// Register "int" type
	intType, err := RegisterBasicType(ts, "int", cel.IntType, convertNumericType[int64])
	if err != nil {
		return err
	}
	sts.IntType = intType

	// Register "uint" type
	uintType, err := RegisterBasicType(ts, "uint", cel.IntType, convertNumericType[uint64])
	if err != nil {
		return err
	}
	sts.UIntType = uintType

	// Register "double" type
	doubleType, err := RegisterBasicType(ts, "double", cel.DoubleType, convertNumericType[float64])
	if err != nil {
		return err
	}
	sts.DoubleType = doubleType

	// Register "bytes" type
	bytesType, err := RegisterBasicType(ts, "bytes", cel.BytesType, func(value any) (any, error) {
		vle, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("bytes requires a base64 unicode string, found: %T `%v`", value, value)
		}

		decoded, err := base64.StdEncoding.DecodeString(vle)
		if err != nil {
			return nil, fmt.Errorf("bytes requires a base64 encoded string: %w", err)
		}

		return decoded, nil
	})
	if err != nil {
		return err
	}
	sts.BytesType = bytesType

	// Register "duration" type
	durationType, err := RegisterBasicType(ts, "duration", cel.DurationType, func(value any) (any, error) {
		vle, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("durations requires a duration string, found: %T", value)
		}

		d, err := time.ParseDuration(vle)
		if err != nil {
			return nil, fmt.Errorf("could not parse duration string `%s`: %w", vle, err)
		}

		return d, nil
	})
	if err != nil {
		return err
	}
	sts.DurationType = durationType

	// Register "timestamp" type
	timestampType, err := RegisterBasicType(ts, "timestamp", cel.TimestampType, func(value any) (any, error) {
		vle, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("timestamps requires a RFC 3339 formatted timestamp string, found: %T `%v`", value, value)
		}

		d, err := time.Parse(time.RFC3339, vle)
		if err != nil {
			return nil, fmt.Errorf("could not parse RFC 3339 formatted timestamp string `%s`: %w", vle, err)
		}

		return d, nil
	})
	if err != nil {
		return err
	}
	sts.TimestampType = timestampType

	listTypeBuilder, err := RegisterGenericType(ts, "list", 1,
		func(childTypes []VariableType) VariableType {
			return VariableType{
				localName:  "list",
				celType:    cel.ListType(childTypes[0].celType),
				childTypes: childTypes,
				converter: func(value any) (any, error) {
					vle, ok := value.([]any)
					if !ok {
						return nil, fmt.Errorf("list requires a list, found: %T", value)
					}

					converted := make([]any, 0, len(vle))
					for index, item := range vle {
						convertedItem, err := childTypes[0].ConvertValue(item)
						if err != nil {
							return nil, fmt.Errorf("found an invalid value for item at index %d: %w", index, err)
						}
						converted = append(converted, convertedItem)
					}

					return converted, nil
				},
			}
		})
	if err != nil {
		return err
	}
	sts.listTypeBuilder = listTypeBuilder

	mapTypeBuilder, err := RegisterGenericType(ts, "map", 1,
		func(childTypes []VariableType) VariableType {
			return VariableType{
				localName:  "map",
				celType:    cel.MapType(cel.StringType, childTypes[0].celType),
				childTypes: childTypes,
				converter: func(value any) (any, error) {
					vle, ok := value.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("map requires a map, found: %T", value)
					}

					converted := make(map[string]any, len(vle))
					for key, item := range vle {
						convertedItem, err := childTypes[0].ConvertValue(item)
						if err != nil {
							return nil, fmt.Errorf("found an invalid value for key `%s`: %w", key, err)
						}

						converted[key] = convertedItem
					}

					return converted, nil
				},
			}
		},
	)
	if err != nil {
		return err
	}
	sts.mapTypeBuilder = mapTypeBuilder

	if err := RegisterMethodOnDefinedType(ts, cel.MapType(cel.StringType, cel.DynType),
		"isSubtreeOf",
		[]*cel.Type{cel.MapType(cel.StringType, cel.DynType)},
		cel.BoolType,
		func(arg ...ref.Val) ref.Val {
			map0 := arg[0].Value().(map[string]any)
			map1 := arg[1].Value().(map[string]any)
			return types.Bool(subtree(map0, map1))
		},
	); err != nil {
		return err
	}

	ipAddressType, err := RegisterIPAddressType(ts)
	if err != nil {
		return err
	}
	sts.IPAddressType = ipAddressType

	return nil
}

func requireType[T any](value any) (any, error) {
	vle, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("a %T value is required, but found %T `%v`", *new(T), value, value)
	}
	return vle, nil
}

func convertNumericType[T int64 | uint64 | float64](value any) (any, error) {
	directValue, ok := value.(T)
	if ok {
		return directValue, nil
	}

	floatValue, ok := value.(float64)
	bigFloat := big.NewFloat(floatValue)
	if !ok {
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("a %T value is required, but found %T `%v`", *new(T), value, value)
		}

		f, _, err := big.ParseFloat(stringValue, 10, 64, 0)
		if err != nil {
			return nil, fmt.Errorf("a %T value is required, but found invalid string value `%v`", *new(T), value)
		}

		bigFloat = f
	}

	// Convert the float to the int or uint if necessary.
	n := *new(T)
	switch any(n).(type) {
	case int64:
		if !bigFloat.IsInt() {
			return nil, fmt.Errorf("a int value is required, but found numeric value `%s`", bigFloat.String())
		}

		numericValue, _ := bigFloat.Int64()
		return numericValue, nil

	case uint64:
		if !bigFloat.IsInt() {
			return nil, fmt.Errorf("a uint value is required, but found numeric value `%s`", bigFloat.String())
		}

		numericValue, _ := bigFloat.Int64()
		if numericValue < 0 {
			return nil, fmt.Errorf("a uint value is required, but found int64 value `%s`", bigFloat.String())
		}
		return uint64(numericValue), nil

	case float64:
		numericValue, _ := bigFloat.Float64()
		return numericValue, nil

	default:
		return nil, spiceerrors.MustBugf("unsupported numeric type in caveat number type conversion: %T", n)
	}
}

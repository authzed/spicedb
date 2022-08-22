package caveats

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"github.com/authzed/spicedb/pkg/caveats/customtypes"
)

// VariableType defines the supported types of variables in caveats.
type VariableType struct {
	name    string
	celType *cel.Type
}

var (
	AnyType       = VariableType{"any", cel.AnyType}
	BooleanType   = VariableType{"bool", cel.BoolType}
	StringType    = VariableType{"string", cel.StringType}
	IntType       = VariableType{"int", cel.IntType}
	UIntType      = VariableType{"uint", cel.UintType}
	BytesType     = VariableType{"bytes", cel.BytesType}
	DoubleType    = VariableType{"double", cel.DoubleType}
	DurationType  = VariableType{"duration", cel.DurationType}
	TimestampType = VariableType{"timestamp", cel.TimestampType}
)

var basicTypes = []VariableType{
	AnyType,
	BooleanType,
	StringType,
	IntType,
	UIntType,
	BytesType,
	DoubleType,
	DurationType,
	TimestampType,
}

const (
	listTypeKeyword = "list"
	mapTypeKeyword  = "map"
)

// ListType returns a new type of list over items of the given type.
func ListType(valueType VariableType) VariableType {
	return VariableType{
		name:    fmt.Sprintf("%s<%s>", listTypeKeyword, valueType.name),
		celType: cel.ListType(valueType.celType),
	}
}

// MapType returns a type of a map with keys and values of the given types.
func MapType(keyType VariableType, valueType VariableType) VariableType {
	return VariableType{
		name:    fmt.Sprintf("%s<%s, %s>", mapTypeKeyword, keyType.name, valueType.name),
		celType: cel.MapType(keyType.celType, valueType.celType),
	}
}

// BasicTypeKeywords returns the keywords associated with basic types.
func BasicTypeKeywords() []string {
	keywords := make([]string, 0, len(basicTypes))
	for _, basicType := range basicTypes {
		keywords = append(keywords, basicType.name)
	}
	return keywords
}

// TypeKeywords returns all keywords associated with types.
func TypeKeywords() []string {
	keywords := append(BasicTypeKeywords(), listTypeKeyword, mapTypeKeyword)
	for customTypeName := range customtypes.CustomTypes {
		keywords = append(keywords, strings.ToLower(customTypeName))
	}
	return keywords
}

// Register the custom types.

// IPAddressType defines a specialized representation of an IPAddress.
var IPAddressType = VariableType{"ipaddress", cel.ObjectType("IPAddress")}

type customTypeAdapter struct{}

func (customTypeAdapter) NativeToValue(value interface{}) ref.Val {
	switch t := value.(type) {
	case customtypes.IPAddress:
		return t

	default:
		return types.DefaultTypeAdapter.NativeToValue(value)
	}
}

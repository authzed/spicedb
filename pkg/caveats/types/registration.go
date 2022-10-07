package types

import (
	"github.com/google/cel-go/cel"
)

var definitions = map[string]typeDefinition{}

// CustomTypes holds the set of custom types defined and exported by this package. This is exported
// so that the CEL environment construction can apply the necessary env options for the custom
// types.
var CustomTypes = map[string][]cel.EnvOption{}

type (
	typedValueConverter func(value any) (any, error)
)

type typeDefinition struct {
	// localName is the localized name/keyword for the type.
	localName string

	// childTypeCount is the number of generics on the type, if any.
	childTypeCount uint

	// asVariableType converts the type definition into a VariableType.
	asVariableType func(childTypes []VariableType) VariableType
}

// registerBasicType registers a basic type with the given keyword, CEL type, and converter.
func registerBasicType(keyword string, celType *cel.Type, converter typedValueConverter) VariableType {
	varType := VariableType{
		localName:  keyword,
		celType:    celType,
		childTypes: nil,
		converter:  converter,
	}

	definitions[keyword] = typeDefinition{
		localName:      keyword,
		childTypeCount: 0,
		asVariableType: func(childTypes []VariableType) VariableType {
			return varType
		},
	}
	return varType
}

// registerBasicType registers a type with at least one generic.
func registerGenericType(
	keyword string,
	childTypeCount uint,
	asVariableType func(childTypes []VariableType) VariableType,
) func(childTypes ...VariableType) VariableType {
	definitions[keyword] = typeDefinition{
		localName:      keyword,
		childTypeCount: childTypeCount,
		asVariableType: asVariableType,
	}
	return func(childTypes ...VariableType) VariableType {
		if uint(len(childTypes)) != childTypeCount {
			panic("invalid number of parameters given to type constructor")
		}

		return asVariableType(childTypes)
	}
}

// registerCustomType registers a custom type that wraps a base CEL type.
func registerCustomType(keyword string, baseCelType *cel.Type, converter typedValueConverter, opts ...cel.EnvOption) VariableType {
	CustomTypes[keyword] = opts
	return registerBasicType(keyword, baseCelType, converter)
}

package types

import (
	"fmt"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types/ref"
)

var definitions = map[string]typeDefinition{}

// CustomTypes holds the set of custom types defined and exported by this package. This is exported
// so that the CEL environment construction can apply the necessary env options for the custom
// types.
var CustomTypes = map[string][]cel.EnvOption{}

// CustomMethodsOnTypes holds a set of new methods applied over defined types. This is exported
// so that the CEL environment construction can apply the necessary env options to support these methods
var CustomMethodsOnTypes []cel.EnvOption

type (
	typedValueConverter func(value any) (any, error)
)

type typeDefinition struct {
	// localName is the localized name/keyword for the type.
	localName string

	// childTypeCount is the number of generics on the type, if any.
	childTypeCount uint

	// asVariableType converts the type definition into a VariableType.
	asVariableType func(childTypes []VariableType) (*VariableType, error)
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
		asVariableType: func(childTypes []VariableType) (*VariableType, error) {
			return &varType, nil
		},
	}
	return varType
}

// registerGenericType registers a type with at least one generic.
func registerGenericType(
	keyword string,
	childTypeCount uint,
	asVariableType func(childTypes []VariableType) VariableType,
) func(childTypes ...VariableType) (VariableType, error) {
	definitions[keyword] = typeDefinition{
		localName:      keyword,
		childTypeCount: childTypeCount,
		asVariableType: func(childTypes []VariableType) (*VariableType, error) {
			if uint(len(childTypes)) != childTypeCount {
				return nil, fmt.Errorf("type `%s` requires %d generic types; found %d", keyword, childTypeCount, len(childTypes))
			}

			built := asVariableType(childTypes)
			return &built, nil
		},
	}
	return func(childTypes ...VariableType) (VariableType, error) {
		if uint(len(childTypes)) != childTypeCount {
			return VariableType{}, fmt.Errorf("invalid number of parameters given to type constructor. expected: %d, found: %d", childTypeCount, len(childTypes))
		}

		return asVariableType(childTypes), nil
	}
}

// registerCustomType registers a custom type that wraps a base CEL type.
func registerCustomType[T CustomType](keyword string, baseCelType *cel.Type, converter typedValueConverter, opts ...cel.EnvOption) VariableType {
	CustomTypes[keyword] = opts
	return registerBasicType(keyword, baseCelType, converter)
}

func registerMethodOnDefinedType(baseType *cel.Type, name string, args []*cel.Type, returnType *cel.Type, binding func(arg ...ref.Val) ref.Val) {
	finalArgs := make([]*cel.Type, 0, len(args)+1)
	finalArgs = append(finalArgs, baseType)
	finalArgs = append(finalArgs, args...)
	method := cel.Function(name, cel.MemberOverload(name, finalArgs, returnType, cel.FunctionBinding(binding)))
	CustomMethodsOnTypes = append(CustomMethodsOnTypes, method)
}

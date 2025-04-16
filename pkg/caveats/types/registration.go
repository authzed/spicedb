package types

import (
	"fmt"

	"github.com/authzed/cel-go/cel"
	"github.com/authzed/cel-go/common/types/ref"

	"github.com/authzed/spicedb/pkg/genutil"
)

type (
	typedValueConverter func(value any) (any, error)
)

type typeDefinition struct {
	// localName is the localized name/keyword for the type.
	localName string

	// childTypeCount is the number of generics on the type, if any.
	childTypeCount uint8

	// asVariableType converts the type definition into a VariableType.
	asVariableType func(childTypes []VariableType) (*VariableType, error)
}

// RegisterBasicType registers a basic type with the given keyword, CEL type, and converter.
func RegisterBasicType(ts *TypeSet, keyword string, celType *cel.Type, converter typedValueConverter) (VariableType, error) {
	if ts.isFrozen {
		return VariableType{}, fmt.Errorf("cannot register new types after the TypeSet is frozen")
	}

	varType := VariableType{
		localName:  keyword,
		celType:    celType,
		childTypes: nil,
		converter:  converter,
	}

	ts.definitions[keyword] = typeDefinition{
		localName:      keyword,
		childTypeCount: 0,
		asVariableType: func(childTypes []VariableType) (*VariableType, error) {
			return &varType, nil
		},
	}
	return varType, nil
}

func MustRegisterBasicType(ts *TypeSet, keyword string, celType *cel.Type, converter typedValueConverter) VariableType {
	varType, err := RegisterBasicType(ts, keyword, celType, converter)
	if err != nil {
		panic(fmt.Sprintf("failed to register basic type %s: %v", keyword, err))
	}
	return varType
}

type GenericTypeBuilder func(childTypes ...VariableType) (VariableType, error)

// RegisterGenericType registers a type with at least one generic.
func RegisterGenericType(
	ts *TypeSet,
	keyword string,
	childTypeCount uint8,
	asVariableType func(childTypes []VariableType) VariableType,
) (GenericTypeBuilder, error) {
	if ts.isFrozen {
		return nil, fmt.Errorf("cannot register new types after the TypeSet is frozen")
	}

	ts.definitions[keyword] = typeDefinition{
		localName:      keyword,
		childTypeCount: childTypeCount,
		asVariableType: func(childTypes []VariableType) (*VariableType, error) {
			childTypeLength, err := genutil.EnsureUInt8(len(childTypes))
			if err != nil {
				return nil, err
			}

			if childTypeLength != childTypeCount {
				return nil, fmt.Errorf("type `%s` requires %d generic types; found %d", keyword, childTypeCount, len(childTypes))
			}

			built := asVariableType(childTypes)
			return &built, nil
		},
	}
	return func(childTypes ...VariableType) (VariableType, error) {
		childTypeLength, err := genutil.EnsureUInt8(len(childTypes))
		if err != nil {
			return VariableType{}, err
		}

		if childTypeLength != childTypeCount {
			return VariableType{}, fmt.Errorf("invalid number of parameters given to type constructor. expected: %d, found: %d", childTypeCount, len(childTypes))
		}

		return asVariableType(childTypes), nil
	}, nil
}

func MustRegisterGenericType(
	ts *TypeSet,
	keyword string,
	childTypeCount uint8,
	asVariableType func(childTypes []VariableType) VariableType,
) GenericTypeBuilder {
	genericTypeBuilder, err := RegisterGenericType(ts, keyword, childTypeCount, asVariableType)
	if err != nil {
		panic(fmt.Sprintf("failed to register generic type %s: %v", keyword, err))
	}
	return genericTypeBuilder
}

// RegisterCustomType registers a custom type that wraps a base CEL type.
func RegisterCustomType[T CustomType](ts *TypeSet, keyword string, baseCelType *cel.Type, converter typedValueConverter, opts ...cel.EnvOption) (VariableType, error) {
	if ts.isFrozen {
		return VariableType{}, fmt.Errorf("cannot register new types after the TypeSet is frozen")
	}

	if err := RegisterCustomCELOptions(ts, opts...); err != nil {
		return VariableType{}, err
	}

	return RegisterBasicType(ts, keyword, baseCelType, converter)
}

// RegisterCustomTypeWithName registers a custom type with a specific name.
func RegisterMethodOnDefinedType(ts *TypeSet, baseType *cel.Type, name string, args []*cel.Type, returnType *cel.Type, binding func(arg ...ref.Val) ref.Val) error {
	finalArgs := make([]*cel.Type, 0, len(args)+1)
	finalArgs = append(finalArgs, baseType)
	finalArgs = append(finalArgs, args...)
	method := cel.Function(name, cel.MemberOverload(name, finalArgs, returnType, cel.FunctionBinding(binding)))

	return RegisterCustomCELOptions(ts, method)
}

// RegisterCustomOptions registers custom CEL environment options for the TypeSet.
func RegisterCustomCELOptions(ts *TypeSet, opts ...cel.EnvOption) error {
	if ts.isFrozen {
		return fmt.Errorf("cannot register new options after the TypeSet is frozen")
	}
	ts.customOptions = append(ts.customOptions, opts...)
	return nil
}

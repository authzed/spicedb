package types

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// EncodeParameterTypes converts the map of internal caveat types into a map of types for storing
// the caveat in the core.
func EncodeParameterTypes(parametersAndTypes map[string]VariableType) map[string]*core.CaveatTypeReference {
	encoded := make(map[string]*core.CaveatTypeReference, len(parametersAndTypes))
	for name, varType := range parametersAndTypes {
		encoded[name] = EncodeParameterType(varType)
	}
	return encoded
}

// EncodeParameterType converts an internal caveat type into a storable core type.
func EncodeParameterType(varType VariableType) *core.CaveatTypeReference {
	childTypes := make([]*core.CaveatTypeReference, 0, len(varType.childTypes))
	for _, childType := range varType.childTypes {
		childTypes = append(childTypes, EncodeParameterType(childType))
	}

	return &core.CaveatTypeReference{
		TypeName:   varType.localName,
		ChildTypes: childTypes,
	}
}

// DecodeParameterType decodes the core caveat parameter type into an internal caveat type.
func DecodeParameterType(parameterType *core.CaveatTypeReference) (*VariableType, error) {
	typeDef, ok := definitions[parameterType.TypeName]
	if !ok {
		return nil, fmt.Errorf("unknown caveat parameter type `%s`", parameterType.TypeName)
	}

	if len(parameterType.ChildTypes) != int(typeDef.childTypeCount) {
		return nil, fmt.Errorf(
			"caveat parameter type `%s` requires %d child types; found %d",
			parameterType.TypeName,
			len(parameterType.ChildTypes),
			typeDef.childTypeCount,
		)
	}

	childTypes := make([]VariableType, 0, typeDef.childTypeCount)
	for _, encodedChildType := range parameterType.ChildTypes {
		childType, err := DecodeParameterType(encodedChildType)
		if err != nil {
			return nil, err
		}
		childTypes = append(childTypes, *childType)
	}

	return typeDef.asVariableType(childTypes)
}

// DecodeParameterTypes decodes the core caveat parameter types into internal caveat types.
func DecodeParameterTypes(parameters map[string]*core.CaveatTypeReference) (map[string]VariableType, error) {
	parameterTypes := make(map[string]VariableType, len(parameters))
	for paramName, paramType := range parameters {
		decodedType, err := DecodeParameterType(paramType)
		if err != nil {
			return nil, err
		}

		parameterTypes[paramName] = *decodedType
	}
	return parameterTypes, nil
}

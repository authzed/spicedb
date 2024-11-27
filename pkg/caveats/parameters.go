package caveats

import (
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// UnknownParameterOption is the option to ConvertContextToParameters around handling
// of unknown parameters.
type UnknownParameterOption int

const (
	// SkipUnknownParameters indicates that unknown parameters should be skipped in conversion.
	SkipUnknownParameters UnknownParameterOption = 0

	// ErrorForUnknownParameters indicates that unknown parameters should return an error.
	ErrorForUnknownParameters UnknownParameterOption = 1
)

// ConvertContextToParameters converts the given context into parameters of the types specified.
// Returns a type error if type conversion failed.
func ConvertContextToParameters(
	contextMap map[string]any,
	parameterTypes map[string]*core.CaveatTypeReference,
	unknownParametersOption UnknownParameterOption,
) (map[string]any, error) {
	if len(contextMap) == 0 {
		return nil, nil
	}

	if len(parameterTypes) == 0 {
		return nil, fmt.Errorf("missing parameters for caveat")
	}

	converted := make(map[string]any, len(contextMap))

	for key, value := range contextMap {
		paramType, ok := parameterTypes[key]
		if !ok {
			if unknownParametersOption == ErrorForUnknownParameters {
				return nil, fmt.Errorf("unknown parameter `%s`", key)
			}

			continue
		}

		varType, err := types.DecodeParameterType(paramType)
		if err != nil {
			return nil, err
		}

		convertedParam, err := varType.ConvertValue(value)
		if err != nil {
			return nil, ParameterConversionError{fmt.Errorf("could not convert context parameter `%s`: %w", key, err), key}
		}

		converted[key] = convertedParam
	}
	return converted, nil
}

// ParameterTypeString returns the string form of the type reference.
func ParameterTypeString(typeRef *core.CaveatTypeReference) string {
	var sb strings.Builder
	sb.WriteString(typeRef.TypeName)
	if len(typeRef.ChildTypes) > 0 {
		sb.WriteString("<")
		for idx, childType := range typeRef.ChildTypes {
			if idx > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ParameterTypeString(childType))
		}
		sb.WriteString(">")
	}

	return sb.String()
}

package caveats

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ConvertContextToParameters converts the given context into parameters of the types specified.
// Returns a type error if type conversion failed.
func ConvertContextToParameters(contextMap map[string]any, parameterTypes map[string]*core.CaveatTypeReference) (map[string]any, error) {
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
			continue
		}

		varType, err := types.DecodeParameterType(paramType)
		if err != nil {
			return nil, err
		}

		convertedParam, err := varType.ConvertValue(value)
		if err != nil {
			return nil, ParameterConversionErr{fmt.Errorf("could not convert context parameter `%s`: %w", key, err), key}
		}

		converted[key] = convertedParam
	}
	return converted, nil
}

// ParameterConversionErr is an error in type conversion of a supplied parameter.
type ParameterConversionErr struct {
	error
	parameterName string
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err ParameterConversionErr) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("parameterName", err.parameterName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ParameterConversionErr) DetailsMetadata() map[string]string {
	return map[string]string{
		"parameter_name": err.parameterName,
	}
}

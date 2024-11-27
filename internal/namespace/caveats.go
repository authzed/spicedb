package namespace

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/typesystem"
)

// ValidateCaveatDefinition validates the parameters and types within the given caveat
// definition, including usage of the parameters.
func ValidateCaveatDefinition(caveat *core.CaveatDefinition) error {
	// Ensure all parameters are used by the caveat expression itself.
	parameterTypes, err := caveattypes.DecodeParameterTypes(caveat.ParameterTypes)
	if err != nil {
		return typesystem.NewTypeWithSourceError(
			fmt.Errorf("could not decode caveat parameters `%s`: %w", caveat.Name, err),
			caveat,
			caveat.Name,
		)
	}

	deserialized, err := caveats.DeserializeCaveat(caveat.SerializedExpression, parameterTypes)
	if err != nil {
		return typesystem.NewTypeWithSourceError(
			fmt.Errorf("could not decode caveat `%s`: %w", caveat.Name, err),
			caveat,
			caveat.Name,
		)
	}

	if len(caveat.ParameterTypes) == 0 {
		return typesystem.NewTypeWithSourceError(
			fmt.Errorf("caveat `%s` must have at least one parameter defined", caveat.Name),
			caveat,
			caveat.Name,
		)
	}

	referencedNames, err := deserialized.ReferencedParameters(maps.Keys(caveat.ParameterTypes))
	if err != nil {
		return err
	}

	for paramName, paramType := range caveat.ParameterTypes {
		_, err := caveattypes.DecodeParameterType(paramType)
		if err != nil {
			return typesystem.NewTypeWithSourceError(
				fmt.Errorf("type error for parameter `%s` for caveat `%s`: %w", paramName, caveat.Name, err),
				caveat,
				paramName,
			)
		}

		if !referencedNames.Has(paramName) {
			return typesystem.NewTypeWithSourceError(
				NewUnusedCaveatParameterErr(caveat.Name, paramName),
				caveat,
				paramName,
			)
		}
	}

	return nil
}

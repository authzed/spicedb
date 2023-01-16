package namespace

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ValidateCaveatDefinition validates the parameters and types within the given caveat
// definition, including usage of the parameters.
func ValidateCaveatDefinition(caveat *core.CaveatDefinition) error {
	// Ensure all parameters are used by the caveat expression itself.
	deserialized, err := caveats.DeserializeCaveat(caveat.SerializedExpression)
	if err != nil {
		return newTypeErrorWithSource(
			fmt.Errorf("could not decode caveat `%s`: %w", caveat.Name, err),
			caveat,
			caveat.Name,
		)
	}

	if len(caveat.ParameterTypes) == 0 {
		return newTypeErrorWithSource(
			fmt.Errorf("caveat `%s` must have at least one parameter defined", caveat.Name),
			caveat,
			caveat.Name,
		)
	}

	referencedNames := deserialized.ReferencedParameters(maps.Keys(caveat.ParameterTypes))
	for paramName, paramType := range caveat.ParameterTypes {
		_, err := caveattypes.DecodeParameterType(paramType)
		if err != nil {
			return newTypeErrorWithSource(
				fmt.Errorf("type error for parameter `%s` for caveat `%s`: %w", paramName, caveat.Name, err),
				caveat,
				paramName,
			)
		}

		if !referencedNames.Has(paramName) {
			return newTypeErrorWithSource(
				NewUnusedCaveatParameterErr(caveat.Name, paramName),
				caveat,
				paramName,
			)
		}
	}

	return nil
}

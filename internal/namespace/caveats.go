package namespace

import (
	"fmt"
	"maps"
	"slices"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
)

// ValidateCaveatDefinition validates the parameters and types within the given caveat
// definition, including usage of the parameters.
func ValidateCaveatDefinition(ts *caveattypes.TypeSet, caveat *core.CaveatDefinition) error {
	// Ensure all parameters are used by the caveat expression itself.
	parameterTypes, err := caveattypes.DecodeParameterTypes(ts, caveat.GetParameterTypes())
	if err != nil {
		return schema.NewTypeWithSourceError(
			fmt.Errorf("could not decode caveat parameters `%s`: %w", caveat.GetName(), err),
			caveat,
			caveat.GetName(),
		)
	}

	deserialized, err := caveats.DeserializeCaveatWithTypeSet(ts, caveat.GetSerializedExpression(), parameterTypes)
	if err != nil {
		return schema.NewTypeWithSourceError(
			fmt.Errorf("could not decode caveat `%s`: %w", caveat.GetName(), err),
			caveat,
			caveat.GetName(),
		)
	}

	if len(caveat.GetParameterTypes()) == 0 {
		return schema.NewTypeWithSourceError(
			fmt.Errorf("caveat `%s` must have at least one parameter defined", caveat.GetName()),
			caveat,
			caveat.GetName(),
		)
	}

	referencedNames, err := deserialized.ReferencedParameters(slices.Collect(maps.Keys(caveat.GetParameterTypes())))
	if err != nil {
		return err
	}

	for paramName, paramType := range caveat.GetParameterTypes() {
		_, err := caveattypes.DecodeParameterType(ts, paramType)
		if err != nil {
			return schema.NewTypeWithSourceError(
				fmt.Errorf("type error for parameter `%s` for caveat `%s`: %w", paramName, caveat.GetName(), err),
				caveat,
				paramName,
			)
		}

		if !referencedNames.Has(paramName) {
			return schema.NewTypeWithSourceError(
				NewUnusedCaveatParameterErr(caveat.GetName(), paramName),
				caveat,
				paramName,
			)
		}
	}

	return nil
}

package caveats

import (
	"fmt"

	"github.com/authzed/cel-go/cel"

	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Environment defines the evaluation environment for a caveat.
type Environment struct {
	ts        *types.TypeSet
	variables map[string]types.VariableType
}

// NewEnvironmentWithDefaultTypeSet creates and returns a new environment for compiling a caveat,
// with the default type set.
func NewEnvironmentWithDefaultTypeSet() *Environment {
	return &Environment{
		ts:        types.Default.TypeSet,
		variables: map[string]types.VariableType{},
	}
}

// NewEnvironmentWithTypeSet creates and returns a new environment for compiling a caveat
// with the given TypeSet.
func NewEnvironmentWithTypeSet(ts *types.TypeSet) *Environment {
	return &Environment{
		ts:        ts,
		variables: map[string]types.VariableType{},
	}
}

// EnvForVariablesWithDefaultTypeSet returns a new environment constructed for the given variables.
func EnvForVariablesWithDefaultTypeSet(vars map[string]types.VariableType) (*Environment, error) {
	return EnvForVariablesWithTypeSet(types.Default.TypeSet, vars)
}

// EnvForVariablesWithTypeSet returns a new environment constructed for the given variables.
func EnvForVariablesWithTypeSet(ts *types.TypeSet, vars map[string]types.VariableType) (*Environment, error) {
	e := NewEnvironmentWithTypeSet(ts)
	for varName, varType := range vars {
		err := e.AddVariable(varName, varType)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

// MustEnvForVariablesWithDefaultTypeSet returns a new environment constructed for the given variables
// or panics.
func MustEnvForVariablesWithDefaultTypeSet(vars map[string]types.VariableType) *Environment {
	env, err := EnvForVariablesWithDefaultTypeSet(vars)
	if err != nil {
		panic(err)
	}
	return env
}

// AddVariable adds a variable with the given type to the environment.
func (e *Environment) AddVariable(name string, varType types.VariableType) error {
	if _, ok := e.variables[name]; ok {
		return fmt.Errorf("variable `%s` already exists", name)
	}

	e.variables[name] = varType
	return nil
}

// EncodedParametersTypes returns the map of encoded parameters for the environment.
func (e *Environment) EncodedParametersTypes() map[string]*core.CaveatTypeReference {
	return types.EncodeParameterTypes(e.variables)
}

// asCelEnvironment converts the exported Environment into an internal CEL environment.
func (e *Environment) asCelEnvironment(extraOptions ...cel.EnvOption) (*cel.Env, error) {
	tsOptions, err := e.ts.EnvOptions()
	if err != nil {
		return nil, err
	}

	opts := make([]cel.EnvOption, 0, len(extraOptions)+len(e.variables)+len(tsOptions)+2)
	opts = append(opts, extraOptions...)
	opts = append(opts, tsOptions...)

	// Set options.
	// DefaultUTCTimeZone: ensure all timestamps are evaluated at UTC
	opts = append(opts, cel.DefaultUTCTimeZone(true))

	// OptionalTypes: enable optional typing syntax, e.g. `sometype?.foo`
	// See: https://github.com/google/cel-spec/wiki/proposal-246
	opts = append(opts, cel.OptionalTypes(cel.OptionalTypesVersion(0)))

	// EnableMacroCallTracking: enables tracking of call macros so when we call AstToString we get
	// back out the expected expressions.
	// See: https://github.com/authzed/cel-go/issues/474
	opts = append(opts, cel.EnableMacroCallTracking())

	// ParserExpressionSizeLimit: disable the size limit for codepoints in expressions.
	// This has to be disabled due to us padding out the whitespace in expression parsing based on
	// schema size. We instead do our own expression size check in the Compile method.
	// TODO(jschorr): Remove this once the whitespace hack is removed.
	opts = append(opts, cel.ParserExpressionSizeLimit(-1))

	for name, varType := range e.variables {
		opts = append(opts, cel.Variable(name, varType.CelType()))
	}
	return cel.NewEnv(opts...)
}

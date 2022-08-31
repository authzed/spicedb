package caveats

import (
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/authzed/spicedb/pkg/caveats/customtypes"
)

// Environment defines the evaluation environment for a caveat.
type Environment struct {
	variables map[string]*cel.Type
}

// NewEnvironment creates and returns a new environment for compiling a caveat.
func NewEnvironment() *Environment {
	return &Environment{
		variables: map[string]*cel.Type{},
	}
}

// EnvForVariables returns a new environment constructed for the given variables.
func EnvForVariables(vars map[string]VariableType) (*Environment, error) {
	e := NewEnvironment()
	for varName, varType := range vars {
		err := e.AddVariable(varName, varType)
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

func mustEnvForVariables(vars map[string]VariableType) *Environment {
	env, err := EnvForVariables(vars)
	if err != nil {
		panic(err)
	}
	return env
}

// AddVariable adds a variable with the given type to the environment.
func (e *Environment) AddVariable(name string, varType VariableType) error {
	if _, ok := e.variables[name]; ok {
		return fmt.Errorf("variable `%s` already exists", name)
	}

	e.variables[name] = varType.celType
	return nil
}

// asCelEnvironment converts the exported Environment into an internal CEL environment.
func (e *Environment) asCelEnvironment() (*cel.Env, error) {
	opts := make([]cel.EnvOption, 0, len(e.variables)+len(customtypes.CustomTypes)+2)

	// Add the custom type adapter and functions.
	opts = append(opts, cel.CustomTypeAdapter(&customTypeAdapter{}))
	for _, customTypeOpts := range customtypes.CustomTypes {
		opts = append(opts, customTypeOpts...)
	}

	// Set options.
	// DefaultUTCTimeZone: ensure all timestamps are evaluated at UTC
	opts = append(opts, cel.DefaultUTCTimeZone(true))

	for name, varType := range e.variables {
		opts = append(opts, cel.Variable(name, varType))
	}
	return cel.NewEnv(opts...)
}

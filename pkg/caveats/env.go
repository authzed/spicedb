package caveats

import (
	"fmt"
	"sync"

	"github.com/authzed/cel-go/cel"

	"github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Environment defines the evaluation environment for a caveat.
type Environment struct {
	ts        *types.TypeSet
	variables map[string]types.VariableType

	celEnvMu sync.Mutex
	celEnv   *cel.Env // GUARDED_BY(celEnvMu)
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

	// Invalidate the cached CEL environment, as it no longer reflects the set
	// of variables defined on this environment.
	e.celEnvMu.Lock()
	defer e.celEnvMu.Unlock()
	e.celEnv = nil

	return nil
}

// EncodedParametersTypes returns the map of encoded parameters for the environment.
func (e *Environment) EncodedParametersTypes() map[string]*core.CaveatTypeReference {
	return types.EncodeParameterTypes(e.variables)
}

// asCelEnvironment converts the exported Environment into an internal CEL environment.
//
// The environment is derived from the TypeSet's cached base environment, so
// the (expensive) standard and custom type declarations are only built once
// per TypeSet.
//
// The result is cached on the Environment and invalidated by AddVariable.
func (e *Environment) asCelEnvironment() (*cel.Env, error) {
	e.celEnvMu.Lock()
	defer e.celEnvMu.Unlock()

	if e.celEnv != nil {
		return e.celEnv, nil
	}

	baseEnv, err := e.ts.BaseCelEnvironment()
	if err != nil {
		return nil, err
	}

	opts := make([]cel.EnvOption, 0, len(e.variables))
	for name, varType := range e.variables {
		opts = append(opts, cel.Variable(name, varType.CelType()))
	}

	newCelEnv, err := baseEnv.Extend(opts...)
	if err != nil {
		return nil, err
	}
	e.celEnv = newCelEnv

	return e.celEnv, nil
}

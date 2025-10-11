package types

import (
	"errors"
	"fmt"

	"github.com/authzed/cel-go/cel"
)

// TypeSet defines a set of types that can be used in caveats. It is used to register custom types
// and methods that can be used in caveats. The types are registered by calling the RegisterType
// function. The types are then used to build the CEL environment for the caveat.
type TypeSet struct {
	// definitions holds the set of all types defined and exported by this package, by name.
	definitions map[string]typeDefinition

	// customOptions holds a set of custom options that can be used to create a CEL environment
	// for the caveat.
	customOptions []cel.EnvOption

	// isFrozen indicates whether the TypeSet is frozen. A frozen TypeSet cannot be modified.
	isFrozen bool
}

// Freeze marks the TypeSet as frozen. A frozen TypeSet cannot be modified.
func (ts *TypeSet) Freeze() {
	ts.isFrozen = true
}

// EnvOptions returns the set of environment options that can be used to create a CEL environment
// for the caveat. This includes the custom types and methods defined in the TypeSet.
func (ts *TypeSet) EnvOptions() ([]cel.EnvOption, error) {
	if !ts.isFrozen {
		return nil, errors.New("cannot get env options from a non-frozen TypeSet")
	}
	return ts.customOptions, nil
}

// BuildType builds a variable type from its name and child types.
func (ts *TypeSet) BuildType(name string, childTypes []VariableType) (*VariableType, error) {
	if !ts.isFrozen {
		return nil, errors.New("cannot build types from a non-frozen TypeSet")
	}

	typeDef, ok := ts.definitions[name]
	if !ok {
		return nil, fmt.Errorf("unknown type `%s`", name)
	}

	return typeDef.asVariableType(childTypes)
}

// NewTypeSet creates a new TypeSet. The TypeSet is not frozen and can be modified.
func NewTypeSet() *TypeSet {
	return &TypeSet{
		definitions:   map[string]typeDefinition{},
		customOptions: []cel.EnvOption{},
		isFrozen:      false,
	}
}
